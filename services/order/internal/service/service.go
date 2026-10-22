// Package service chứa nghiệp vụ của order: tạo đơn từ một hold và (từ M5) điều phối Saga
// thanh toán.
//
// Order là "orchestrator" của Saga: nó gọi inventory đồng bộ qua gRPC (GetHold, ExtendHold, sau
// này ConfirmHold/ReleaseHold) và nói chuyện với payment bất đồng bộ qua Redpanda. Mọi message
// gửi đi đều ghi vào bảng outbox trong CÙNG transaction với thay đổi của đơn (Transactional
// Outbox), relay publish sau.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	catalogv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/catalog/v1"
	inventoryv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/inventory/v1"
	"github.com/tmtuan04/ticketing/order/internal/repo"
)

// Giá trị cột orders.status, khớp CHECK constraint trong migration.
const (
	StatusPending         = "pending"
	StatusAwaitingPayment = "awaiting_payment"
	StatusPaid            = "paid"
	StatusConfirmed       = "confirmed"
	StatusCancelled       = "cancelled"
	StatusRefunding       = "refunding"
	StatusRefunded        = "refunded"
)

// Các bước ghi vào order_saga_log.
const stepOrderCreated = "order_created"

const maxIdempotencyKeyLen = 200

// orderIDNamespace sinh order_id cố định từ (user_id, idempotency_key), cùng ý tưởng
// holdIDNamespace của inventory: hai request trùng key chạy song song tranh cùng một khóa chính.
var orderIDNamespace = uuid.MustParse("4f1d7a2c-8b3e-4c6a-9e5f-2a7b1c9d3e60")

// Config: tham số nghiệp vụ.
type Config struct {
	// PaymentTimeout: thời gian user có để trả tiền, tính từ lúc tạo đơn.
	PaymentTimeout time.Duration
	// HoldBuffer: hold được gia hạn tới PaymentTimeout + HoldBuffer. Phần đệm che độ trễ của
	// webhook/message và lệch giờ giữa các service, để tiền về đúng hạn thì ghế vẫn còn giữ.
	HoldBuffer time.Duration
	// CallTimeout: deadline cho mỗi lời gọi gRPC xuống inventory/catalog.
	CallTimeout time.Duration
}

// Service an toàn khi dùng đồng thời (pool và gRPC client đều thread-safe).
//
// inventory/catalog là interface client sinh từ proto: chạy thật thì là kết nối gRPC, test thì
// thay bằng bản giả (xem service_test.go).
type Service struct {
	pool      *pgxpool.Pool
	q         *repo.Queries
	inventory inventoryv1.InventoryServiceClient
	catalog   catalogv1.CatalogServiceClient
	cfg       Config
	log       *slog.Logger
}

func New(pool *pgxpool.Pool, inventory inventoryv1.InventoryServiceClient, catalog catalogv1.CatalogServiceClient, cfg Config, log *slog.Logger) *Service {
	return &Service{pool: pool, q: repo.New(pool), inventory: inventory, catalog: catalog, cfg: cfg, log: log}
}

// CreateRequest là input của CreateOrder (đã parse uuid ở tầng grpcserver).
type CreateRequest struct {
	UserID         uuid.UUID
	HoldID         uuid.UUID
	IdempotencyKey string
}

// CreateOrder tạo đơn AWAITING_PAYMENT từ một hold và xếp lệnh payment.requested vào outbox.
//
// Luồng xử lý:
//  1. replay: key đã dùng -> trả đơn cũ. Hold đã có đơn (key khác) -> ErrOrderExists.
//  2. inventory.GetHold: hold phải của user và còn active.
//  3. catalog: tính tiền theo giá vé (không tin giá từ client).
//  4. inventory.ExtendHold: hold sống tới PaymentTimeout + HoldBuffer.
//  5. MỘT transaction: INSERT orders + order_saga_log + outbox.
//
// Bước 2-4 gọi ra ngoài nên nằm TRƯỚC transaction: không giữ transaction/khóa DB trong lúc chờ
// mạng. Nếu bước 4 xong mà bước 5 lỗi, hold chỉ bị gia hạn mà không có đơn: vô hại (vẫn hết hạn
// sau tối đa PaymentTimeout + HoldBuffer), và client retry cùng key sẽ làm lại từ đầu.
func (s *Service) CreateOrder(ctx context.Context, req CreateRequest) (repo.Order, error) {
	if err := validateCreate(req); err != nil {
		return repo.Order{}, err
	}
	if o, ok, err := s.replay(ctx, req); ok || err != nil {
		return o, err
	}

	hold, err := s.activeHold(ctx, req)
	if err != nil {
		return repo.Order{}, err
	}
	amount, currency, err := s.price(ctx, hold)
	if err != nil {
		return repo.Order{}, err
	}

	// Chốt hạn thanh toán TRƯỚC khi gia hạn hold, để luôn có payment_deadline + HoldBuffer <= hạn hold.
	deadline := time.Now().Add(s.cfg.PaymentTimeout)
	if err := s.extendHold(ctx, req, s.cfg.PaymentTimeout+s.cfg.HoldBuffer); err != nil {
		return repo.Order{}, err
	}

	seatIDs, err := parseUUIDs(hold.GetSeatIds())
	if err != nil {
		return repo.Order{}, fmt.Errorf("inventory returned bad seat id: %w", err)
	}
	eventID, err := uuid.Parse(hold.GetEventId())
	if err != nil {
		return repo.Order{}, fmt.Errorf("inventory returned bad event id: %w", err)
	}
	params := repo.CreateOrderParams{
		ID:              uuid.NewSHA1(orderIDNamespace, []byte(req.UserID.String()+":"+req.IdempotencyKey)),
		UserID:          req.UserID,
		EventID:         eventID,
		HoldID:          req.HoldID,
		SeatIds:         seatIDs,
		Amount:          amount,
		Currency:        currency,
		Status:          StatusAwaitingPayment,
		PaymentDeadline: deadline,
		IdempotencyKey:  req.IdempotencyKey,
	}
	order, err := s.insertOrder(ctx, params, hold.GetExpiresAt().AsTime())
	if err == nil {
		return order, nil
	}
	// Vi phạm unique: hoặc request song song cùng key đã thắng (trả đơn của nó), hoặc hold vừa
	// có đơn với key khác.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		if o, ok, rerr := s.replay(ctx, req); ok || rerr != nil {
			return o, rerr
		}
		return repo.Order{}, ErrOrderExists
	}
	return repo.Order{}, err
}

// insertOrder ghi đơn, saga log và message payment.requested trong một transaction: cả ba cùng
// tồn tại hoặc cùng không. Đây là toàn bộ ý nghĩa của Transactional Outbox.
func (s *Service) insertOrder(ctx context.Context, p repo.CreateOrderParams, holdExpiresAt time.Time) (repo.Order, error) {
	var order repo.Order
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		var err error
		if order, err = q.CreateOrder(ctx, p); err != nil {
			return err
		}
		detail, err := json.Marshal(map[string]any{"hold_id": order.HoldID, "amount": order.Amount})
		if err != nil {
			return err
		}
		if err := q.InsertSagaLog(ctx, repo.InsertSagaLogParams{
			OrderID: order.ID, Step: stepOrderCreated, Status: order.Status, Detail: detail,
		}); err != nil {
			return err
		}
		payload, err := json.Marshal(PaymentRequested{
			OrderID:         order.ID,
			UserID:          order.UserID,
			Amount:          order.Amount,
			Currency:        order.Currency,
			PaymentDeadline: order.PaymentDeadline,
		})
		if err != nil {
			return err
		}
		return q.InsertOutbox(ctx, repo.InsertOutboxParams{
			MessageID:   uuid.New(),
			AggregateID: order.ID,
			Topic:       TopicPaymentCommands,
			Key:         order.ID.String(),
			Type:        TypePaymentRequested,
			Payload:     payload,
		})
	})
	if err != nil {
		return repo.Order{}, err
	}
	s.log.InfoContext(ctx, "order created", "order_id", order.ID, "hold_id", order.HoldID,
		"amount", order.Amount, "hold_expires_at", holdExpiresAt)
	return order, nil
}

// replay trả đơn đã tạo bằng cùng key (ok=true). Cùng key nhưng khác hold là lỗi client.
// Key mới nhưng hold đã có đơn: của user khác -> ErrPermissionDenied, của chính user -> ErrOrderExists.
func (s *Service) replay(ctx context.Context, req CreateRequest) (repo.Order, bool, error) {
	o, err := s.q.GetOrderByIdempotencyKey(ctx, repo.GetOrderByIdempotencyKeyParams{UserID: req.UserID, IdempotencyKey: req.IdempotencyKey})
	switch {
	case err == nil:
		if o.HoldID != req.HoldID {
			return repo.Order{}, false, ErrIdempotencyMismatch
		}
		return o, true, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return repo.Order{}, false, err
	}
	o, err = s.q.GetOrderByHold(ctx, req.HoldID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return repo.Order{}, false, nil
	case err != nil:
		return repo.Order{}, false, err
	case o.UserID != req.UserID:
		return repo.Order{}, false, ErrPermissionDenied
	case o.IdempotencyKey == req.IdempotencyKey:
		// Request song song cùng key vừa commit giữa hai câu SELECT ở trên: đó là đơn của mình.
		return o, true, nil
	default:
		return repo.Order{}, false, ErrOrderExists
	}
}

// activeHold đọc hold từ inventory và kiểm tra thuộc user, còn active.
func (s *Service) activeHold(ctx context.Context, req CreateRequest) (*inventoryv1.Hold, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.CallTimeout)
	defer cancel()
	res, err := s.inventory.GetHold(ctx, &inventoryv1.GetHoldRequest{HoldId: req.HoldID.String()})
	if err != nil {
		return nil, fromInventory("GetHold", err)
	}
	hold := res.GetHold()
	switch {
	case hold.GetUserId() != req.UserID.String():
		return nil, ErrPermissionDenied
	case hold.GetStatus() != inventoryv1.HoldStatus_HOLD_STATUS_ACTIVE:
		return nil, ErrHoldNotActive
	}
	return hold, nil
}

// extendHold gọi inventory.ExtendHold. Hold có thể vừa hết hạn/bị nhả giữa GetHold và lúc này
// -> FAILED_PRECONDITION -> ErrHoldNotActive, không tạo đơn.
func (s *Service) extendHold(ctx context.Context, req CreateRequest, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.CallTimeout)
	defer cancel()
	_, err := s.inventory.ExtendHold(ctx, &inventoryv1.ExtendHoldRequest{
		HoldId: req.HoldID.String(),
		UserId: req.UserID.String(),
		Ttl:    durationpb.New(ttl),
	})
	if err != nil {
		return fromInventory("ExtendHold", err)
	}
	return nil
}

// price tính tổng tiền các ghế của hold theo giá hạng vé trong catalog. Catalog chỉ cho một loại
// tiền mỗi event nên mọi hạng vé cùng currency.
func (s *Service) price(ctx context.Context, hold *inventoryv1.Hold) (int64, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.CallTimeout)
	defer cancel()
	ev, err := s.catalog.GetEvent(ctx, &catalogv1.GetEventRequest{EventId: hold.GetEventId()})
	if err != nil {
		return 0, "", fmt.Errorf("%w: catalog.GetEvent: %v", ErrUnavailable, err)
	}
	// Đọc cả sơ đồ ghế của event rồi lọc: đơn giản, đủ nhanh với vài nghìn ghế. Nếu event rất
	// lớn thì thêm RPC lấy ghế theo danh sách id.
	seats, err := s.catalog.ListSeats(ctx, &catalogv1.ListSeatsRequest{EventId: hold.GetEventId()})
	if err != nil {
		return 0, "", fmt.Errorf("%w: catalog.ListSeats: %v", ErrUnavailable, err)
	}

	tiers := map[string]*catalogv1.TicketTier{}
	for _, t := range ev.GetEvent().GetTiers() {
		tiers[t.GetId()] = t
	}
	seatTier := map[string]string{}
	for _, st := range seats.GetSeats() {
		seatTier[st.GetId()] = st.GetTierId()
	}
	var (
		amount   int64
		currency string
	)
	for _, id := range hold.GetSeatIds() {
		t, ok := tiers[seatTier[id]]
		if !ok {
			// Ghế đã được giữ ở inventory mà catalog không biết giá: dữ liệu hai bên lệch nhau.
			return 0, "", fmt.Errorf("catalog has no price for seat %s of event %s", id, hold.GetEventId())
		}
		amount += t.GetPriceAmount()
		currency = t.GetCurrency()
	}
	return amount, currency, nil
}

// GetOrder đọc đơn theo id.
func (s *Service) GetOrder(ctx context.Context, id uuid.UUID) (repo.Order, error) {
	o, err := s.q.GetOrder(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return repo.Order{}, ErrNotFound
	}
	return o, err
}

func validateCreate(req CreateRequest) error {
	switch {
	case req.UserID == uuid.Nil:
		return invalidArg("user_id is required")
	case req.HoldID == uuid.Nil:
		return invalidArg("hold_id is required")
	case req.IdempotencyKey == "":
		return invalidArg("idempotency_key is required")
	case len(req.IdempotencyKey) > maxIdempotencyKeyLen:
		return invalidArg("idempotency_key is longer than %d characters", maxIdempotencyKeyLen)
	}
	return nil
}

// fromInventory đổi gRPC status của inventory sang lỗi nghiệp vụ của order. Lỗi hạ tầng (mất kết
// nối, quá hạn...) thành ErrUnavailable để client biết nên thử lại.
func fromInventory(method string, err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return ErrHoldNotFound
	case codes.PermissionDenied:
		return ErrPermissionDenied
	case codes.FailedPrecondition:
		return ErrHoldNotActive
	default:
		return fmt.Errorf("%w: inventory.%s: %v", ErrUnavailable, method, err)
	}
}

func parseUUIDs(ss []string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, len(ss))
	for i, s := range ss {
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, err
		}
		out[i] = id
	}
	return out, nil
}
