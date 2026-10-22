// Integration test cho package service: Postgres THẬT (testcontainers) + inventory/catalog GIẢ.
//
//	go test ./...          # cần Docker đang chạy
//	go test -short ./...   # bỏ qua test cần container
//
// inventory/catalog giả là struct nhúng interface client sinh từ proto và chỉ viết lại các
// method order thực sự gọi. Method nào chưa viết mà bị gọi sẽ panic (nil interface) -> test lộ
// ngay việc order gọi thêm RPC ngoài dự kiến.
package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	catalogv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/catalog/v1"
	inventoryv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/inventory/v1"
	"github.com/tmtuan04/ticketing/order/internal/service"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	code, err := runWithPostgres(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func runWithPostgres(m *testing.M) (int, error) {
	ctx := context.Background()
	migrations, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil || len(migrations) == 0 {
		return 0, fmt.Errorf("no migrations found: %v", err)
	}
	slices.Sort(migrations)
	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("order_db"),
		tcpostgres.WithUsername("app"),
		tcpostgres.WithPassword("app"),
		tcpostgres.WithOrderedInitScripts(migrations...),
		tcpostgres.BasicWaitStrategies(),
	)
	defer func() { _ = testcontainers.TerminateContainer(pg) }()
	if err != nil {
		return 0, err
	}
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return 0, err
	}
	if pool, err = pgxpool.New(ctx, dsn); err != nil {
		return 0, err
	}
	defer pool.Close()
	return m.Run(), nil
}

// --- inventory / catalog giả ---------------------------------------------------------------

type fakeInventory struct {
	inventoryv1.InventoryServiceClient
	mu      sync.Mutex
	holds   map[string]*inventoryv1.Hold
	getErr  error // lỗi trả cho GetHold (mô phỏng inventory sập)
	extErr  error // lỗi trả cho ExtendHold (mô phỏng hold vừa hết hạn)
	extends []time.Duration
}

func (f *fakeInventory) GetHold(_ context.Context, in *inventoryv1.GetHoldRequest, _ ...grpc.CallOption) (*inventoryv1.GetHoldResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	h, ok := f.holds[in.GetHoldId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "hold not found")
	}
	// Trả bản sao như server gRPC thật (message luôn được serialize), để các goroutine không
	// dùng chung con trỏ với dữ liệu của fake.
	return &inventoryv1.GetHoldResponse{Hold: proto.CloneOf(h)}, nil
}

func (f *fakeInventory) ExtendHold(_ context.Context, in *inventoryv1.ExtendHoldRequest, _ ...grpc.CallOption) (*inventoryv1.ExtendHoldResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.extErr != nil {
		return nil, f.extErr
	}
	f.extends = append(f.extends, in.GetTtl().AsDuration())
	h := f.holds[in.GetHoldId()]
	h.ExpiresAt = timestamppb.New(time.Now().Add(in.GetTtl().AsDuration()))
	return &inventoryv1.ExtendHoldResponse{Hold: proto.CloneOf(h)}, nil
}

type fakeCatalog struct {
	catalogv1.CatalogServiceClient
	event *catalogv1.Event
	seats []*catalogv1.Seat
}

func (f *fakeCatalog) GetEvent(context.Context, *catalogv1.GetEventRequest, ...grpc.CallOption) (*catalogv1.GetEventResponse, error) {
	return &catalogv1.GetEventResponse{Event: f.event}, nil
}

func (f *fakeCatalog) ListSeats(context.Context, *catalogv1.ListSeatsRequest, ...grpc.CallOption) (*catalogv1.ListSeatsResponse, error) {
	return &catalogv1.ListSeatsResponse{Seats: f.seats}, nil
}

const (
	priceVIP = 2_000_000
	priceStd = 800_000
)

// fixture: một event có 1 ghế VIP + 2 ghế Standard, và inventory giả chưa có hold nào.
type fixture struct {
	svc       *service.Service
	inv       *fakeInventory
	eventID   string
	vip, std1 string
	std2      string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	if pool == nil {
		t.Skip("integration test: chạy không có -short")
	}
	f := &fixture{
		inv:     &fakeInventory{holds: map[string]*inventoryv1.Hold{}},
		eventID: uuid.NewString(),
		vip:     uuid.NewString(),
		std1:    uuid.NewString(),
		std2:    uuid.NewString(),
	}
	vipTier, stdTier := uuid.NewString(), uuid.NewString()
	cat := &fakeCatalog{
		event: &catalogv1.Event{Id: f.eventID, Tiers: []*catalogv1.TicketTier{
			{Id: vipTier, Name: "VIP", PriceAmount: priceVIP, Currency: "VND"},
			{Id: stdTier, Name: "Standard", PriceAmount: priceStd, Currency: "VND"},
		}},
		seats: []*catalogv1.Seat{
			{Id: f.vip, TierId: vipTier}, {Id: f.std1, TierId: stdTier}, {Id: f.std2, TierId: stdTier},
		},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f.svc = service.New(pool, f.inv, cat, service.Config{
		PaymentTimeout: 10 * time.Minute, HoldBuffer: 2 * time.Minute, CallTimeout: time.Second,
	}, log)
	return f
}

// addHold tạo hold active của user trong inventory giả.
func (f *fixture) addHold(user uuid.UUID, seats ...string) uuid.UUID {
	id := uuid.New()
	f.inv.mu.Lock()
	defer f.inv.mu.Unlock()
	f.inv.holds[id.String()] = &inventoryv1.Hold{
		Id: id.String(), EventId: f.eventID, UserId: user.String(), SeatIds: seats,
		Status:    inventoryv1.HoldStatus_HOLD_STATUS_ACTIVE,
		ExpiresAt: timestamppb.New(time.Now().Add(5 * time.Minute)),
	}
	return id
}

// --- test ----------------------------------------------------------------------------------

// Happy path: đơn AWAITING_PAYMENT đúng tiền, hold được gia hạn, outbox có payment.requested,
// saga log có bước order_created.
func TestCreateOrder(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	user := uuid.New()
	holdID := f.addHold(user, f.vip, f.std1)

	before := time.Now()
	o, err := f.svc.CreateOrder(ctx, service.CreateRequest{UserID: user, HoldID: holdID, IdempotencyKey: "k1"})
	must(t, err)
	if o.Status != service.StatusAwaitingPayment || o.Amount != priceVIP+priceStd || o.Currency != "VND" {
		t.Fatalf("đơn sai: status=%s amount=%d currency=%s", o.Status, o.Amount, o.Currency)
	}
	if d := o.PaymentDeadline.Sub(before); d < 10*time.Minute || d > 10*time.Minute+5*time.Second {
		t.Fatalf("payment_deadline lệch: +%s", d)
	}
	if len(f.inv.extends) != 1 || f.inv.extends[0] != 12*time.Minute {
		t.Fatalf("ExtendHold muốn 1 lần ttl 12m, nhận %v", f.inv.extends)
	}

	var (
		topic, key, typ string
		payload         []byte
	)
	must(t, pool.QueryRow(ctx, `SELECT topic, key, type, payload FROM outbox WHERE aggregate_id = $1 AND published_at IS NULL`, o.ID).
		Scan(&topic, &key, &typ, &payload))
	if topic != service.TopicPaymentCommands || key != o.ID.String() || typ != service.TypePaymentRequested {
		t.Fatalf("outbox sai: topic=%s key=%s type=%s", topic, key, typ)
	}
	var msg service.PaymentRequested
	must(t, json.Unmarshal(payload, &msg))
	if msg.OrderID != o.ID || msg.UserID != user || msg.Amount != o.Amount || !msg.PaymentDeadline.Equal(o.PaymentDeadline) {
		t.Fatalf("payload sai: %+v", msg)
	}

	var step string
	must(t, pool.QueryRow(ctx, `SELECT step FROM order_saga_log WHERE order_id = $1`, o.ID).Scan(&step))
	if step != "order_created" {
		t.Fatalf("saga log step %s", step)
	}

	got, err := f.svc.GetOrder(ctx, o.ID)
	must(t, err)
	if got.ID != o.ID {
		t.Fatal("GetOrder trả đơn khác")
	}
	_, err = f.svc.GetOrder(ctx, uuid.New())
	wantErr(t, err, service.ErrNotFound)
}

// Client retry dồn dập cùng key: chỉ một đơn, một message outbox. Cùng key khác hold: lỗi client.
func TestCreateOrder_Idempotency(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	user := uuid.New()
	holdID := f.addHold(user, f.std1)
	req := service.CreateRequest{UserID: user, HoldID: holdID, IdempotencyKey: "retry"}

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		ids = map[uuid.UUID]int{}
	)
	for range 20 {
		wg.Go(func() {
			o, err := f.svc.CreateOrder(ctx, req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Error(err)
				return
			}
			ids[o.ID]++
		})
	}
	wg.Wait()
	if len(ids) != 1 {
		t.Fatalf("muốn 1 đơn, nhận %v", ids)
	}
	var outbox int
	must(t, pool.QueryRow(ctx, `SELECT count(*) FROM outbox o JOIN orders d ON d.id = o.aggregate_id WHERE d.hold_id = $1`, holdID).Scan(&outbox))
	if outbox != 1 {
		t.Fatalf("outbox có %d message, muốn 1", outbox)
	}

	other := f.addHold(user, f.std2)
	_, err := f.svc.CreateOrder(ctx, service.CreateRequest{UserID: user, HoldID: other, IdempotencyKey: "retry"})
	wantErr(t, err, service.ErrIdempotencyMismatch)
}

// Một hold chỉ có một đơn, kể cả khi user đổi idempotency key.
func TestCreateOrder_OneOrderPerHold(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	user := uuid.New()
	holdID := f.addHold(user, f.vip)
	_, err := f.svc.CreateOrder(ctx, service.CreateRequest{UserID: user, HoldID: holdID, IdempotencyKey: "a"})
	must(t, err)

	_, err = f.svc.CreateOrder(ctx, service.CreateRequest{UserID: user, HoldID: holdID, IdempotencyKey: "b"})
	wantErr(t, err, service.ErrOrderExists)
	_, err = f.svc.CreateOrder(ctx, service.CreateRequest{UserID: uuid.New(), HoldID: holdID, IdempotencyKey: "c"})
	wantErr(t, err, service.ErrPermissionDenied)
}

// Hold không dùng được hoặc inventory lỗi: trả lỗi đúng loại và KHÔNG ghi gì vào DB.
func TestCreateOrder_HoldRejected(t *testing.T) {
	cases := map[string]struct {
		setup func(f *fixture, user uuid.UUID) uuid.UUID
		want  error
	}{
		"hold không tồn tại": {
			setup: func(*fixture, uuid.UUID) uuid.UUID { return uuid.New() },
			want:  service.ErrHoldNotFound,
		},
		"hold của người khác": {
			setup: func(f *fixture, _ uuid.UUID) uuid.UUID { return f.addHold(uuid.New(), f.vip) },
			want:  service.ErrPermissionDenied,
		},
		"hold đã hết hạn": {
			setup: func(f *fixture, user uuid.UUID) uuid.UUID {
				id := f.addHold(user, f.vip)
				f.inv.holds[id.String()].Status = inventoryv1.HoldStatus_HOLD_STATUS_EXPIRED
				return id
			},
			want: service.ErrHoldNotActive,
		},
		"hold hết hạn ngay trước ExtendHold": {
			setup: func(f *fixture, user uuid.UUID) uuid.UUID {
				f.inv.extErr = status.Error(codes.FailedPrecondition, "hold has expired")
				return f.addHold(user, f.vip)
			},
			want: service.ErrHoldNotActive,
		},
		"inventory không phản hồi": {
			setup: func(f *fixture, user uuid.UUID) uuid.UUID {
				f.inv.getErr = status.Error(codes.Unavailable, "connection refused")
				return f.addHold(user, f.vip)
			},
			want: service.ErrUnavailable,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			user := uuid.New()
			holdID := tc.setup(f, user)
			_, err := f.svc.CreateOrder(context.Background(), service.CreateRequest{UserID: user, HoldID: holdID, IdempotencyKey: "k"})
			wantErr(t, err, tc.want)
			var n int
			must(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM orders WHERE hold_id = $1`, holdID).Scan(&n))
			if n != 0 {
				t.Fatal("lỗi mà vẫn tạo đơn")
			}
		})
	}
}

func TestCreateOrder_Validation(t *testing.T) {
	f := newFixture(t)
	user, holdID := uuid.New(), uuid.New()
	long := string(make([]byte, 201))
	for name, req := range map[string]service.CreateRequest{
		"thiếu user":  {HoldID: holdID, IdempotencyKey: "k"},
		"thiếu hold":  {UserID: user, IdempotencyKey: "k"},
		"thiếu key":   {UserID: user, HoldID: holdID},
		"key quá dài": {UserID: user, HoldID: holdID, IdempotencyKey: long},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := f.svc.CreateOrder(context.Background(), req)
			wantErr(t, err, service.ErrInvalidArgument)
		})
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func wantErr(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("muốn lỗi %v, nhận %v", target, err)
	}
}
