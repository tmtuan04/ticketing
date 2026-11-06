package service

// Nửa sau của Saga (M6): phản ứng với kết quả thanh toán từ payment.events và hủy đơn quá hạn.
//
// Mỗi bước chạy trong MỘT transaction:
//
//	BEGIN
//	  INSERT inbox(message_id)            -- trùng => message đã xử lý, bỏ qua
//	  SELECT order FOR UPDATE             -- khóa đơn
//	  gọi inventory (ConfirmHold / ReleaseHold), cả hai đều idempotent
//	  UPDATE orders + order_saga_log + outbox
//	COMMIT
//
// Gọi inventory TRONG lúc giữ khóa đơn: đổi lại transaction dài thêm một lời gọi gRPC (có
// CallTimeout), nhưng consumer và timeout worker không bao giờ chen nhau trên cùng một đơn (worker
// dùng SKIP LOCKED nên bỏ qua đơn đang khóa). Process chết sau khi inventory đã đổi mà trước COMMIT:
// message được giao lại, inbox chưa có (đã ROLLBACK), đơn vẫn ở trạng thái cũ, và lời gọi inventory
// lặp lại trả cùng kết quả nhờ idempotent.
//
// Trạng thái PAID chỉ tồn tại bên trong transaction: payment.succeeded và ConfirmHold xảy ra cùng
// lúc nên đơn đi thẳng AWAITING_PAYMENT -> CONFIRMED (hoặc -> REFUNDING). order_saga_log vẫn ghi
// bước payment_succeeded với status paid để thấy đủ các bước.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	inventoryv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/inventory/v1"
	"github.com/tmtuan04/ticketing/order/internal/repo"
)

// Các bước ghi vào order_saga_log (ngoài stepOrderCreated).
const (
	stepPaymentSucceeded = "payment_succeeded"
	stepHoldConfirmed    = "hold_confirmed"
	stepRefundRequested  = "refund_requested"
	stepPaymentFailed    = "payment_failed"
	stepPaymentTimeout   = "payment_timeout"
	stepPaymentRefunded  = "payment_refunded"
)

// Giá trị orders.failure_reason.
const (
	ReasonPaymentFailed  = "payment_failed"
	ReasonPaymentTimeout = "payment_timeout"
	// Tiền về nhưng hold đã hết hạn/bị nhả nên không bán được ghế -> hoàn tiền.
	ReasonHoldExpired = "hold_expired"
	// Tiền về sau khi đơn đã bị hủy (thường do quá payment_deadline) -> hoàn tiền.
	ReasonPaidAfterCancel = "paid_after_cancel"
)

// IncomingMessage là một record đọc từ Redpanda, đã tách header.
type IncomingMessage struct {
	MessageID uuid.UUID
	Topic     string
	Type      string
	Value     []byte
}

// HandleResult cho consumer biết message đã được xử lý thế nào (để log).
type HandleResult string

const (
	ResultProcessed HandleResult = "processed"
	ResultDuplicate HandleResult = "duplicate" // message_id đã có trong inbox
	ResultIgnored   HandleResult = "ignored"   // type lạ, hoặc đơn không còn ở trạng thái chờ sự kiện này
)

// HandlePaymentEvent áp một message từ payment.events lên đơn.
//
// Lỗi trả về: ErrInvalidMessage (sai contract, đẩy DLQ), ErrUnavailable (inventory lỗi, nên thử
// lại), hoặc lỗi DB. Mọi lỗi đều ROLLBACK cả inbox, nên giao lại message là an toàn.
func (s *Service) HandlePaymentEvent(ctx context.Context, msg IncomingMessage) (HandleResult, error) {
	known := msg.Type == TypePaymentSucceeded || msg.Type == TypePaymentFailed || msg.Type == TypePaymentRefunded
	var p PaymentResult
	if known {
		// Parse trước transaction: payload hỏng thì không cần mở transaction.
		var err error
		if p, err = ParsePaymentResult(msg.Value); err != nil {
			return "", err
		}
	}

	var result HandleResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		fresh, err := q.InsertInbox(ctx, repo.InsertInboxParams{MessageID: msg.MessageID, Topic: msg.Topic})
		if err != nil {
			return err
		}
		if fresh == 0 {
			result = ResultDuplicate
			return nil
		}
		if !known {
			// Vẫn ghi inbox (COMMIT) để không xử lý lại mãi; type mới thêm sau sẽ cần deploy order mới.
			s.log.WarnContext(ctx, "ignoring unknown message type", "type", msg.Type, "message_id", msg.MessageID)
			result = ResultIgnored
			return nil
		}
		order, err := q.LockOrder(ctx, p.OrderID)
		if errors.Is(err, pgx.ErrNoRows) {
			// payment.requested chỉ được publish sau khi đơn đã commit, nên không có chuyện sự kiện
			// đến trước đơn: đơn không tồn tại nghĩa là message sai.
			return fmt.Errorf("%w: order %s not found", ErrInvalidMessage, p.OrderID)
		}
		if err != nil {
			return err
		}
		switch msg.Type {
		case TypePaymentSucceeded:
			result, err = s.onPaymentSucceeded(ctx, q, order, p, msg.MessageID)
		case TypePaymentFailed:
			result, err = s.onPaymentFailed(ctx, q, order, p, msg.MessageID)
		default:
			result, err = s.onPaymentRefunded(ctx, q, order, p, msg.MessageID)
		}
		return err
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

// onPaymentSucceeded: xác nhận ghế. Không xác nhận được (hold hết hạn, hoặc đơn đã bị hủy) thì
// đơn chuyển REFUNDING và Payment nhận lệnh hoàn tiền.
func (s *Service) onPaymentSucceeded(ctx context.Context, q *repo.Queries, o repo.Order, p PaymentResult, mid uuid.UUID) (HandleResult, error) {
	paid := step{stepPaymentSucceeded, StatusPaid, map[string]any{"payment_intent_id": p.PaymentIntentID, "message_id": mid}}
	switch o.Status {
	case StatusAwaitingPayment:
		err := s.confirmHold(ctx, o.HoldID)
		switch {
		case err == nil:
			o, err = s.advance(ctx, q, o, StatusConfirmed, "",
				[]step{paid, {stepHoldConfirmed, StatusConfirmed, nil}},
				outboxMsg{TopicOrderEvents, TypeOrderConfirmed, nil})
			if err != nil {
				return "", err
			}
			s.log.InfoContext(ctx, "order confirmed", "order_id", o.ID, "hold_id", o.HoldID)
			return ResultProcessed, nil
		case errors.Is(err, ErrHoldNotActive), errors.Is(err, ErrHoldNotFound):
			return s.requestRefund(ctx, q, o, p, ReasonHoldExpired, paid)
		default:
			return "", err
		}
	case StatusCancelled:
		// Ghế đã được nhả lúc hủy đơn, chỉ còn cách trả tiền lại.
		paid.status = StatusCancelled
		return s.requestRefund(ctx, q, o, p, ReasonPaidAfterCancel, paid)
	default:
		// Payment chỉ phát một kết quả cho mỗi intent, nên tới đây là bất thường (vd sửa tay DB).
		s.log.WarnContext(ctx, "payment.succeeded for order not awaiting payment", "order_id", o.ID, "status", o.Status)
		return ResultIgnored, nil
	}
}

func (s *Service) requestRefund(ctx context.Context, q *repo.Queries, o repo.Order, p PaymentResult, reason string, before step) (HandleResult, error) {
	o, err := s.advance(ctx, q, o, StatusRefunding, reason,
		[]step{before, {stepRefundRequested, StatusRefunding, map[string]any{"reason": reason}}},
		outboxMsg{TopicPaymentCommands, TypePaymentRefundRequested, PaymentRefundRequested{
			OrderID: o.ID, PaymentIntentID: p.PaymentIntentID, Amount: p.Amount, Currency: p.Currency, Reason: reason,
		}})
	if err != nil {
		return "", err
	}
	s.log.WarnContext(ctx, "payment succeeded but seats not confirmed, refunding", "order_id", o.ID, "reason", reason)
	return ResultProcessed, nil
}

// onPaymentFailed: nhả ghế và hủy đơn.
func (s *Service) onPaymentFailed(ctx context.Context, q *repo.Queries, o repo.Order, p PaymentResult, mid uuid.UUID) (HandleResult, error) {
	if o.Status != StatusAwaitingPayment {
		// Thường là đơn đã bị timeout worker hủy trước khi kết quả về: không còn gì phải làm.
		s.log.InfoContext(ctx, "payment.failed for order not awaiting payment", "order_id", o.ID, "status", o.Status)
		return ResultIgnored, nil
	}
	// ErrHoldNotFound: không còn gì để nhả, vẫn hủy đơn. ErrHoldNotActive ở đây nghĩa là hold đã bán
	// trong khi đơn chưa trả tiền: dữ liệu lệch, trả lỗi để consumer retry rồi đẩy DLQ.
	if err := s.releaseHold(ctx, o); err != nil && !errors.Is(err, ErrHoldNotFound) {
		return "", err
	}
	o, err := s.advance(ctx, q, o, StatusCancelled, ReasonPaymentFailed,
		[]step{{stepPaymentFailed, StatusCancelled, map[string]any{
			"payment_intent_id": p.PaymentIntentID, "provider_reason": p.FailureReason, "message_id": mid,
		}}},
		outboxMsg{TopicOrderEvents, TypeOrderCancelled, nil})
	if err != nil {
		return "", err
	}
	s.log.InfoContext(ctx, "order cancelled", "order_id", o.ID, "reason", ReasonPaymentFailed)
	return ResultProcessed, nil
}

// onPaymentRefunded: kết thúc nhánh hoàn tiền. failure_reason giữ lý do vì sao phải hoàn.
func (s *Service) onPaymentRefunded(ctx context.Context, q *repo.Queries, o repo.Order, p PaymentResult, mid uuid.UUID) (HandleResult, error) {
	if o.Status != StatusRefunding {
		s.log.WarnContext(ctx, "payment.refunded for order not refunding", "order_id", o.ID, "status", o.Status)
		return ResultIgnored, nil
	}
	o, err := s.advance(ctx, q, o, StatusRefunded, o.FailureReason,
		[]step{{stepPaymentRefunded, StatusRefunded, map[string]any{"payment_intent_id": p.PaymentIntentID, "message_id": mid}}},
		outboxMsg{TopicOrderEvents, TypeOrderRefunded, nil})
	if err != nil {
		return "", err
	}
	s.log.InfoContext(ctx, "order refunded", "order_id", o.ID, "reason", o.FailureReason)
	return ResultProcessed, nil
}

// ExpireDueOrders hủy tối đa `limit` đơn đã quá payment_deadline mà chưa có kết quả thanh toán:
// nhả hold rồi chuyển CANCELLED. Trả về số đơn đã hủy. Exported để test gọi trực tiếp, giống
// ExpireDueHolds của inventory.
//
// Cả lô nằm trong một transaction giữ khóa các đơn. Đơn nào inventory từ chối nhả (hold đã bán:
// consumer đang xử lý dở payment.succeeded của đơn đó) thì bỏ qua, để lại cho consumer. Inventory
// không phản hồi thì dừng lô nhưng vẫn COMMIT các đơn đã nhả xong.
func (s *Service) ExpireDueOrders(ctx context.Context, limit int32) (int, error) {
	var (
		n       int
		stopErr error
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		due, err := q.LockDueOrders(ctx, limit)
		if err != nil {
			return err
		}
		for _, o := range due {
			err := s.releaseHold(ctx, o)
			switch {
			case err == nil, errors.Is(err, ErrHoldNotFound):
			case errors.Is(err, ErrUnavailable), ctx.Err() != nil:
				stopErr = err
				return nil
			default:
				s.log.WarnContext(ctx, "skip expiring order", "order_id", o.ID, "hold_id", o.HoldID, "err", err)
				continue
			}
			o, err = s.advance(ctx, q, o, StatusCancelled, ReasonPaymentTimeout,
				[]step{{stepPaymentTimeout, StatusCancelled, map[string]any{"payment_deadline": o.PaymentDeadline}}},
				outboxMsg{TopicOrderEvents, TypeOrderCancelled, nil})
			if err != nil {
				return err
			}
			s.log.InfoContext(ctx, "order cancelled", "order_id", o.ID, "reason", ReasonPaymentTimeout)
			n++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, stopErr
}

// step là một dòng order_saga_log.
type step struct {
	name   string
	status string // trạng thái đơn sau bước này
	detail map[string]any
}

// outboxMsg là message cần publish. payload nil = OrderEvent dựng từ đơn sau khi cập nhật.
type outboxMsg struct {
	topic   string
	typ     string
	payload any
}

// advance đổi trạng thái đơn (đang bị khóa), ghi saga log và outbox, tất cả trên q của transaction.
func (s *Service) advance(ctx context.Context, q *repo.Queries, o repo.Order, to, reason string, steps []step, msgs ...outboxMsg) (repo.Order, error) {
	o, err := q.UpdateOrderStatus(ctx, repo.UpdateOrderStatusParams{ID: o.ID, Status: to, FailureReason: reason})
	if err != nil {
		return repo.Order{}, err
	}
	for _, st := range steps {
		detail := []byte("{}")
		if st.detail != nil {
			if detail, err = json.Marshal(st.detail); err != nil {
				return repo.Order{}, err
			}
		}
		if err := q.InsertSagaLog(ctx, repo.InsertSagaLogParams{OrderID: o.ID, Step: st.name, Status: st.status, Detail: detail}); err != nil {
			return repo.Order{}, err
		}
	}
	for _, m := range msgs {
		payload := m.payload
		if payload == nil {
			payload = orderEvent(o)
		}
		if err := enqueue(ctx, q, o.ID, m.topic, m.typ, payload); err != nil {
			return repo.Order{}, err
		}
	}
	return o, nil
}

// enqueue ghi một message vào outbox. Luôn gọi bên trong transaction nghiệp vụ.
func enqueue(ctx context.Context, q *repo.Queries, orderID uuid.UUID, topic, typ string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return q.InsertOutbox(ctx, repo.InsertOutboxParams{
		MessageID:   uuid.New(),
		AggregateID: orderID,
		Topic:       topic,
		Key:         orderID.String(),
		Type:        typ,
		Payload:     b,
	})
}

func orderEvent(o repo.Order) OrderEvent {
	return OrderEvent{
		OrderID: o.ID, UserID: o.UserID, EventID: o.EventID, SeatIDs: o.SeatIds,
		Amount: o.Amount, Currency: o.Currency, Status: o.Status, FailureReason: o.FailureReason,
	}
}

// confirmHold: held -> sold. Hold hết hạn/đã nhả -> ErrHoldNotActive.
func (s *Service) confirmHold(ctx context.Context, holdID uuid.UUID) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.CallTimeout)
	defer cancel()
	if _, err := s.inventory.ConfirmHold(ctx, &inventoryv1.ConfirmHoldRequest{HoldId: holdID.String()}); err != nil {
		return fromInventory("ConfirmHold", err)
	}
	return nil
}

// releaseHold nhả hold của đơn. Idempotent phía inventory: hold đã nhả/hết hạn thì vẫn thành công.
func (s *Service) releaseHold(ctx context.Context, o repo.Order) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.CallTimeout)
	defer cancel()
	if _, err := s.inventory.ReleaseHold(ctx, &inventoryv1.ReleaseHoldRequest{HoldId: o.HoldID.String(), UserId: o.UserID.String()}); err != nil {
		return fromInventory("ReleaseHold", err)
	}
	return nil
}
