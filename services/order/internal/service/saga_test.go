// Integration test cho Saga (saga.go): Postgres THẬT + inventory giả, dùng chung TestMain và
// fixture với service_test.go.
package service_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/inventory/v1"
	"github.com/tmtuan04/ticketing/order/internal/repo"
	"github.com/tmtuan04/ticketing/order/internal/service"
)

// --- helper --------------------------------------------------------------------------------

// newOrder tạo đơn AWAITING_PAYMENT cho một user mới, giữ ghế VIP.
func (f *fixture) newOrder(t *testing.T) repo.Order {
	t.Helper()
	user := uuid.New()
	o, err := f.svc.CreateOrder(context.Background(), service.CreateRequest{
		UserID: user, HoldID: f.addHold(user, f.vip), IdempotencyKey: "k",
	})
	must(t, err)
	return o
}

// intentID: payment_intent_id cố định theo đơn, như Payment (một intent mỗi đơn).
func intentID(o repo.Order) uuid.UUID { return uuid.NewSHA1(uuid.Nil, o.ID[:]) }

// paymentMsg dựng message payment.events như Payment publish, message_id mới mỗi lần.
func paymentMsg(t *testing.T, typ string, o repo.Order, failureReason string) service.IncomingMessage {
	t.Helper()
	b, err := json.Marshal(service.PaymentResult{
		OrderID: o.ID, PaymentIntentID: intentID(o), Amount: o.Amount, Currency: o.Currency,
		FailureReason: failureReason, OccurredAt: time.Now(),
	})
	must(t, err)
	return service.IncomingMessage{MessageID: uuid.New(), Topic: service.TopicPaymentEvents, Type: typ, Value: b}
}

func handle(t *testing.T, f *fixture, msg service.IncomingMessage, want service.HandleResult) {
	t.Helper()
	got, err := f.svc.HandlePaymentEvent(context.Background(), msg)
	must(t, err)
	if got != want {
		t.Fatalf("HandlePaymentEvent: muốn %s, nhận %s", want, got)
	}
}

func wantOrder(t *testing.T, id uuid.UUID, status, reason string) repo.Order {
	t.Helper()
	o, err := repo.New(pool).GetOrder(context.Background(), id)
	must(t, err)
	if o.Status != status || o.FailureReason != reason {
		t.Fatalf("đơn %s: muốn %s/%q, nhận %s/%q", id, status, reason, o.Status, o.FailureReason)
	}
	return o
}

// outboxTypes: type các message outbox của đơn, theo thứ tự ghi.
func outboxTypes(t *testing.T, id uuid.UUID) []string {
	t.Helper()
	return column(t, `SELECT type FROM outbox WHERE aggregate_id = $1 ORDER BY id`, id)
}

func sagaSteps(t *testing.T, id uuid.UUID) []string {
	t.Helper()
	return column(t, `SELECT step || ':' || status FROM order_saga_log WHERE order_id = $1 ORDER BY id`, id)
}

func column(t *testing.T, sql string, id uuid.UUID) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), sql, id)
	must(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		must(t, rows.Scan(&s))
		out = append(out, s)
	}
	must(t, rows.Err())
	return out
}

func wantList(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: muốn %v, nhận %v", what, want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: muốn %v, nhận %v", what, want, got)
		}
	}
}

// expire đẩy payment_deadline của đơn về quá khứ để timeout worker nhìn thấy.
func expire(t *testing.T, ids ...uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `UPDATE orders SET payment_deadline = now() - interval '1 second' WHERE id = ANY($1)`, ids)
	must(t, err)
}

func inboxHas(t *testing.T, mid uuid.UUID) bool {
	t.Helper()
	var n int
	must(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM inbox WHERE message_id = $1`, mid).Scan(&n))
	return n == 1
}

// --- test ----------------------------------------------------------------------------------

// Happy path: payment.succeeded -> ghế bán, đơn CONFIRMED, phát order.confirmed. Giao lại cùng
// message (relay publish lại / consumer chết trước khi commit offset) không có tác dụng gì thêm.
func TestSaga_PaymentSucceeded(t *testing.T) {
	f := newFixture(t)
	o := f.newOrder(t)
	msg := paymentMsg(t, service.TypePaymentSucceeded, o, "")

	handle(t, f, msg, service.ResultProcessed)
	got := wantOrder(t, o.ID, service.StatusConfirmed, "")
	if got.Version != o.Version+1 {
		t.Fatalf("version %d -> %d, muốn tăng 1", o.Version, got.Version)
	}
	if f.inv.holdStatus(o.HoldID) != inventoryv1.HoldStatus_HOLD_STATUS_CONFIRMED {
		t.Fatal("hold chưa được confirm")
	}
	wantList(t, "outbox", outboxTypes(t, o.ID), service.TypePaymentRequested, service.TypeOrderConfirmed)
	wantList(t, "saga log", sagaSteps(t, o.ID),
		"order_created:awaiting_payment", "payment_succeeded:paid", "hold_confirmed:confirmed")

	var ev service.OrderEvent
	var payload []byte
	must(t, pool.QueryRow(context.Background(), `SELECT payload FROM outbox WHERE aggregate_id = $1 AND type = $2`,
		o.ID, service.TypeOrderConfirmed).Scan(&payload))
	must(t, json.Unmarshal(payload, &ev))
	if ev.OrderID != o.ID || ev.Status != service.StatusConfirmed || ev.Amount != o.Amount || len(ev.SeatIDs) != 1 {
		t.Fatalf("order.confirmed sai: %+v", ev)
	}

	handle(t, f, msg, service.ResultDuplicate)
	if f.inv.confirms != 1 {
		t.Fatalf("ConfirmHold bị gọi %d lần", f.inv.confirms)
	}
	wantList(t, "outbox", outboxTypes(t, o.ID), service.TypePaymentRequested, service.TypeOrderConfirmed)
}

// Tiền về nhưng hold đã hết hạn: REFUNDING + lệnh hoàn tiền; payment.refunded -> REFUNDED.
func TestSaga_PaymentSucceededAfterHoldExpired(t *testing.T) {
	f := newFixture(t)
	o := f.newOrder(t)
	f.inv.setHoldStatus(o.HoldID, inventoryv1.HoldStatus_HOLD_STATUS_EXPIRED)

	handle(t, f, paymentMsg(t, service.TypePaymentSucceeded, o, ""), service.ResultProcessed)
	wantOrder(t, o.ID, service.StatusRefunding, service.ReasonHoldExpired)
	wantList(t, "outbox", outboxTypes(t, o.ID), service.TypePaymentRequested, service.TypePaymentRefundRequested)

	var (
		topic, key string
		payload    []byte
	)
	must(t, pool.QueryRow(context.Background(), `SELECT topic, key, payload FROM outbox WHERE aggregate_id = $1 AND type = $2`,
		o.ID, service.TypePaymentRefundRequested).Scan(&topic, &key, &payload))
	var cmd service.PaymentRefundRequested
	must(t, json.Unmarshal(payload, &cmd))
	if topic != service.TopicPaymentCommands || key != o.ID.String() ||
		cmd.PaymentIntentID != intentID(o) || cmd.Amount != o.Amount || cmd.Reason != service.ReasonHoldExpired {
		t.Fatalf("refund_requested sai: topic=%s key=%s %+v", topic, key, cmd)
	}

	handle(t, f, paymentMsg(t, service.TypePaymentRefunded, o, ""), service.ResultProcessed)
	wantOrder(t, o.ID, service.StatusRefunded, service.ReasonHoldExpired)
	wantList(t, "saga log", sagaSteps(t, o.ID), "order_created:awaiting_payment", "payment_succeeded:paid",
		"refund_requested:refunding", "payment_refunded:refunded")
	wantList(t, "outbox", outboxTypes(t, o.ID),
		service.TypePaymentRequested, service.TypePaymentRefundRequested, service.TypeOrderRefunded)
}

// Thanh toán lỗi: nhả ghế, CANCELLED. Sự kiện đến sau cho đơn đã kết thúc thì bỏ qua.
func TestSaga_PaymentFailed(t *testing.T) {
	f := newFixture(t)
	o := f.newOrder(t)

	handle(t, f, paymentMsg(t, service.TypePaymentFailed, o, "card_declined"), service.ResultProcessed)
	wantOrder(t, o.ID, service.StatusCancelled, service.ReasonPaymentFailed)
	if f.inv.holdStatus(o.HoldID) != inventoryv1.HoldStatus_HOLD_STATUS_RELEASED {
		t.Fatal("hold chưa được nhả")
	}
	wantList(t, "outbox", outboxTypes(t, o.ID), service.TypePaymentRequested, service.TypeOrderCancelled)

	handle(t, f, paymentMsg(t, service.TypePaymentFailed, o, "card_declined"), service.ResultIgnored)
	handle(t, f, paymentMsg(t, service.TypePaymentRefunded, o, ""), service.ResultIgnored)
	wantOrder(t, o.ID, service.StatusCancelled, service.ReasonPaymentFailed)
}

// Quá payment_deadline: timeout worker nhả ghế và hủy đơn. Tiền về muộn sau đó -> hoàn tiền,
// không đụng tới inventory (ghế có thể đã thuộc về người khác).
func TestSaga_TimeoutThenLatePayment(t *testing.T) {
	f := newFixture(t)
	o := f.newOrder(t)
	expire(t, o.ID)

	if _, err := f.svc.ExpireDueOrders(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	wantOrder(t, o.ID, service.StatusCancelled, service.ReasonPaymentTimeout)
	if f.inv.holdStatus(o.HoldID) != inventoryv1.HoldStatus_HOLD_STATUS_RELEASED {
		t.Fatal("hold chưa được nhả")
	}

	handle(t, f, paymentMsg(t, service.TypePaymentSucceeded, o, ""), service.ResultProcessed)
	wantOrder(t, o.ID, service.StatusRefunding, service.ReasonPaidAfterCancel)
	if f.inv.confirms != 0 {
		t.Fatal("không được confirm hold của đơn đã hủy")
	}
	handle(t, f, paymentMsg(t, service.TypePaymentRefunded, o, ""), service.ResultProcessed)
	wantOrder(t, o.ID, service.StatusRefunded, service.ReasonPaidAfterCancel)
	wantList(t, "saga log", sagaSteps(t, o.ID), "order_created:awaiting_payment", "payment_timeout:cancelled",
		"payment_succeeded:cancelled", "refund_requested:refunding", "payment_refunded:refunded")
}

// Inventory sập lúc xử lý: lỗi ErrUnavailable, mọi thứ ROLLBACK (cả inbox), nên giao lại cùng
// message sau khi inventory sống lại thì xử lý bình thường.
func TestSaga_InventoryDownRollsBack(t *testing.T) {
	f := newFixture(t)
	o := f.newOrder(t)
	msg := paymentMsg(t, service.TypePaymentSucceeded, o, "")

	f.inv.confirmErr = status.Error(codes.Unavailable, "connection refused")
	_, err := f.svc.HandlePaymentEvent(context.Background(), msg)
	wantErr(t, err, service.ErrUnavailable)
	wantOrder(t, o.ID, service.StatusAwaitingPayment, "")
	if inboxHas(t, msg.MessageID) {
		t.Fatal("lỗi mà inbox vẫn ghi message")
	}

	f.inv.confirmErr = nil
	handle(t, f, msg, service.ResultProcessed)
	wantOrder(t, o.ID, service.StatusConfirmed, "")
}

// Message sai contract -> ErrInvalidMessage (consumer đẩy DLQ), không ghi inbox. Type lạ thì ghi
// inbox rồi bỏ qua.
func TestSaga_InvalidMessages(t *testing.T) {
	f := newFixture(t)
	o := f.newOrder(t)
	bad := func(typ string, value string) service.IncomingMessage {
		return service.IncomingMessage{MessageID: uuid.New(), Topic: service.TopicPaymentEvents, Type: typ, Value: []byte(value)}
	}
	unknownOrder := paymentMsg(t, service.TypePaymentSucceeded, repo.Order{ID: uuid.New(), Amount: 1, Currency: "VND"}, "")
	for name, msg := range map[string]service.IncomingMessage{
		"không phải JSON":     bad(service.TypePaymentSucceeded, "{oops"),
		"thiếu order_id":      bad(service.TypePaymentFailed, `{"payment_intent_id":"`+uuid.NewString()+`"}`),
		"thiếu intent":        bad(service.TypePaymentRefunded, `{"order_id":"`+o.ID.String()+`"}`),
		"đơn không tồn tại":   unknownOrder,
		"order_id không uuid": bad(service.TypePaymentSucceeded, `{"order_id":"x","payment_intent_id":"y"}`),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := f.svc.HandlePaymentEvent(context.Background(), msg)
			wantErr(t, err, service.ErrInvalidMessage)
			if inboxHas(t, msg.MessageID) {
				t.Fatal("message lỗi mà vẫn ghi inbox")
			}
		})
	}

	unknown := bad("payment.something_new", "{}")
	handle(t, f, unknown, service.ResultIgnored)
	if !inboxHas(t, unknown.MessageID) {
		t.Fatal("type lạ phải được ghi inbox để không xử lý lại")
	}
	wantOrder(t, o.ID, service.StatusAwaitingPayment, "")
}

// Timeout worker: bỏ qua đơn có hold đã bán (consumer đang xử lý dở), dừng lô khi inventory sập
// nhưng không hủy đơn nào chưa nhả được ghế.
func TestExpireDueOrders_SkipsAndStops(t *testing.T) {
	f := newFixture(t)
	sold, normal := f.newOrder(t), f.newOrder(t)
	f.inv.setHoldStatus(sold.HoldID, inventoryv1.HoldStatus_HOLD_STATUS_CONFIRMED)
	expire(t, sold.ID, normal.ID)

	f.inv.releaseErr = status.Error(codes.Unavailable, "connection refused")
	_, err := f.svc.ExpireDueOrders(context.Background(), 100)
	wantErr(t, err, service.ErrUnavailable)
	wantOrder(t, normal.ID, service.StatusAwaitingPayment, "")

	f.inv.releaseErr = nil
	_, err = f.svc.ExpireDueOrders(context.Background(), 100)
	must(t, err)
	wantOrder(t, normal.ID, service.StatusCancelled, service.ReasonPaymentTimeout)
	wantOrder(t, sold.ID, service.StatusAwaitingPayment, "")

	// consumer xử lý xong payment.succeeded của đơn bị bỏ qua (ConfirmHold idempotent) -> CONFIRMED
	handle(t, f, paymentMsg(t, service.TypePaymentSucceeded, sold, ""), service.ResultProcessed)
	wantOrder(t, sold.ID, service.StatusConfirmed, "")
}

// Consumer và timeout worker tranh nhau trên các đơn vừa quá hạn: mỗi đơn chỉ có hai kết cục hợp
// lệ. Không bao giờ có đơn CONFIRMED mà ghế đã nhả, hay đơn bị hủy/hoàn tiền mà ghế đã bán.
func TestSaga_ConsumerRacesTimeoutWorker(t *testing.T) {
	f := newFixture(t)
	orders := make([]repo.Order, 30)
	ids := make([]uuid.UUID, len(orders))
	for i := range orders {
		orders[i] = f.newOrder(t)
		ids[i] = orders[i].ID
	}
	expire(t, ids...)

	var wg sync.WaitGroup
	for _, o := range orders {
		msg := paymentMsg(t, service.TypePaymentSucceeded, o, "")
		wg.Go(func() {
			if _, err := f.svc.HandlePaymentEvent(context.Background(), msg); err != nil {
				t.Error(err)
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			for range 5 {
				if _, err := f.svc.ExpireDueOrders(context.Background(), 7); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()

	outcomes := map[string]int{}
	for _, o := range orders {
		got, err := repo.New(pool).GetOrder(context.Background(), o.ID)
		must(t, err)
		hold := f.inv.holdStatus(o.HoldID)
		switch {
		case got.Status == service.StatusConfirmed && hold == inventoryv1.HoldStatus_HOLD_STATUS_CONFIRMED:
		case got.Status == service.StatusRefunding && got.FailureReason == service.ReasonPaidAfterCancel &&
			hold == inventoryv1.HoldStatus_HOLD_STATUS_RELEASED:
		default:
			t.Fatalf("đơn %s: status=%s reason=%s hold=%s", o.ID, got.Status, got.FailureReason, hold)
		}
		outcomes[got.Status]++
	}
	t.Logf("kết cục: %v", outcomes)
}
