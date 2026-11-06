package service

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Contract của các message Order trao đổi qua Redpanda. Payload là JSON trong cột outbox.payload;
// message_id và type đi kèm dưới dạng header khi relay publish (M4.4), để consumer chống trùng
// và phân loại mà không cần parse payload.
const (
	// TopicPaymentCommands: lệnh Order gửi cho Payment. Key = order_id để mọi lệnh của cùng một
	// đơn vào cùng partition, giữ đúng thứ tự (requested trước refund_requested).
	TopicPaymentCommands = "payment.commands"
	// TopicPaymentEvents: kết quả Payment gửi lại, Order consume (M6). Key = order_id.
	TopicPaymentEvents = "payment.events"
	// TopicOrderEvents: đơn đi tới trạng thái cuối. Chưa ai consume (Notification nằm ngoài phạm vi),
	// nhưng phát sẵn để service khác đăng ký mà không phải sửa order.
	TopicOrderEvents = "order.events"

	TypePaymentRequested       = "payment.requested"
	TypePaymentRefundRequested = "payment.refund_requested"

	TypePaymentSucceeded = "payment.succeeded"
	TypePaymentFailed    = "payment.failed"
	TypePaymentRefunded  = "payment.refunded"

	TypeOrderConfirmed = "order.confirmed"
	TypeOrderCancelled = "order.cancelled"
	TypeOrderRefunded  = "order.refunded"
)

// PaymentRequested: Payment tạo payment_intent cho đơn (idempotent theo order_id).
//
// [Go] Phần `json:"order_id"` là "struct tag": metadata gắn vào field, encoding/json đọc nó để
// đặt tên key khi encode/decode (giống @Expose({ name }) của class-transformer).
type PaymentRequested struct {
	OrderID         uuid.UUID `json:"order_id"`
	UserID          uuid.UUID `json:"user_id"`
	Amount          int64     `json:"amount"`
	Currency        string    `json:"currency"`
	PaymentDeadline time.Time `json:"payment_deadline"`
}

// PaymentRefundRequested: tiền đã thu nhưng đơn không giữ được ghế -> Payment hoàn tiền.
type PaymentRefundRequested struct {
	OrderID         uuid.UUID `json:"order_id"`
	PaymentIntentID uuid.UUID `json:"payment_intent_id"`
	Amount          int64     `json:"amount"`
	Currency        string    `json:"currency"`
	Reason          string    `json:"reason"`
}

// PaymentResult: payload chung của payment.succeeded / payment.failed / payment.refunded, khớp
// interface PaymentResult bên payment (services/payment/src/payments/events.ts).
type PaymentResult struct {
	OrderID         uuid.UUID `json:"order_id"`
	PaymentIntentID uuid.UUID `json:"payment_intent_id"`
	Amount          int64     `json:"amount"`
	Currency        string    `json:"currency"`
	FailureReason   string    `json:"failure_reason"` // rỗng trừ payment.failed
	OccurredAt      time.Time `json:"occurred_at"`
}

// OrderEvent: payload các message trên order.events.
type OrderEvent struct {
	OrderID       uuid.UUID   `json:"order_id"`
	UserID        uuid.UUID   `json:"user_id"`
	EventID       uuid.UUID   `json:"event_id"`
	SeatIDs       []uuid.UUID `json:"seat_ids"`
	Amount        int64       `json:"amount"`
	Currency      string      `json:"currency"`
	Status        string      `json:"status"`
	FailureReason string      `json:"failure_reason"`
}

// ParsePaymentResult đọc payload từ payment.events; sai contract thì trả ErrInvalidMessage (retry
// cũng vô ích, consumer đẩy thẳng sang DLQ).
func ParsePaymentResult(value []byte) (PaymentResult, error) {
	var p PaymentResult
	if err := json.Unmarshal(value, &p); err != nil {
		return PaymentResult{}, fmt.Errorf("%w: payload: %v", ErrInvalidMessage, err)
	}
	switch {
	case p.OrderID == uuid.Nil:
		return PaymentResult{}, fmt.Errorf("%w: order_id is required", ErrInvalidMessage)
	case p.PaymentIntentID == uuid.Nil:
		return PaymentResult{}, fmt.Errorf("%w: payment_intent_id is required", ErrInvalidMessage)
	}
	return p, nil
}
