package service

import (
	"time"

	"github.com/google/uuid"
)

// Contract của các message Order gửi qua Redpanda. Payload là JSON trong cột outbox.payload;
// message_id và type đi kèm dưới dạng header khi relay publish (M4.4), để consumer chống trùng
// và phân loại mà không cần parse payload.
const (
	// TopicPaymentCommands: lệnh Order gửi cho Payment. Key = order_id để mọi lệnh của cùng một
	// đơn vào cùng partition, giữ đúng thứ tự (requested trước refund_requested).
	TopicPaymentCommands = "payment.commands"

	TypePaymentRequested = "payment.requested"
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
