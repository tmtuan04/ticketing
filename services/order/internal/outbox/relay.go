// Package outbox là nửa sau của Transactional Outbox: relay đọc các message mà nghiệp vụ đã ghi
// vào bảng outbox (cùng transaction với thay đổi của đơn) rồi publish lên Redpanda.
//
// Mỗi lô chạy trong MỘT transaction:
//
//	BEGIN
//	  SELECT ... WHERE published_at IS NULL ORDER BY id LIMIT n FOR UPDATE SKIP LOCKED
//	  publish cả lô lên Redpanda, chờ broker xác nhận (ack)
//	  UPDATE outbox SET published_at = now() WHERE id = ANY(...)
//	COMMIT
//
// Đảm bảo AT-LEAST-ONCE: publish lỗi -> ROLLBACK, lô vẫn chưa publish và được thử lại. Publish xong
// nhưng COMMIT lỗi (process chết đúng lúc đó) -> lô bị publish lại lần nữa. Vì vậy consumer phải
// chống trùng bằng header message_id (bảng inbox), không được giả định mỗi message đến đúng 1 lần.
//
// Thứ tự: trong một relay, message được publish theo id tăng dần và cùng key (order_id) vào cùng
// partition, nên consumer thấy đúng thứ tự. Nhiều relay song song (nhiều replica) thì hai message
// cùng một đơn có thể bị hai relay lấy và publish lệch thứ tự. Saga của order không gặp trường hợp
// này: mỗi lệnh mới (vd payment.refund_requested) chỉ được ghi SAU KHI nhận phản hồi của lệnh
// trước, lúc đó lệnh trước đã publish xong từ lâu.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tmtuan04/ticketing/order/internal/repo"
)

// Tên header Kafka đi kèm mỗi message. Consumer đọc header để chống trùng và phân loại mà không
// cần parse payload.
const (
	HeaderMessageID = "message_id"
	HeaderType      = "type"
)

// Producer là thứ relay cần từ Kafka client: gửi một lô record và chờ broker ack.
// *kgo.Client (franz-go) thỏa interface này.
type Producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// Relay giữ cấu hình vòng publish. Chạy nhiều Relay song song (cùng hoặc khác process) an toàn.
type Relay struct {
	pool       *pgxpool.Pool
	q          *repo.Queries
	producer   Producer
	interval   time.Duration
	batchSize  int32
	maxBackoff time.Duration
	log        *slog.Logger
}

func New(pool *pgxpool.Pool, producer Producer, interval time.Duration, batchSize int32, log *slog.Logger) *Relay {
	return &Relay{
		pool: pool, q: repo.New(pool), producer: producer,
		interval: interval, batchSize: batchSize, maxBackoff: 10 * time.Second, log: log,
	}
}

// Run chặn cho tới khi ctx bị hủy. Mỗi `interval` publish hết message đang chờ; khi Redpanda
// hoặc Postgres lỗi thì chờ tăng dần (interval, 2x, 4x... tối đa maxBackoff) để không dội lỗi.
//
// Polling đơn giản và đủ dùng: độ trễ tối đa ~interval (mặc định 500ms). Cách "xịn" hơn là
// LISTEN/NOTIFY hoặc CDC (Debezium đọc WAL), đổi lại phức tạp hơn nhiều.
func (r *Relay) Run(ctx context.Context) {
	wait := r.interval
	for {
		if err := r.drain(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			wait = min(wait*2, r.maxBackoff)
			r.log.WarnContext(ctx, "outbox relay failed, backing off", "err", err, "retry_in", wait.String())
		} else {
			wait = r.interval
		}
		// [Go] time.NewTimer thay vì time.After: Stop() giải phóng timer ngay khi ctx bị hủy.
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// drain publish từng lô cho tới khi hết message chờ (lô cuối không đầy) hoặc gặp lỗi.
func (r *Relay) drain(ctx context.Context) error {
	for {
		n, err := r.PublishBatch(ctx)
		if err != nil {
			return err
		}
		if n < int(r.batchSize) {
			return nil
		}
	}
}

// PublishBatch publish một lô trong một transaction, trả về số message đã publish.
// Exported để test gọi trực tiếp (nhanh, xác định), giống ExpireDueHolds của inventory.
func (r *Relay) PublishBatch(ctx context.Context) (int, error) {
	var n int
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		q := r.q.WithTx(tx)
		rows, err := q.LockUnpublished(ctx, r.batchSize)
		if err != nil || len(rows) == 0 {
			return err
		}

		records := make([]*kgo.Record, len(rows))
		ids := make([]int64, len(rows))
		for i, m := range rows {
			// KHÔNG gán Timestamp = created_at: franz-go tính RecordDeliveryTimeout từ Timestamp của
			// record, nên message tồn đọng lâu (vd Redpanda sập vài phút) sẽ bị coi là quá hạn ngay
			// khi gửi và không bao giờ publish được. Để trống thì franz-go gán thời điểm publish.
			records[i] = &kgo.Record{
				Topic: m.Topic,
				Key:   []byte(m.Key),
				Value: m.Payload,
				Headers: []kgo.RecordHeader{
					{Key: HeaderMessageID, Value: []byte(m.MessageID.String())},
					{Key: HeaderType, Value: []byte(m.Type)},
				},
			}
			ids[i] = m.ID
		}
		// ProduceSync chờ broker ack cả lô. Một record lỗi là coi cả lô lỗi -> ROLLBACK; các record
		// đã gửi thành công sẽ bị gửi lại lần sau (at-least-once, consumer tự chống trùng).
		if err := r.producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
			return fmt.Errorf("produce: %w", err)
		}
		if err := q.MarkPublished(ctx, ids); err != nil {
			return err
		}
		n = len(rows)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if n > 0 {
		r.log.DebugContext(ctx, "outbox published", "count", n)
	}
	return n, nil
}

// ErrNoBrokers báo cấu hình thiếu địa chỉ Redpanda.
var ErrNoBrokers = errors.New("outbox: no kafka brokers configured")

// NewProducer tạo Kafka client cho relay.
//
// franz-go mặc định bật idempotent producer (acks=all + số thứ tự): broker tự bỏ record trùng do
// client retry trong cùng một phiên kết nối, nên retry ở tầng mạng không sinh bản sao. Bản sao do
// relay publish lại cả lô (sau ROLLBACK) thì vẫn có, consumer xử lý bằng message_id.
func NewProducer(brokers []string, deliveryTimeout time.Duration) (*kgo.Client, error) {
	if len(brokers) == 0 {
		return nil, ErrNoBrokers
	}
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// Hết thời gian này mà chưa gửi được thì ProduceSync trả lỗi -> relay ROLLBACK và backoff,
		// không treo vô hạn khi Redpanda sập.
		kgo.RecordDeliveryTimeout(deliveryTimeout),
		// Không bật kgo.AllowAutoTopicCreation: topic phải được tạo trước (job redpanda-init trong
		// compose) với số partition đúng; tạo ngầm thì chỉ 1 partition, không scale được consumer.
	)
}
