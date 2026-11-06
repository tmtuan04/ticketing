// Package consumer đọc payment.events bằng consumer group và giao từng message cho Saga
// (service.HandlePaymentEvent), kèm retry và Dead Letter Queue.
//
// Commit offset: chỉ đánh dấu (mark) record SAU KHI xử lý xong, franz-go commit các mark định kỳ
// và khi rời group. Process chết giữa chừng -> record được giao lại -> inbox chống trùng. Đây là
// at-least-once ở tầng Kafka + exactly-once về hiệu ứng nhờ inbox.
//
// Lỗi được chia ba loại:
//   - service.ErrInvalidMessage (payload hỏng, thiếu message_id, đơn không tồn tại): xử lý lại cũng
//     vô ích -> đẩy ngay sang "<topic>.dlq".
//   - lỗi tạm thời (inventory không phản hồi, mất kết nối Postgres): retry MÃI với backoff, chặn
//     partition đó. Không đẩy DLQ vì lỗi không nằm ở message: đẩy đi thì một khoản thanh toán thật bị
//     bỏ dở chỉ vì inventory sập vài phút.
//   - lỗi khác (không rõ nguyên nhân): retry tối đa MaxAttempts lần rồi đẩy DLQ, để một message lỗi
//     không chặn cả partition mãi mãi.
//
// Thứ tự: mỗi partition xử lý tuần tự bởi một goroutine, các partition chạy song song. Mọi sự kiện
// của một đơn cùng key (order_id) nên cùng partition, luôn được xử lý đúng thứ tự.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tmtuan04/ticketing/order/internal/outbox"
	"github.com/tmtuan04/ticketing/order/internal/service"
)

// Header thêm vào record khi đẩy sang DLQ (ngoài các header gốc), đủ để biết vì sao và từ đâu,
// rồi replay bằng tay sau khi sửa lỗi.
const (
	HeaderDLQError     = "dlq_error"
	HeaderDLQTopic     = "dlq_source_topic"
	HeaderDLQPartition = "dlq_source_partition"
	HeaderDLQOffset    = "dlq_source_offset"
	HeaderDLQAttempts  = "dlq_attempts"
)

// DLQSuffix: record lỗi của topic T nằm ở topic T + DLQSuffix (tạo sẵn bởi redpanda-init).
const DLQSuffix = ".dlq"

// Handler xử lý một message. *service.Service.HandlePaymentEvent thỏa kiểu này.
type Handler func(ctx context.Context, msg service.IncomingMessage) (service.HandleResult, error)

type Config struct {
	Brokers     []string
	Group       string
	Topic       string
	MaxAttempts int           // số lần thử tối đa với lỗi không rõ loại trước khi đẩy DLQ
	MinBackoff  time.Duration // chờ giữa hai lần thử, nhân đôi sau mỗi lần...
	MaxBackoff  time.Duration // ...tối đa tới mức này
}

type Consumer struct {
	cl     *kgo.Client
	dlq    outbox.Producer
	handle Handler
	cfg    Config
	log    *slog.Logger
}

// New tạo consumer. dlq là producer dùng để đẩy record lỗi (dùng chung client với outbox relay).
// Giống outbox.NewProducer: chưa kết nối ngay, Redpanda chưa lên thì Run chỉ chờ.
func New(cfg Config, dlq outbox.Producer, handle Handler, log *slog.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, outbox.ErrNoBrokers
	}
	c := &Consumer{dlq: dlq, handle: handle, cfg: cfg, log: log}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		// Group mới (chưa có offset) đọc từ đầu topic: sự kiện phát ra trước lần chạy đầu không bị bỏ sót.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Chỉ commit những record đã Mark (xử lý xong), không commit theo những gì vừa poll về.
		kgo.AutoCommitMarks(),
		// Không rebalance giữa lúc đang xử lý một lô: partition bị thu hồi sau khi lô xong và mark đã
		// được commit (OnPartitionsRevoked), replica nhận partition không xử lý lại phần đã xong.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
			if err := cl.CommitMarkedOffsets(ctx); err != nil {
				c.log.WarnContext(ctx, "commit on revoke failed", "err", err)
			}
		}),
	)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	return c, nil
}

// Run chặn tới khi ctx bị hủy.
func (c *Consumer) Run(ctx context.Context) {
	for {
		fetches := c.cl.PollRecords(ctx, 500)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			// Poll nào cũng chặn rebalance, kể cả poll bị hủy này; không mở lại thì Close() (rời group)
			// đứng chờ mãi.
			c.cl.AllowRebalance()
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.WarnContext(ctx, "fetch failed", "topic", topic, "partition", partition, "err", err)
		})
		var wg sync.WaitGroup
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			wg.Go(func() {
				for _, r := range p.Records {
					if c.process(ctx, r) != nil {
						return // đang tắt: record này chưa mark, lần sau được giao lại
					}
					c.cl.MarkCommitRecords(r)
				}
			})
		})
		wg.Wait()
		c.cl.AllowRebalance()
	}
}

// Close commit các mark còn lại rồi rời group, để replica khác nhận partition ngay thay vì chờ
// session timeout. Gọi sau khi Run đã trả về.
func (c *Consumer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.cl.CommitMarkedOffsets(ctx); err != nil {
		c.log.Warn("final commit failed", "err", err)
	}
	c.cl.Close()
}

// process xử lý một record cho tới khi xong (thành công, bỏ qua, hoặc đã nằm trong DLQ). Chỉ trả
// lỗi khi ctx bị hủy giữa chừng.
func (c *Consumer) process(ctx context.Context, r *kgo.Record) error {
	mid, err := uuid.Parse(header(r, outbox.HeaderMessageID))
	if err != nil {
		return c.deadLetter(ctx, r, fmt.Errorf("%w: missing or invalid message_id header", service.ErrInvalidMessage), 1)
	}
	msg := service.IncomingMessage{MessageID: mid, Topic: r.Topic, Type: header(r, outbox.HeaderType), Value: r.Value}
	log := c.log.With("message_id", mid, "type", msg.Type, "partition", r.Partition, "offset", r.Offset)

	wait := c.cfg.MinBackoff
	for attempt := 1; ; attempt++ {
		res, err := c.handle(ctx, msg)
		if err == nil {
			log.DebugContext(ctx, "message handled", "result", res)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch {
		case errors.Is(err, service.ErrInvalidMessage):
			return c.deadLetter(ctx, r, err, attempt)
		case Transient(err):
			log.WarnContext(ctx, "transient error, retrying", "attempt", attempt, "err", err, "retry_in", wait.String())
		case attempt >= c.cfg.MaxAttempts:
			return c.deadLetter(ctx, r, err, attempt)
		default:
			log.WarnContext(ctx, "handle failed, retrying", "attempt", attempt, "err", err, "retry_in", wait.String())
		}
		if err := sleep(ctx, wait); err != nil {
			return err
		}
		wait = min(wait*2, c.cfg.MaxBackoff)
	}
}

// deadLetter chép record sang "<topic>.dlq" kèm header lỗi. Thử lại tới khi Redpanda nhận (hoặc
// đang tắt): record chỉ được mark sau khi đã nằm an toàn trong DLQ.
func (c *Consumer) deadLetter(ctx context.Context, r *kgo.Record, cause error, attempts int) error {
	headers := append(append([]kgo.RecordHeader(nil), r.Headers...),
		kgo.RecordHeader{Key: HeaderDLQError, Value: []byte(cause.Error())},
		kgo.RecordHeader{Key: HeaderDLQTopic, Value: []byte(r.Topic)},
		kgo.RecordHeader{Key: HeaderDLQPartition, Value: []byte(strconv.Itoa(int(r.Partition)))},
		kgo.RecordHeader{Key: HeaderDLQOffset, Value: []byte(strconv.FormatInt(r.Offset, 10))},
		kgo.RecordHeader{Key: HeaderDLQAttempts, Value: []byte(strconv.Itoa(attempts))},
	)
	rec := &kgo.Record{Topic: r.Topic + DLQSuffix, Key: r.Key, Value: r.Value, Headers: headers}
	c.log.ErrorContext(ctx, "moving message to DLQ", "topic", rec.Topic, "partition", r.Partition,
		"offset", r.Offset, "attempts", attempts, "err", cause)
	wait := c.cfg.MinBackoff
	for {
		err := c.dlq.ProduceSync(ctx, rec).FirstErr()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.log.WarnContext(ctx, "publish to DLQ failed, retrying", "err", err, "retry_in", wait.String())
		if err := sleep(ctx, wait); err != nil {
			return err
		}
		wait = min(wait*2, c.cfg.MaxBackoff)
	}
}

// Transient báo lỗi do phụ thuộc tạm thời không dùng được (inventory, Postgres), đáng retry mãi.
func Transient(err error) bool {
	if errors.Is(err, service.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) {
		return true
	}
	var connErr *pgconn.ConnectError
	var netErr net.Error
	if errors.As(err, &connErr) || errors.As(err, &netErr) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// 08xxx: mất kết nối; 57P01: server đang tắt; 40001/40P01: serialization/deadlock, thử lại là qua.
		return pgErr.Code[:2] == "08" || pgErr.Code == "57P01" || pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	return false
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
