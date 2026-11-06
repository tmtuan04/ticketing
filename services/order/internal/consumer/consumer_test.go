// Integration test cho consumer: Redpanda THẬT (testcontainers), handler giả điều khiển bằng
// header type của từng message.
//
//	go test ./internal/consumer/      # cần Docker
//	go test -short ./...              # bỏ qua
package consumer_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/testcontainers/testcontainers-go"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tmtuan04/ticketing/order/internal/consumer"
	"github.com/tmtuan04/ticketing/order/internal/outbox"
	"github.com/tmtuan04/ticketing/order/internal/service"
)

var broker string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	rp, err := tcredpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	if err == nil {
		broker, err = rp.KafkaSeedBroker(ctx)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		_ = testcontainers.TerminateContainer(rp)
		os.Exit(1)
	}
	code := m.Run()
	_ = testcontainers.TerminateContainer(rp)
	os.Exit(code)
}

// --- helper --------------------------------------------------------------------------------

const maxAttempts = 3

// fakeHandler quyết định kết quả theo type: "ok", "invalid" (ErrInvalidMessage), "boom" (lỗi lạ,
// luôn lỗi), "flaky" (ErrUnavailable 5 lần đầu, nhiều hơn maxAttempts, rồi thành công).
type fakeHandler struct {
	mu    sync.Mutex
	calls map[uuid.UUID]int
	done  []uuid.UUID // message xử lý thành công, theo thứ tự
}

func (h *fakeHandler) handle(_ context.Context, msg service.IncomingMessage) (service.HandleResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls[msg.MessageID]++
	switch msg.Type {
	case "invalid":
		return "", fmt.Errorf("%w: bad payload", service.ErrInvalidMessage)
	case "boom":
		return "", errors.New("unexpected bug")
	case "flaky":
		if h.calls[msg.MessageID] <= 5 {
			return "", fmt.Errorf("%w: inventory down", service.ErrUnavailable)
		}
	}
	h.done = append(h.done, msg.MessageID)
	return service.ResultProcessed, nil
}

func (h *fakeHandler) snapshot() (map[uuid.UUID]int, []uuid.UUID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	calls := make(map[uuid.UUID]int, len(h.calls))
	for k, v := range h.calls {
		calls[k] = v
	}
	return calls, append([]uuid.UUID(nil), h.done...)
}

type env struct {
	topic, group string
	producer     *kgo.Client
	handler      *fakeHandler
}

func setup(t *testing.T) *env {
	t.Helper()
	if broker == "" {
		t.Skip("integration test: chạy không có -short")
	}
	e := &env{topic: "test." + uuid.NewString()[:8], group: "g-" + uuid.NewString()[:8], handler: &fakeHandler{calls: map[uuid.UUID]int{}}}
	adm, err := kgo.NewClient(kgo.SeedBrokers(broker))
	must(t, err)
	defer adm.Close()
	res, err := kadm.NewClient(adm).CreateTopics(context.Background(), 3, 1, nil, e.topic, e.topic+consumer.DLQSuffix)
	must(t, err)
	must(t, res.Error())
	e.producer, err = outbox.NewProducer([]string{broker}, 5*time.Second)
	must(t, err)
	t.Cleanup(e.producer.Close)
	return e
}

// start chạy consumer tới khi test kết thúc hoặc stop() được gọi.
func (e *env) start(t *testing.T) (stop func()) {
	t.Helper()
	c, err := consumer.New(consumer.Config{
		Brokers: []string{broker}, Group: e.group, Topic: e.topic,
		MaxAttempts: maxAttempts, MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
	}, e.producer, e.handler.handle, slog.New(slog.NewTextHandler(io.Discard, nil)))
	must(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		c.Close()
		close(done)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("consumer không dừng")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// send publish message với key cho trước; mid = uuid.Nil thì không gắn header message_id.
func (e *env) send(t *testing.T, key, typ string, mid uuid.UUID) {
	t.Helper()
	headers := []kgo.RecordHeader{{Key: outbox.HeaderType, Value: []byte(typ)}}
	if mid != uuid.Nil {
		headers = append(headers, kgo.RecordHeader{Key: outbox.HeaderMessageID, Value: []byte(mid.String())})
	}
	must(t, e.producer.ProduceSync(context.Background(), &kgo.Record{
		Topic: e.topic, Key: []byte(key), Value: []byte(`{"n":1}`), Headers: headers,
	}).FirstErr())
}

// waitDone chờ tới khi handler xử lý thành công đủ n message.
func (e *env) waitDone(t *testing.T, n int) []uuid.UUID {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, done := e.handler.snapshot()
		if len(done) >= n {
			return done
		}
		if time.Now().After(deadline) {
			t.Fatalf("handler mới xong %d/%d message", len(done), n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (e *env) readDLQ(t *testing.T, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(e.topic+consumer.DLQSuffix),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	must(t, err)
	defer cl.Close()
	var out []*kgo.Record
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		fs := cl.PollFetches(ctx)
		cancel()
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
		if soon := time.Now().Add(time.Second); len(out) >= want && soon.Before(deadline) {
			deadline = soon
		}
	}
	return out
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// --- test ----------------------------------------------------------------------------------

// Message cùng key được xử lý đúng thứ tự; offset được commit nên consumer khởi động lại (cùng
// group) không xử lý lại message cũ.
func TestConsumer_OrderedAndCommitted(t *testing.T) {
	e := setup(t)
	keys := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	sent := map[string][]uuid.UUID{}
	keyOf := map[uuid.UUID]string{}
	for range 10 {
		for _, k := range keys {
			mid := uuid.New()
			sent[k] = append(sent[k], mid)
			keyOf[mid] = k
			e.send(t, k, "ok", mid)
		}
	}
	stop := e.start(t)
	done := e.waitDone(t, 30)
	stop()

	next := map[string]int{}
	for _, mid := range done {
		k := keyOf[mid]
		if sent[k][next[k]] != mid {
			t.Fatalf("key %s xử lý lệch thứ tự", k)
		}
		next[k]++
	}

	// khởi động lại: chỉ message mới được xử lý
	last := uuid.New()
	e.send(t, keys[0], "ok", last)
	e.start(t)
	done = e.waitDone(t, 31)
	calls, _ := e.handler.snapshot()
	if done[30] != last || len(done) != 31 {
		t.Fatalf("sau restart xử lý %d message, muốn 31", len(done))
	}
	for mid, n := range calls {
		if n != 1 {
			t.Fatalf("message %s bị xử lý %d lần", mid, n)
		}
	}
}

// Message sai contract hoặc thiếu message_id vào DLQ ngay (không retry) kèm header lỗi; lỗi lạ
// vào DLQ sau maxAttempts lần; message phía sau cùng partition vẫn được xử lý.
func TestConsumer_DeadLetters(t *testing.T) {
	e := setup(t)
	key := uuid.NewString()
	invalid, boom, after := uuid.New(), uuid.New(), uuid.New()
	e.send(t, key, "invalid", invalid)
	e.send(t, key, "ok", uuid.Nil)
	e.send(t, key, "boom", boom)
	e.send(t, key, "ok", after)
	e.start(t)

	done := e.waitDone(t, 1)
	if done[0] != after {
		t.Fatal("message hợp lệ phía sau không được xử lý")
	}
	calls, _ := e.handler.snapshot()
	if calls[invalid] != 1 || calls[boom] != maxAttempts {
		t.Fatalf("số lần thử: invalid=%d boom=%d, muốn 1 và %d", calls[invalid], calls[boom], maxAttempts)
	}

	dlq := e.readDLQ(t, 3)
	if len(dlq) != 3 {
		t.Fatalf("DLQ có %d record, muốn 3", len(dlq))
	}
	want := []struct {
		mid      string
		attempts int
	}{{invalid.String(), 1}, {"", 1}, {boom.String(), maxAttempts}}
	for i, r := range dlq {
		if string(r.Key) != key || string(r.Value) != `{"n":1}` || header(r, outbox.HeaderMessageID) != want[i].mid {
			t.Fatalf("DLQ #%d sai: key=%s headers=%v", i, r.Key, r.Headers)
		}
		if header(r, consumer.HeaderDLQTopic) != e.topic || header(r, consumer.HeaderDLQError) == "" ||
			header(r, consumer.HeaderDLQAttempts) != strconv.Itoa(want[i].attempts) {
			t.Fatalf("DLQ #%d thiếu header lỗi: %v", i, r.Headers)
		}
	}
}

// Lỗi tạm thời (inventory sập) được retry quá maxAttempts mà không vào DLQ.
func TestConsumer_RetriesTransientErrors(t *testing.T) {
	e := setup(t)
	flaky := uuid.New()
	e.send(t, uuid.NewString(), "flaky", flaky)
	e.start(t)

	e.waitDone(t, 1)
	calls, _ := e.handler.snapshot()
	if calls[flaky] != 6 {
		t.Fatalf("message flaky được thử %d lần, muốn 6", calls[flaky])
	}
	if dlq := e.readDLQ(t, 0); len(dlq) != 0 {
		t.Fatalf("lỗi tạm thời mà vẫn vào DLQ: %d record", len(dlq))
	}
}

func TestTransient(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"inventory unavailable": {fmt.Errorf("%w: inventory.ConfirmHold", service.ErrUnavailable), true},
		"pg mất kết nối":        {&pgconn.PgError{Code: "08006"}, true},
		"pg deadlock":           {&pgconn.PgError{Code: "40P01"}, true},
		"pg vi phạm ràng buộc":  {&pgconn.PgError{Code: "23514"}, false},
		"message sai":           {service.ErrInvalidMessage, false},
		"lỗi lạ":                {errors.New("boom"), false},
	} {
		if got := consumer.Transient(tc.err); got != tc.want {
			t.Errorf("%s: Transient=%v, muốn %v", name, got, tc.want)
		}
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
