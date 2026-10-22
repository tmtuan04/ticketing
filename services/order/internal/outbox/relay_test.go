// Integration test cho outbox relay: Postgres và Redpanda THẬT (testcontainers).
//
//	go test ./internal/outbox/          # cần Docker
//	go test -short ./...                # bỏ qua
//
// Mỗi test publish vào topic riêng và chỉ ghi message cho topic đó; các test chạy tuần tự nên
// relay của test này không đụng message của test khác.
package outbox_test

import (
	"context"
	"encoding/json"
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
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tmtuan04/ticketing/order/internal/outbox"
	"github.com/tmtuan04/ticketing/order/internal/repo"
)

var (
	pool   *pgxpool.Pool
	broker string
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	code, err := runWithContainers(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func runWithContainers(m *testing.M) (int, error) {
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

	// Cùng image với compose để hành vi broker giống môi trường chạy thật.
	rp, err := tcredpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	defer func() { _ = testcontainers.TerminateContainer(rp) }()
	if err != nil {
		return 0, err
	}
	if broker, err = rp.KafkaSeedBroker(ctx); err != nil {
		return 0, err
	}
	return m.Run(), nil
}

// --- helper --------------------------------------------------------------------------------

func setup(t *testing.T, partitions int32) (topic string, producer *kgo.Client) {
	t.Helper()
	if pool == nil {
		t.Skip("integration test: chạy không có -short")
	}
	topic = "test." + uuid.NewString()[:8]
	adm, err := kgo.NewClient(kgo.SeedBrokers(broker))
	must(t, err)
	defer adm.Close()
	res, err := kadm.NewClient(adm).CreateTopic(context.Background(), partitions, 1, nil, topic)
	must(t, err)
	must(t, res.Err)

	producer, err = outbox.NewProducer([]string{broker}, 5*time.Second)
	must(t, err)
	t.Cleanup(producer.Close)
	return topic, producer
}

func newRelay(p outbox.Producer, batch int32) *outbox.Relay {
	return outbox.New(pool, p, 50*time.Millisecond, batch, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// seq là payload test: thứ tự message trong cùng một key.
type seq struct {
	N int `json:"n"`
}

// insert ghi n message cho mỗi key, xen kẽ giữa các key (k0#0, k1#0, ..., k0#1, k1#1, ...),
// trả về map message_id -> key.
func insert(t *testing.T, topic string, keys []uuid.UUID, n int) map[string]uuid.UUID {
	t.Helper()
	q := repo.New(pool)
	ids := map[string]uuid.UUID{}
	for i := range n {
		for _, k := range keys {
			payload, err := json.Marshal(seq{N: i})
			must(t, err)
			mid := uuid.New()
			must(t, q.InsertOutbox(context.Background(), repo.InsertOutboxParams{
				MessageID: mid, AggregateID: k, Topic: topic, Key: k.String(), Type: "test.event", Payload: payload,
			}))
			ids[mid.String()] = k
		}
	}
	return ids
}

// consume đọc từ đầu topic tới khi đủ want record hoặc quá hạn. Đọc thêm một lúc sau khi đủ để
// bắt cả record bị publish thừa (nếu relay có lỗi).
func consume(t *testing.T, topic string, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic),
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

func unpublished(t *testing.T, topic string) int {
	t.Helper()
	var n int
	must(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE topic = $1 AND published_at IS NULL`, topic).Scan(&n))
	return n
}

// --- test ----------------------------------------------------------------------------------

// 4 relay (mô phỏng 4 replica order) cùng publish 300 message: mỗi message lên Redpanda đúng một
// lần, kèm header message_id/type, và mọi dòng outbox được đánh dấu đã publish.
func TestRelay_ConcurrentRelaysPublishOnce(t *testing.T) {
	topic, producer := setup(t, 3)
	keys := make([]uuid.UUID, 30)
	for i := range keys {
		keys[i] = uuid.New()
	}
	sent := insert(t, topic, keys, 10)

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			r := newRelay(producer, 25)
			for {
				n, err := r.PublishBatch(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		})
	}
	wg.Wait()

	if n := unpublished(t, topic); n != 0 {
		t.Fatalf("còn %d message chưa publish", n)
	}
	got := consume(t, topic, len(sent))
	seen := map[string]int{}
	for _, r := range got {
		mid := header(r, outbox.HeaderMessageID)
		seen[mid]++
		if k, ok := sent[mid]; !ok || string(r.Key) != k.String() || header(r, outbox.HeaderType) != "test.event" {
			t.Fatalf("record lạ: key=%s headers=%v", r.Key, r.Headers)
		}
	}
	if len(got) != len(sent) || len(seen) != len(sent) {
		t.Fatalf("publish %d record (%d message khác nhau), muốn đúng %d", len(got), len(seen), len(sent))
	}
}

// Một relay: message cùng key vào cùng partition, consumer đọc đúng thứ tự đã ghi.
func TestRelay_PreservesOrderPerKey(t *testing.T) {
	topic, producer := setup(t, 3)
	keys := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	sent := insert(t, topic, keys, 20)

	r := newRelay(producer, 7) // lô lẻ để thứ tự phải giữ được qua nhiều lô
	for {
		n, err := r.PublishBatch(context.Background())
		must(t, err)
		if n == 0 {
			break
		}
	}

	next := map[string]int{}
	partition := map[string]int32{}
	for _, rec := range consume(t, topic, len(sent)) {
		k := string(rec.Key)
		var s seq
		must(t, json.Unmarshal(rec.Value, &s))
		if s.N != next[k] {
			t.Fatalf("key %s: nhận #%d, muốn #%d", k, s.N, next[k])
		}
		next[k]++
		if p, ok := partition[k]; ok && p != rec.Partition {
			t.Fatalf("key %s nằm ở 2 partition %d và %d", k, p, rec.Partition)
		}
		partition[k] = rec.Partition
	}
	for _, k := range keys {
		if next[k.String()] != 20 {
			t.Fatalf("key %s: nhận %d/20 message", k, next[k.String()])
		}
	}
}

// Redpanda không tới được: lô bị ROLLBACK, message vẫn nằm trong outbox (không mất, không bị
// đánh dấu nhầm là đã gửi); Redpanda sống lại thì relay publish đủ.
func TestRelay_BrokerDownKeepsMessages(t *testing.T) {
	topic, producer := setup(t, 1)
	sent := insert(t, topic, []uuid.UUID{uuid.New()}, 5)

	// Cổng 1: không có gì lắng nghe -> mọi lần gửi đều lỗi, hết DeliveryTimeout thì ProduceSync trả lỗi.
	dead, err := outbox.NewProducer([]string{"127.0.0.1:1"}, time.Second)
	must(t, err)
	defer dead.Close()
	n, err := newRelay(dead, 100).PublishBatch(context.Background())
	if err == nil || n != 0 {
		t.Fatalf("broker chết mà PublishBatch trả n=%d err=%v", n, err)
	}
	if got := unpublished(t, topic); got != len(sent) {
		t.Fatalf("sau lỗi còn %d/%d message chờ publish", got, len(sent))
	}

	n, err = newRelay(producer, 100).PublishBatch(context.Background())
	must(t, err)
	if n != len(sent) || unpublished(t, topic) != 0 {
		t.Fatalf("broker sống lại: publish %d, còn chờ %d", n, unpublished(t, topic))
	}
	if got := consume(t, topic, len(sent)); len(got) != len(sent) {
		t.Fatalf("topic có %d record, muốn %d", len(got), len(sent))
	}
}

// Message tồn đọng lâu (Redpanda sập một lúc, relay dừng...) vẫn publish được. Bắt lỗi từng gặp:
// gán Record.Timestamp = created_at khiến franz-go coi record là quá RecordDeliveryTimeout ngay.
func TestRelay_PublishesOldMessages(t *testing.T) {
	topic, producer := setup(t, 1)
	sent := insert(t, topic, []uuid.UUID{uuid.New()}, 3)
	_, err := pool.Exec(context.Background(), `UPDATE outbox SET created_at = now() - interval '1 hour' WHERE topic = $1`, topic)
	must(t, err)

	n, err := newRelay(producer, 100).PublishBatch(context.Background())
	must(t, err)
	if n != len(sent) {
		t.Fatalf("publish %d/%d message cũ", n, len(sent))
	}
}

// Vòng Run: message ghi vào outbox sau khi relay đã chạy vẫn được publish trong vài chu kỳ.
func TestRelay_RunPicksUpNewMessages(t *testing.T) {
	topic, producer := setup(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		newRelay(producer, 100).Run(ctx)
		close(done)
	}()

	sent := insert(t, topic, []uuid.UUID{uuid.New()}, 3)
	deadline := time.Now().Add(10 * time.Second)
	for unpublished(t, topic) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("relay không publish message mới")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run không dừng khi ctx bị hủy")
	}
	if got := consume(t, topic, len(sent)); len(got) != len(sent) {
		t.Fatalf("topic có %d record, muốn %d", len(got), len(sent))
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
