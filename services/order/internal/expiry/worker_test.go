// Unit test cho vòng lặp của timeout worker (logic hủy đơn nằm ở service, test trong saga_test.go).
package expiry_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/tmtuan04/ticketing/order/internal/expiry"
)

// fakeExpirer trả lần lượt các kết quả trong script, hết script thì trả 0.
type fakeExpirer struct {
	mu     sync.Mutex
	script []result
	calls  int
}

type result struct {
	n   int
	err error
}

func (f *fakeExpirer) ExpireDueOrders(context.Context, int32) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.script) == 0 {
		return 0, nil
	}
	r := f.script[0]
	f.script = f.script[1:]
	return r.n, r.err
}

func (f *fakeExpirer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// Lô đầy thì chạy lô tiếp ngay trong cùng chu kỳ; lỗi không làm worker dừng; ctx hủy thì Run trả về.
func TestWorker_DrainsAndSurvivesErrors(t *testing.T) {
	f := &fakeExpirer{script: []result{{10, nil}, {10, nil}, {3, nil}, {0, errors.New("db down")}}}
	w := expiry.New(f, 20*time.Millisecond, 10, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for f.callCount() < 6 {
		if time.Now().After(deadline) {
			t.Fatalf("worker mới gọi %d lần", f.callCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run không dừng khi ctx bị hủy")
	}
}
