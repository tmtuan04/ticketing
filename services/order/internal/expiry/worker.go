// Package expiry chạy timeout worker của Saga: định kỳ hủy các đơn quá payment_deadline mà chưa
// có kết quả thanh toán (service.ExpireDueOrders). Cùng khuôn vòng lặp với outbox relay.
//
// Nhiều replica chạy song song an toàn: mỗi lô khóa đơn bằng FOR UPDATE SKIP LOCKED.
package expiry

import (
	"context"
	"log/slog"
	"time"
)

// Expirer là thứ worker cần từ service. *service.Service thỏa interface này.
type Expirer interface {
	ExpireDueOrders(ctx context.Context, limit int32) (int, error)
}

type Worker struct {
	svc        Expirer
	interval   time.Duration
	batchSize  int32
	maxBackoff time.Duration
	log        *slog.Logger
}

func New(svc Expirer, interval time.Duration, batchSize int32, log *slog.Logger) *Worker {
	return &Worker{svc: svc, interval: interval, batchSize: batchSize, maxBackoff: 30 * time.Second, log: log}
}

// Run chặn tới khi ctx bị hủy. Mỗi `interval` hủy hết đơn quá hạn (lô đầy thì chạy lô tiếp ngay);
// lỗi thì chờ tăng dần tới maxBackoff.
func (w *Worker) Run(ctx context.Context) {
	wait := w.interval
	for {
		if err := w.drain(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			wait = min(wait*2, w.maxBackoff)
			w.log.WarnContext(ctx, "expiry worker failed, backing off", "err", err, "retry_in", wait.String())
		} else {
			wait = w.interval
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func (w *Worker) drain(ctx context.Context) error {
	for {
		n, err := w.svc.ExpireDueOrders(ctx, w.batchSize)
		if err != nil {
			return err
		}
		// n < batchSize cả khi lô có đơn bị bỏ qua (đơn đang được consumer xử lý), nên không lặp vô hạn
		// trên cùng một đơn bị kẹt.
		if n < int(w.batchSize) {
			return nil
		}
	}
}
