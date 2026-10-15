// Package reaper định kỳ thu hồi các hold quá hạn, trả ghế về available.
// Chạy nhiều instance song song an toàn nhờ FOR UPDATE SKIP LOCKED ở tầng query.
package reaper

import (
	"context"
	"log/slog"
	"time"
)

type Expirer interface {
	ExpireDueHolds(ctx context.Context, batchSize int32) (int, error)
}

type Reaper struct {
	exp       Expirer
	interval  time.Duration
	batchSize int32
	log       *slog.Logger
}

func New(exp Expirer, interval time.Duration, batchSize int32, log *slog.Logger) *Reaper {
	return &Reaper{exp: exp, interval: interval, batchSize: batchSize, log: log}
}

// Run chặn cho tới khi ctx bị hủy.
func (r *Reaper) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		r.sweep(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (r *Reaper) sweep(ctx context.Context) {
	// Lô đầy nghĩa là còn tồn đọng: quét tiếp ngay thay vì chờ tick sau.
	for {
		n, err := r.exp.ExpireDueHolds(ctx, r.batchSize)
		if err != nil {
			if ctx.Err() == nil {
				r.log.ErrorContext(ctx, "reaper sweep failed", "err", err)
			}
			return
		}
		if n > 0 {
			r.log.InfoContext(ctx, "expired holds", "count", n)
		}
		if n < int(r.batchSize) {
			return
		}
	}
}
