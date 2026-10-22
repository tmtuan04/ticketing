// Package reaper định kỳ thu hồi các hold quá hạn, trả ghế về available.
// Chạy nhiều instance song song an toàn nhờ FOR UPDATE SKIP LOCKED ở tầng query.
//
// "Reaper" (thần chết) là tên gọi phổ biến cho job nền dọn dẹp thứ đã hết hạn.
// Lưu ý: kể cả khi reaper chưa kịp chạy, hệ thống vẫn đúng (HoldSeats coi ghế held quá hạn
// là lấy được). Reaper chỉ giúp dữ liệu trong DB "gọn" và trạng thái hold được cập nhật.
package reaper

import (
	"context"
	"log/slog"
	"time"
)

// Expirer là thứ reaper cần: một hàm expire một lô hold quá hạn, trả về số hold đã xử lý.
// *service.Service thỏa interface này.
//
// [Go] Interface trong Go được thỏa NGẦM: kiểu nào có đủ method với đúng chữ ký thì tự động
// "implements", không cần khai báo. Quy ước Go: interface do BÊN DÙNG định nghĩa (ở đây là
// reaper), chỉ gồm đúng những method nó cần -> reaper không phụ thuộc vào package service,
// và test có thể truyền một fake Expirer.
type Expirer interface {
	ExpireDueHolds(ctx context.Context, batchSize int32) (int, error)
}

// Reaper giữ cấu hình của vòng quét.
//
// [Go] Field chữ thường -> chỉ package reaper truy cập được; bên ngoài phải tạo qua New().
type Reaper struct {
	exp       Expirer
	interval  time.Duration
	batchSize int32
	log       *slog.Logger
}

// New tạo Reaper. Gọi Run để bắt đầu quét.
func New(exp Expirer, interval time.Duration, batchSize int32, log *slog.Logger) *Reaper {
	return &Reaper{exp: exp, interval: interval, batchSize: batchSize, log: log}
}

// Run chặn cho tới khi ctx bị hủy.
// Quét một lần ngay khi khởi động, sau đó cứ mỗi `interval` quét một lần.
// main.go chạy hàm này trong goroutine riêng.
func (r *Reaper) Run(ctx context.Context) {
	// [Go] time.Ticker gửi một giá trị vào channel t.C sau mỗi khoảng interval (như setInterval).
	// Phải Stop() khi xong để giải phóng tài nguyên -> dùng defer.
	t := time.NewTicker(r.interval)
	defer t.Stop()
	// [Go] `for { ... }` không điều kiện là vòng lặp vô hạn (Go chỉ có `for`, không có `while`).
	for {
		r.sweep(ctx)
		// Chờ: hoặc ctx bị hủy (tắt service) -> thoát, hoặc tới tick kế tiếp -> quét tiếp.
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// sweep quét lặp từng lô cho tới khi hết hold quá hạn (hoặc gặp lỗi).
func (r *Reaper) sweep(ctx context.Context) {
	// Lô đầy nghĩa là còn tồn đọng: quét tiếp ngay thay vì chờ tick sau.
	for {
		n, err := r.exp.ExpireDueHolds(ctx, r.batchSize)
		if err != nil {
			// Lỗi do đang tắt (ctx đã hủy) là bình thường, không cần log ồn ào.
			// Lỗi khác thì log; tick sau sẽ thử lại, không cần retry ở đây.
			if ctx.Err() == nil {
				r.log.ErrorContext(ctx, "reaper sweep failed", "err", err)
			}
			return
		}
		if n > 0 {
			r.log.InfoContext(ctx, "expired holds", "count", n)
		}
		// Lấy được ít hơn batchSize => đã hết việc, dừng chờ tick sau.
		if n < int(r.batchSize) {
			return
		}
	}
}
