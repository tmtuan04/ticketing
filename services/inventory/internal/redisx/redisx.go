// Package redisx là lớp lọc nhanh trước Postgres: phần lớn request tranh cùng ghế bị loại
// ở Redis mà không chạm DB. Redis không phải nguồn dữ liệu gốc; mất key chỉ làm request đi
// tiếp xuống Postgres, nơi UPDATE có điều kiện vẫn đảm bảo không bán trùng.
//
// Mô hình dữ liệu trong Redis: mỗi ghế là một key, value là hold_id đang giữ ghế đó.
//
//	seat:{<event_id>}:<seat_id>  =  <hold_id>    (TTL = HoldTTL khi đang giữ, không TTL khi đã bán)
//
// Thao tác trên nhiều ghế dùng Lua script vì Redis chạy cả script một cách nguyên tử (atomic):
// không request nào chen vào giữa bước "kiểm tra" và bước "ghi".
//
// Tên "redisx" (Redis + extensions) để không trùng tên với package thư viện "redis".
package redisx

import (
	"context"
	// [Go] Import với tên `_` = chỉ import để lấy "tác dụng phụ", không dùng tên package.
	// Ở đây cần package embed để directive //go:embed bên dưới hoạt động.
	_ "embed"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// [Go] `var ( ... )` khai báo nhiều biến cấp package một lúc. Chúng được khởi tạo một lần khi
// chương trình khởi động (trước main).
var (
	// [Go] //go:embed là directive của compiler (KHÔNG có dấu cách sau //): nhúng nội dung
	// file hold.lua vào biến string ngay lúc build. Binary tự chứa script, không cần mang
	// file .lua theo khi deploy.
	//go:embed hold.lua
	holdSrc string
	// redis.NewScript tính sẵn SHA1 của script. Khi Run, go-redis gửi EVALSHA (chỉ gửi hash);
	// nếu Redis chưa cache script thì tự động gửi lại bằng EVAL kèm toàn bộ nội dung.
	holdScript = redis.NewScript(holdSrc)

	//go:embed release.lua
	releaseSrc    string
	releaseScript = redis.NewScript(releaseSrc)
)

// SeatLocks bọc Redis client, cung cấp các thao tác giữ/nhả/đánh dấu bán ghế.
type SeatLocks struct {
	// UniversalClient là interface chung cho Redis đơn, Sentinel và Cluster -> sau này đổi
	// kiểu triển khai Redis không phải sửa code ở đây.
	rdb redis.UniversalClient
}

// New tạo SeatLocks từ một Redis client có sẵn.
// [Go] *redis.Client (tạo ở main.go) thỏa interface redis.UniversalClient nên truyền vào được.
func New(rdb redis.UniversalClient) *SeatLocks {
	return &SeatLocks{rdb: rdb}
}

// SeatKey dùng hash tag {event_id} để mọi ghế của một event nằm cùng slot,
// nhờ vậy Lua script nhiều key vẫn chạy được nếu sau này chuyển sang Redis Cluster.
//
// (Redis Cluster chia key ra nhiều node theo hash; script chỉ được đụng các key cùng một
// slot. Khi key có {...}, Redis chỉ hash phần trong ngoặc nhọn, nên mọi ghế cùng event_id
// rơi vào cùng slot.)
//
// Hàm này exported (chữ S hoa) vì test cũng cần tạo key để kiểm tra trực tiếp trong Redis.
func SeatKey(eventID, seatID uuid.UUID) string {
	return "seat:{" + eventID.String() + "}:" + seatID.String()
}

// seatKeys tạo danh sách key theo đúng thứ tự seatIDs (thứ tự này quan trọng: hold.lua trả về
// vị trí trong KEYS, TryHold dùng vị trí đó để tra ngược lại seatIDs).
func seatKeys(eventID uuid.UUID, seatIDs []uuid.UUID) []string {
	keys := make([]string, len(seatIDs))
	for i, id := range seatIDs {
		keys[i] = SeatKey(eventID, id)
	}
	return keys
}

// TryHold giữ tất cả ghế cho holdID trong ttl, hoặc không giữ ghế nào.
// Trả về các ghế đang thuộc hold khác; rỗng nghĩa là đã giữ đủ.
//
// Ghế đang thuộc CHÍNH holdID này không tính là bị chiếm -> request retry cùng idempotency key
// (cùng holdID) không tự chặn chính mình.
func (l *SeatLocks) TryHold(ctx context.Context, eventID uuid.UUID, seatIDs []uuid.UUID, holdID uuid.UUID, ttl time.Duration) ([]uuid.UUID, error) {
	// Lệnh SET ... PX nhận mili giây; ttl < 1ms sẽ thành PX 0 và Redis báo lỗi.
	if ttl < time.Millisecond {
		return nil, fmt.Errorf("redisx: ttl too small: %s", ttl)
	}
	// Chạy hold.lua với KEYS = key các ghế, ARGV = [holdID, ttl_ms].
	// Script trả mảng số nguyên -> đọc bằng .Int64Slice().
	idx, err := holdScript.Run(ctx, l.rdb, seatKeys(eventID, seatIDs), holdID.String(), ttl.Milliseconds()).Int64Slice()
	if err != nil {
		return nil, fmt.Errorf("redisx: hold: %w", err)
	}
	// [Go] make([]T, 0, n): độ dài 0 nhưng dung lượng (capacity) n -> append n lần không phải
	// cấp phát lại bộ nhớ.
	taken := make([]uuid.UUID, 0, len(idx))
	for _, i := range idx {
		// Lua đánh chỉ số từ 1, Go từ 0 -> trừ 1.
		taken = append(taken, seatIDs[i-1])
	}
	return taken, nil
}

// Release xóa các key còn thuộc holdID. Trả về số key đã xóa.
// Key nào đã đổi chủ (hold cũ hết hạn, hold khác giữ lại) thì giữ nguyên.
func (l *SeatLocks) Release(ctx context.Context, eventID uuid.UUID, seatIDs []uuid.UUID, holdID uuid.UUID) (int, error) {
	n, err := releaseScript.Run(ctx, l.rdb, seatKeys(eventID, seatIDs), holdID.String()).Int()
	if err != nil {
		return 0, fmt.Errorf("redisx: release: %w", err)
	}
	return n, nil
}

// MarkSold ghi đè key không TTL sau khi Postgres đã xác nhận bán, để mọi request sau
// cho các ghế này bị loại ngay ở Redis.
func (l *SeatLocks) MarkSold(ctx context.Context, eventID uuid.UUID, seatIDs []uuid.UUID, holdID uuid.UUID) error {
	// TxPipelined gom mọi lệnh SET vào một MULTI/EXEC: gửi một lượt qua mạng và Redis thực thi
	// liền một khối. TTL = 0 nghĩa là key không bao giờ hết hạn.
	//
	// [Go] Truyền một hàm ẩn danh làm tham số (callback); go-redis gọi nó với pipeline p.
	// Biến `_` bỏ qua kết quả từng lệnh, ta chỉ quan tâm err.
	_, err := l.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		for _, key := range seatKeys(eventID, seatIDs) {
			p.Set(ctx, key, holdID.String(), 0)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("redisx: mark sold: %w", err)
	}
	return nil
}
