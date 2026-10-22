// Package config đọc cấu hình từ biến môi trường; giá trị mặc định dùng cho chạy local
// với stack docker compose.
//
// [Go] Comment ngay trên dòng `package xxx` là "package doc", hiện trong `go doc` / pkg.go.dev.
// Quy ước: bắt đầu bằng "Package <tên> ...".
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

// Config gom toàn bộ cấu hình của service.
//
// [Go] Tên bắt đầu bằng CHỮ HOA (Config, Load, GRPCAddr...) là "exported" (public), package
// khác dùng được. Chữ thường (env, duration...) là unexported (private trong package).
// Go không có từ khóa public/private, chỉ dựa vào chữ cái đầu.
type Config struct {
	GRPCAddr         string        // địa chỉ lắng nghe gRPC, vd ":50051"
	DatabaseURL      string        // connection string Postgres
	RedisAddr        string        // "host:port" của Redis
	HoldTTL          time.Duration // thời gian giữ ghế trước khi tự hết hạn
	MaxSeatsPerHold  int           // số ghế tối đa trong một lần giữ
	MaxHoldExtension time.Duration // ttl tối đa cho một lần ExtendHold (Order gia hạn khi tạo đơn)
	ReaperInterval   time.Duration // chu kỳ reaper quét hold hết hạn
	ReaperBatchSize  int32         // số hold tối đa reaper xử lý trong một transaction
	LogLevel         slog.Level    // debug | info | warn | error
}

// Load đọc config từ env. Gom TẤT CẢ lỗi lại rồi trả một lần, để người chạy thấy hết các biến
// sai cùng lúc thay vì sửa từng cái một.
func Load() (Config, error) {
	// [Go] Khởi tạo struct theo tên field; field không nhắc tới nhận "zero value"
	// (0 cho số, "" cho string, nil cho pointer/slice/map...). Go không có undefined.
	c := Config{
		GRPCAddr:    env("GRPC_ADDR", ":50051"),
		DatabaseURL: env("DATABASE_URL", "postgres://app:app@localhost:5440/inventory_db?sslmode=disable"),
		RedisAddr:   env("REDIS_ADDR", "localhost:6379"),
	}

	// [Go] `var errs []error` khai báo slice rỗng (nil). Slice là mảng động, giống Array trong JS.
	// Truyền &errs (con trỏ) để các hàm helper append được vào slice này.
	var errs []error
	c.HoldTTL = duration("HOLD_TTL", 5*time.Minute, &errs)
	c.MaxSeatsPerHold = integer("MAX_SEATS_PER_HOLD", 8, &errs)
	c.MaxHoldExtension = duration("MAX_HOLD_EXTENSION", 15*time.Minute, &errs)
	c.ReaperInterval = duration("REAPER_INTERVAL", 5*time.Second, &errs)
	// [Go] Go không tự ép kiểu số; int -> int32 phải viết rõ int32(...).
	c.ReaperBatchSize = int32(integer("REAPER_BATCH_SIZE", 500, &errs))

	// slog.Level biết tự parse "debug"/"info"/"warn"/"error" qua method UnmarshalText.
	// [Go] []byte("...") chuyển string sang mảng byte.
	if err := c.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}

	// errors.Join gộp nhiều lỗi thành một; nếu errs rỗng thì trả về nil (= không lỗi).
	// [Go] `errs...` "trải" slice thành danh sách tham số (giống spread ...arr trong JS).
	return c, errors.Join(errs...)
}

// env trả giá trị biến môi trường key, hoặc def nếu biến không có hay rỗng.
//
// [Go] `key, def string`: hai tham số cùng kiểu thì gộp khai báo kiểu một lần.
func env(key, def string) string {
	// [Go] os.LookupEnv trả 2 giá trị: giá trị và bool "có tồn tại hay không"
	// (mẫu "comma ok" rất hay gặp trong Go, cũng dùng cho map lookup và type assertion).
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// duration parse biến kiểu thời lượng như "5m", "30s", "1h30m". Sai định dạng hoặc <= 0 thì
// ghi lỗi vào errs và trả về mặc định (để Load vẫn chạy tiếp và gom đủ lỗi).
//
// [Go] errs *[]error là CON TRỎ tới slice. Phải dùng con trỏ vì append có thể tạo mảng mới;
// nếu truyền slice theo giá trị thì phần tử append thêm sẽ không phản ánh ra caller.
func duration(key string, def time.Duration, errs *[]error) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		// [Go] *errs là "giải tham chiếu" con trỏ để lấy/gán slice thật.
		// %q in chuỗi có dấu nháy, giúp thấy rõ cả khoảng trắng thừa.
		*errs = append(*errs, fmt.Errorf("%s: must be a positive duration, got %q", key, v))
		return def
	}
	return d
}

// integer parse biến số nguyên dương; cách xử lý lỗi giống duration.
func integer(key string, def int, errs *[]error) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	// strconv.Atoi = "ASCII to integer", tương đương parseInt trong JS nhưng chặt chẽ hơn
	// ("12abc" là lỗi chứ không ra 12).
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		*errs = append(*errs, fmt.Errorf("%s: must be a positive integer, got %q", key, v))
		return def
	}
	return n
}
