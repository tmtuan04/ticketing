package service

import (
	"errors"
	"fmt"
)

// Lỗi nghiệp vụ; tầng gRPC map sang status code.
//
// [Go] Đây là "sentinel errors": các giá trị lỗi cố định khai báo sẵn ở cấp package. Nơi khác
// kiểm tra bằng errors.Is(err, service.ErrNotFound) (xem grpcserver.toStatus), tương tự
// việc định nghĩa các class Exception riêng (NotFoundException...) trong NestJS.
// Quy ước đặt tên: biến lỗi bắt đầu bằng Err..., message viết thường, không dấu chấm cuối.
var (
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrNotFound            = errors.New("hold not found")
	ErrPermissionDenied    = errors.New("hold belongs to another user")
	ErrActiveHoldExists    = errors.New("user already has an active hold for this event")
	ErrIdempotencyMismatch = errors.New("idempotency key was used with a different request")
	ErrHoldNotActive       = errors.New("hold is no longer active")
	ErrHoldExpired         = errors.New("hold has expired")
)

// invalidArg tạo lỗi ErrInvalidArgument kèm mô tả chi tiết, vd:
//
//	invalidArg("at most %d seats per hold", 8)  ->  "invalid argument: at most 8 seats per hold"
//
// Nhờ %w, errors.Is(err, ErrInvalidArgument) vẫn trả true dù message đã có thêm chi tiết.
//
// [Go] `args ...any` là tham số biến đổi (variadic): nhận 0..n giá trị kiểu bất kỳ, bên trong
// hàm args là một slice []any. `args...` khi gọi Sprintf là "trải" slice ra lại.
func invalidArg(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}
