package service

import (
	"errors"
	"fmt"
)

// Lỗi nghiệp vụ; tầng gRPC (grpcserver.toStatus) map sang status code. Cùng quy ước sentinel
// error như inventory/internal/service/errors.go.
var (
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrNotFound            = errors.New("order not found")
	ErrHoldNotFound        = errors.New("hold not found")
	ErrPermissionDenied    = errors.New("hold belongs to another user")
	ErrHoldNotActive       = errors.New("hold is no longer active")
	ErrOrderExists         = errors.New("hold already has an order")
	ErrIdempotencyMismatch = errors.New("idempotency key was used with a different request")
	// ErrUnavailable: service phía sau (inventory, catalog) lỗi hoặc quá hạn. Client nên thử lại
	// với cùng idempotency key.
	ErrUnavailable = errors.New("upstream unavailable")
)

func invalidArg(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}
