package service

import (
	"errors"
	"fmt"
)

// Lỗi nghiệp vụ; tầng gRPC map sang status code.
var (
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrNotFound            = errors.New("hold not found")
	ErrPermissionDenied    = errors.New("hold belongs to another user")
	ErrActiveHoldExists    = errors.New("user already has an active hold for this event")
	ErrIdempotencyMismatch = errors.New("idempotency key was used with a different request")
	ErrHoldNotActive       = errors.New("hold is no longer active")
	ErrHoldExpired         = errors.New("hold has expired")
)

func invalidArg(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}
