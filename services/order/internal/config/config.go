// Package config đọc cấu hình order từ biến môi trường; mặc định dùng cho chạy local với
// stack docker compose. Cùng cách làm như inventory/internal/config.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"
)

type Config struct {
	GRPCAddr         string        // địa chỉ lắng nghe gRPC, vd ":50053"
	DatabaseURL      string        // connection string Postgres (order_db)
	InventoryGRPCURL string        // "host:port" của inventory
	CatalogGRPCURL   string        // "host:port" của catalog
	PaymentTimeout   time.Duration // thời gian user có để trả tiền
	HoldBuffer       time.Duration // hold sống thêm bấy nhiêu sau payment deadline
	CallTimeout      time.Duration // deadline mỗi lời gọi gRPC xuống inventory/catalog
	LogLevel         slog.Level
}

func Load() (Config, error) {
	c := Config{
		GRPCAddr:         env("GRPC_ADDR", ":50053"),
		DatabaseURL:      env("DATABASE_URL", "postgres://app:app@localhost:5440/order_db?sslmode=disable"),
		InventoryGRPCURL: env("INVENTORY_GRPC_URL", "localhost:50051"),
		CatalogGRPCURL:   env("CATALOG_GRPC_URL", "localhost:50052"),
	}
	var errs []error
	c.PaymentTimeout = duration("PAYMENT_TIMEOUT", 10*time.Minute, &errs)
	c.HoldBuffer = duration("HOLD_BUFFER", 2*time.Minute, &errs)
	c.CallTimeout = duration("GRPC_CALL_TIMEOUT", 3*time.Second, &errs)
	if err := c.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	// PaymentTimeout + HoldBuffer là ttl gửi cho inventory.ExtendHold, phải không vượt
	// MAX_HOLD_EXTENSION của inventory (mặc định 15m). Order không đọc được config của inventory,
	// nên nếu đổi một bên thì đổi cả bên kia; sai thì CreateOrder trả INVALID_ARGUMENT từ inventory.
	return c, errors.Join(errs...)
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func duration(key string, def time.Duration, errs *[]error) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		*errs = append(*errs, fmt.Errorf("%s: must be a positive duration, got %q", key, v))
		return def
	}
	return d
}
