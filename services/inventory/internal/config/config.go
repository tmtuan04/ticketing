// Package config đọc cấu hình từ biến môi trường; giá trị mặc định dùng cho chạy local
// với stack docker compose.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

type Config struct {
	GRPCAddr        string
	DatabaseURL     string
	RedisAddr       string
	HoldTTL         time.Duration
	MaxSeatsPerHold int
	ReaperInterval  time.Duration
	ReaperBatchSize int32
	LogLevel        slog.Level
}

func Load() (Config, error) {
	c := Config{
		GRPCAddr:    env("GRPC_ADDR", ":50051"),
		DatabaseURL: env("DATABASE_URL", "postgres://app:app@localhost:5440/inventory_db?sslmode=disable"),
		RedisAddr:   env("REDIS_ADDR", "localhost:6379"),
	}
	var errs []error
	c.HoldTTL = duration("HOLD_TTL", 5*time.Minute, &errs)
	c.MaxSeatsPerHold = integer("MAX_SEATS_PER_HOLD", 8, &errs)
	c.ReaperInterval = duration("REAPER_INTERVAL", 5*time.Second, &errs)
	c.ReaperBatchSize = int32(integer("REAPER_BATCH_SIZE", 500, &errs))
	if err := c.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
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

func integer(key string, def int, errs *[]error) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		*errs = append(*errs, fmt.Errorf("%s: must be a positive integer, got %q", key, v))
		return def
	}
	return n
}
