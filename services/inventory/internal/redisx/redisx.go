// Package redisx là lớp lọc nhanh trước Postgres: phần lớn request tranh cùng ghế bị loại
// ở Redis mà không chạm DB. Redis không phải nguồn dữ liệu gốc; mất key chỉ làm request đi
// tiếp xuống Postgres, nơi UPDATE có điều kiện vẫn đảm bảo không bán trùng.
package redisx

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	//go:embed hold.lua
	holdSrc    string
	holdScript = redis.NewScript(holdSrc)

	//go:embed release.lua
	releaseSrc    string
	releaseScript = redis.NewScript(releaseSrc)
)

type SeatLocks struct {
	rdb redis.UniversalClient
}

func New(rdb redis.UniversalClient) *SeatLocks {
	return &SeatLocks{rdb: rdb}
}

// SeatKey dùng hash tag {event_id} để mọi ghế của một event nằm cùng slot,
// nhờ vậy Lua script nhiều key vẫn chạy được nếu sau này chuyển sang Redis Cluster.
func SeatKey(eventID, seatID uuid.UUID) string {
	return "seat:{" + eventID.String() + "}:" + seatID.String()
}

func seatKeys(eventID uuid.UUID, seatIDs []uuid.UUID) []string {
	keys := make([]string, len(seatIDs))
	for i, id := range seatIDs {
		keys[i] = SeatKey(eventID, id)
	}
	return keys
}

// TryHold giữ tất cả ghế cho holdID trong ttl, hoặc không giữ ghế nào.
// Trả về các ghế đang thuộc hold khác; rỗng nghĩa là đã giữ đủ.
func (l *SeatLocks) TryHold(ctx context.Context, eventID uuid.UUID, seatIDs []uuid.UUID, holdID uuid.UUID, ttl time.Duration) ([]uuid.UUID, error) {
	if ttl < time.Millisecond {
		return nil, fmt.Errorf("redisx: ttl too small: %s", ttl)
	}
	idx, err := holdScript.Run(ctx, l.rdb, seatKeys(eventID, seatIDs), holdID.String(), ttl.Milliseconds()).Int64Slice()
	if err != nil {
		return nil, fmt.Errorf("redisx: hold: %w", err)
	}
	taken := make([]uuid.UUID, 0, len(idx))
	for _, i := range idx {
		taken = append(taken, seatIDs[i-1])
	}
	return taken, nil
}

// Release xóa các key còn thuộc holdID. Trả về số key đã xóa.
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
