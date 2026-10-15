// Package service chứa nghiệp vụ giữ/nhả/xác nhận ghế: Redis lọc nhanh, Postgres chốt chặn cuối.
//
// Quy ước khóa trong một transaction: khóa hold trước, ghế sau (ghế theo thứ tự seat_id),
// để các transaction đồng thời không deadlock.
package service

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tmtuan04/ticketing/inventory/internal/redisx"
	"github.com/tmtuan04/ticketing/inventory/internal/repo"
)

// Giá trị cột status, khớp CHECK constraint trong migration.
const (
	SeatAvailable = "available"
	SeatHeld      = "held"
	SeatSold      = "sold"

	HoldActive    = "active"
	HoldReleased  = "released"
	HoldConfirmed = "confirmed"
	HoldExpired   = "expired"
)

const maxIdempotencyKeyLen = 200

// holdIDNamespace dùng để sinh hold_id cố định từ (user_id, idempotency_key).
var holdIDNamespace = uuid.MustParse("9b0f3c5e-2d4a-4e8b-a1f7-3c6d8e2b5a14")

type Config struct {
	HoldTTL         time.Duration
	MaxSeatsPerHold int
}

type Service struct {
	pool  *pgxpool.Pool
	q     *repo.Queries
	locks *redisx.SeatLocks
	cfg   Config
	log   *slog.Logger
}

func New(pool *pgxpool.Pool, locks *redisx.SeatLocks, cfg Config, log *slog.Logger) *Service {
	return &Service{pool: pool, q: repo.New(pool), locks: locks, cfg: cfg, log: log}
}

type HoldRequest struct {
	EventID        uuid.UUID
	UserID         uuid.UUID
	SeatIDs        []uuid.UUID
	IdempotencyKey string
}

// HoldResult: giữ thành công thì Hold khác nil; ghế bị chiếm thì Unavailable khác rỗng.
// Ghế bị chiếm là kết quả nghiệp vụ bình thường, không phải lỗi.
type HoldResult struct {
	Hold        *repo.Hold
	Unavailable []uuid.UUID
}

// errSeatsTaken chỉ dùng để rollback transaction khi không lấy đủ ghế.
var errSeatsTaken = errors.New("seats taken")

// HoldSeats giữ tất cả ghế yêu cầu hoặc không giữ ghế nào.
//
// hold_id được sinh cố định từ (user_id, idempotency_key): hai request trùng key chạy song song
// sẽ cùng hold_id, nên không chặn nhau ở Redis; ở Postgres request đến sau chờ trên unique index
// rồi trả lại hold của request đến trước.
func (s *Service) HoldSeats(ctx context.Context, req HoldRequest) (HoldResult, error) {
	seatIDs, err := s.validateHold(req)
	if err != nil {
		return HoldResult{}, err
	}
	if res, ok, err := s.replay(ctx, req, seatIDs); ok || err != nil {
		return res, err
	}

	holdID := uuid.NewSHA1(holdIDNamespace, []byte(req.UserID.String()+":"+req.IdempotencyKey))
	expiresAt := time.Now().Add(s.cfg.HoldTTL)

	// 1. Redis: loại nhanh request tranh ghế đã có người giữ.
	redisHeld := false
	taken, err := s.locks.TryHold(ctx, req.EventID, seatIDs, holdID, s.cfg.HoldTTL)
	switch {
	case err != nil:
		// Redis chỉ là lớp lọc: lỗi thì đi thẳng xuống Postgres, tính đúng đắn không đổi.
		s.log.WarnContext(ctx, "redis hold failed, falling back to postgres", "err", err)
	case len(taken) > 0:
		return HoldResult{Unavailable: taken}, nil
	default:
		redisHeld = true
	}

	// 2. Postgres: nguồn dữ liệu gốc.
	hold, unavailable, stale, err := s.holdInDB(ctx, req, seatIDs, holdID, expiresAt)
	if err != nil && isConflict(err) {
		// Request song song cùng key đã thắng: key Redis là của nó (cùng hold_id), không được nhả.
		if res, ok, rerr := s.replay(ctx, req, seatIDs); ok || rerr != nil {
			return res, rerr
		}
	}
	if err != nil || len(unavailable) > 0 {
		if redisHeld {
			s.releaseKeys(ctx, req.EventID, seatIDs, holdID)
		}
		if err != nil {
			if isConflict(err) {
				return HoldResult{}, ErrActiveHoldExists
			}
			return HoldResult{}, err
		}
		return HoldResult{Unavailable: unavailable}, nil
	}
	if stale != nil {
		s.releaseKeys(ctx, stale.EventID, stale.SeatIds, stale.ID)
	}
	return HoldResult{Hold: &hold}, nil
}

func (s *Service) holdInDB(ctx context.Context, req HoldRequest, seatIDs []uuid.UUID, holdID uuid.UUID, expiresAt time.Time) (hold repo.Hold, unavailable []uuid.UUID, stale *repo.Hold, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)

		// Hold active cũ của user đã quá hạn mà reaper chưa quét: expire luôn để không vướng
		// unique index "1 hold active mỗi user mỗi event". Còn hạn thì từ chối.
		active, err := q.GetActiveHoldForUpdate(ctx, repo.GetActiveHoldForUpdateParams{EventID: req.EventID, UserID: req.UserID})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return err
		default:
			if _, err := q.MarkHoldExpired(ctx, active.ID); errors.Is(err, pgx.ErrNoRows) {
				return ErrActiveHoldExists
			} else if err != nil {
				return err
			}
			if _, err := q.ReleaseSeatsByHolds(ctx, []uuid.UUID{active.ID}); err != nil {
				return err
			}
			stale = &active
		}

		// Insert hold trước khi khóa ghế: xung đột idempotency/1-hold-active lộ ra sớm,
		// trước khi giữ khóa ghế.
		hold, err = q.CreateHold(ctx, repo.CreateHoldParams{
			ID:             holdID,
			EventID:        req.EventID,
			UserID:         req.UserID,
			SeatIds:        seatIDs,
			IdempotencyKey: req.IdempotencyKey,
			ExpiresAt:      expiresAt,
		})
		if err != nil {
			return err
		}

		got, err := q.HoldSeats(ctx, repo.HoldSeatsParams{
			EventID:   req.EventID,
			SeatIds:   seatIDs,
			HoldID:    holdID,
			HeldUntil: expiresAt,
		})
		if err != nil {
			return err
		}
		if len(got) != len(seatIDs) {
			unavailable = missing(seatIDs, got)
			return errSeatsTaken
		}
		return nil
	})
	if errors.Is(err, errSeatsTaken) {
		return repo.Hold{}, unavailable, nil, nil
	}
	return hold, nil, stale, err
}

// replay trả lại hold đã tạo với cùng idempotency key (ok=true), kể cả khi hold đã hết hạn hay
// bị nhả. Cùng key nhưng khác event/ghế là lỗi của client.
func (s *Service) replay(ctx context.Context, req HoldRequest, seatIDs []uuid.UUID) (HoldResult, bool, error) {
	h, err := s.q.GetHoldByIdempotencyKey(ctx, repo.GetHoldByIdempotencyKeyParams{UserID: req.UserID, IdempotencyKey: req.IdempotencyKey})
	if errors.Is(err, pgx.ErrNoRows) {
		return HoldResult{}, false, nil
	}
	if err != nil {
		return HoldResult{}, false, err
	}
	if h.EventID != req.EventID || !slices.Equal(sortedSeats(h.SeatIds), seatIDs) {
		return HoldResult{}, false, ErrIdempotencyMismatch
	}
	h = effective(h)
	return HoldResult{Hold: &h}, true, nil
}

func (s *Service) validateHold(req HoldRequest) ([]uuid.UUID, error) {
	switch {
	case req.EventID == uuid.Nil:
		return nil, invalidArg("event_id is required")
	case req.UserID == uuid.Nil:
		return nil, invalidArg("user_id is required")
	case req.IdempotencyKey == "":
		return nil, invalidArg("idempotency_key is required")
	case len(req.IdempotencyKey) > maxIdempotencyKeyLen:
		return nil, invalidArg("idempotency_key is longer than %d characters", maxIdempotencyKeyLen)
	case len(req.SeatIDs) == 0:
		return nil, invalidArg("seat_ids is empty")
	case len(req.SeatIDs) > s.cfg.MaxSeatsPerHold:
		return nil, invalidArg("at most %d seats per hold", s.cfg.MaxSeatsPerHold)
	}
	seatIDs := sortedSeats(req.SeatIDs)
	for i, id := range seatIDs {
		if id == uuid.Nil {
			return nil, invalidArg("seat_ids contains an empty id")
		}
		if i > 0 && id == seatIDs[i-1] {
			return nil, invalidArg("seat_ids contains duplicate %s", id)
		}
	}
	return seatIDs, nil
}

// ReleaseHold nhả hold của user. Idempotent: hold đã nhả hoặc đã hết hạn thì trả lại như cũ.
func (s *Service) ReleaseHold(ctx context.Context, holdID, userID uuid.UUID) (repo.Hold, error) {
	var hold repo.Hold
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		var err error
		hold, err = q.MarkHoldReleased(ctx, repo.MarkHoldReleasedParams{ID: holdID, UserID: userID})
		if err != nil {
			return err
		}
		_, err = q.ReleaseSeatsByHolds(ctx, []uuid.UUID{holdID})
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		h, err := s.GetHold(ctx, holdID)
		switch {
		case err != nil:
			return repo.Hold{}, err
		case h.UserID != userID:
			return repo.Hold{}, ErrPermissionDenied
		case h.Status == HoldConfirmed:
			return repo.Hold{}, ErrHoldNotActive
		default:
			return h, nil
		}
	}
	if err != nil {
		return repo.Hold{}, err
	}
	s.releaseKeys(ctx, hold.EventID, hold.SeatIds, hold.ID)
	return hold, nil
}

// ConfirmHold chuyển ghế held -> sold. Idempotent: hold đã confirm thì trả lại như cũ.
func (s *Service) ConfirmHold(ctx context.Context, holdID uuid.UUID) (repo.Hold, error) {
	var hold repo.Hold
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		var err error
		hold, err = q.MarkHoldConfirmed(ctx, holdID)
		if err != nil {
			return err
		}
		n, err := q.SellSeatsByHold(ctx, holdID)
		if err != nil {
			return err
		}
		if int(n) != len(hold.SeatIds) {
			// Không xảy ra nếu held_until luôn bằng expires_at; vẫn chặn để không bán thiếu ghế.
			return ErrHoldExpired
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		h, err := s.GetHold(ctx, holdID)
		switch {
		case err != nil:
			return repo.Hold{}, err
		case h.Status == HoldConfirmed:
			return h, nil
		case h.Status == HoldExpired:
			return repo.Hold{}, ErrHoldExpired
		default:
			return repo.Hold{}, ErrHoldNotActive
		}
	}
	if err != nil {
		return repo.Hold{}, err
	}
	if err := s.locks.MarkSold(context.WithoutCancel(ctx), hold.EventID, hold.SeatIds, hold.ID); err != nil {
		s.log.WarnContext(ctx, "redis mark sold failed", "hold_id", hold.ID, "err", err)
	}
	return hold, nil
}

// GetHold trả hold với trạng thái hiệu lực: active nhưng đã quá hạn thì báo expired.
func (s *Service) GetHold(ctx context.Context, holdID uuid.UUID) (repo.Hold, error) {
	h, err := s.q.GetHold(ctx, holdID)
	if errors.Is(err, pgx.ErrNoRows) {
		return repo.Hold{}, ErrNotFound
	}
	if err != nil {
		return repo.Hold{}, err
	}
	return effective(h), nil
}

// InitEventInventory tạo ghế cho event. Idempotent. Trả về tổng số ghế của event.
func (s *Service) InitEventInventory(ctx context.Context, eventID uuid.UUID, seatIDs []uuid.UUID) (int64, error) {
	if eventID == uuid.Nil {
		return 0, invalidArg("event_id is required")
	}
	if len(seatIDs) == 0 {
		return 0, invalidArg("seat_ids is empty")
	}
	if slices.Contains(seatIDs, uuid.Nil) {
		return 0, invalidArg("seat_ids contains an empty id")
	}
	if _, err := s.q.InitSeats(ctx, repo.InitSeatsParams{EventID: eventID, SeatIds: seatIDs}); err != nil {
		return 0, err
	}
	return s.q.CountSeatsByEvent(ctx, eventID)
}

func (s *Service) ListSeatAvailability(ctx context.Context, eventID uuid.UUID) ([]repo.ListSeatsByEventRow, error) {
	if eventID == uuid.Nil {
		return nil, invalidArg("event_id is required")
	}
	return s.q.ListSeatsByEvent(ctx, eventID)
}

// ExpireDueHolds expire một lô hold quá hạn và trả ghế về available. Reaper gọi định kỳ.
func (s *Service) ExpireDueHolds(ctx context.Context, batchSize int32) (int, error) {
	var due []repo.ExpireDueHoldsRow
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		var err error
		due, err = q.ExpireDueHolds(ctx, batchSize)
		if err != nil || len(due) == 0 {
			return err
		}
		ids := make([]uuid.UUID, len(due))
		for i, h := range due {
			ids[i] = h.ID
		}
		_, err = q.ReleaseSeatsByHolds(ctx, ids)
		return err
	})
	if err != nil {
		return 0, err
	}
	// Key Redis thường đã tự hết hạn theo PX; nhả lại cho chắc.
	for _, h := range due {
		s.releaseKeys(ctx, h.EventID, h.SeatIds, h.ID)
	}
	return len(due), nil
}

// releaseKeys nhả key Redis sau khi Postgres đã commit. Lỗi chỉ log: key sẽ tự hết hạn theo TTL.
func (s *Service) releaseKeys(ctx context.Context, eventID uuid.UUID, seatIDs []uuid.UUID, holdID uuid.UUID) {
	// Vẫn dọn key kể cả khi client đã hủy request.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if _, err := s.locks.Release(ctx, eventID, seatIDs, holdID); err != nil {
		s.log.WarnContext(ctx, "redis release failed", "hold_id", holdID, "err", err)
	}
}

func effective(h repo.Hold) repo.Hold {
	if h.Status == HoldActive && !h.ExpiresAt.After(time.Now()) {
		h.Status = HoldExpired
	}
	return h
}

// isConflict: vi phạm unique trên bảng holds (trùng idempotency key / hold_id, hoặc user đã có
// hold active trong event).
func isConflict(err error) bool {
	if errors.Is(err, ErrActiveHoldExists) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.TableName == "holds"
}

func sortedSeats(ids []uuid.UUID) []uuid.UUID {
	out := slices.Clone(ids)
	slices.SortFunc(out, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	return out
}

// missing trả về các ghế trong want mà không có trong got.
func missing(want, got []uuid.UUID) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(want)-len(got))
	for _, id := range want {
		if !slices.Contains(got, id) {
			out = append(out, id)
		}
	}
	return out
}
