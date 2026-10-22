// Package service chứa nghiệp vụ giữ/nhả/xác nhận ghế: Redis lọc nhanh, Postgres chốt chặn cuối.
//
// Quy ước khóa trong một transaction: khóa hold trước, ghế sau (ghế theo thứ tự seat_id),
// để các transaction đồng thời không deadlock.
//
// Vòng đời của một hold (lượt giữ chỗ):
//
//	           ReleaseHold
//	active ─────────────────> released
//	  │  │     ConfirmHold
//	  │  └──────────────────> confirmed   (ghế: held -> sold)
//	  │   hết hạn (reaper / tự expire khi user giữ lần mới)
//	  └─────────────────────> expired     (ghế: held -> available)
//
// Trạng thái ghế: available -> held -> sold, hoặc held -> available khi nhả/hết hạn.
//
// Dữ liệu nằm ở hai nơi:
//   - Postgres: nguồn sự thật (source of truth). Mọi quyết định cuối cùng ở đây.
//   - Redis: chỉ để loại nhanh request tranh ghế đã có người giữ, giảm tải cho Postgres khi
//     hàng nghìn người cùng bấm một ghế. Redis sai/mất dữ liệu thì Postgres vẫn chặn đúng.
package service

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices" // các hàm tiện ích cho slice (Sort, Contains, Equal, Clone...), Go 1.21+
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tmtuan04/ticketing/inventory/internal/redisx"
	"github.com/tmtuan04/ticketing/inventory/internal/repo"
)

// Giá trị cột status, khớp CHECK constraint trong migration.
//
// [Go] Go không có enum. Thường dùng hằng số (const) như thế này. Kiểu string được giữ
// nguyên để khớp trực tiếp với cột text trong DB.
const (
	SeatAvailable = "available"
	SeatHeld      = "held"
	SeatSold      = "sold"

	HoldActive    = "active"
	HoldReleased  = "released"
	HoldConfirmed = "confirmed"
	HoldExpired   = "expired"
)

// Giới hạn độ dài idempotency key để client không nhét chuỗi khổng lồ vào DB.
const maxIdempotencyKeyLen = 200

// holdIDNamespace dùng để sinh hold_id cố định từ (user_id, idempotency_key).
//
// UUID v5 = SHA1(namespace + tên): cùng đầu vào luôn ra cùng UUID. Namespace là hằng số tùy
// chọn, chỉ cần cố định mãi mãi (đổi nó thì các request retry sẽ ra hold_id khác).
//
// [Go] uuid.MustParse panic nếu chuỗi sai. Quy ước "Must..." dùng cho giá trị cố định trong
// code, sai là bug của lập trình viên nên panic ngay lúc khởi động là hợp lý.
var holdIDNamespace = uuid.MustParse("9b0f3c5e-2d4a-4e8b-a1f7-3c6d8e2b5a14")

// Config: các tham số nghiệp vụ (lấy từ config.Config trong main.go).
type Config struct {
	HoldTTL         time.Duration
	MaxSeatsPerHold int
}

// Service chứa mọi dependency cần cho nghiệp vụ. An toàn khi dùng đồng thời từ nhiều goroutine
// (gRPC server gọi mỗi request trong một goroutine riêng) vì pool, redis client đều thread-safe
// và Service không có state thay đổi sau khi tạo.
type Service struct {
	pool  *pgxpool.Pool     // dùng để mở transaction
	q     *repo.Queries     // các câu SQL sinh bởi sqlc (chạy ngoài transaction)
	locks *redisx.SeatLocks // lớp lọc Redis
	cfg   Config
	log   *slog.Logger
}

// New tạo Service.
func New(pool *pgxpool.Pool, locks *redisx.SeatLocks, cfg Config, log *slog.Logger) *Service {
	return &Service{pool: pool, q: repo.New(pool), locks: locks, cfg: cfg, log: log}
}

// HoldRequest là input của HoldSeats (đã parse sang uuid ở tầng grpcserver).
type HoldRequest struct {
	EventID        uuid.UUID
	UserID         uuid.UUID
	SeatIDs        []uuid.UUID
	IdempotencyKey string // client tự sinh mỗi lần bấm "giữ ghế"; retry thì gửi lại đúng key cũ
}

// HoldResult: giữ thành công thì Hold khác nil; ghế bị chiếm thì Unavailable khác rỗng.
// Ghế bị chiếm là kết quả nghiệp vụ bình thường, không phải lỗi.
//
// [Go] Hold là con trỏ (*repo.Hold) để có thể mang giá trị nil = "không có hold".
// Struct thường (không phải con trỏ) không bao giờ nil được.
type HoldResult struct {
	Hold        *repo.Hold
	Unavailable []uuid.UUID
}

// errSeatsTaken chỉ dùng để rollback transaction khi không lấy đủ ghế.
// (pgx.BeginFunc rollback khi callback trả lỗi; ta trả lỗi giả này để rollback, rồi bên ngoài
// nhận ra nó và chuyển thành kết quả "ghế bị chiếm" bình thường.)
var errSeatsTaken = errors.New("seats taken")

// HoldSeats giữ tất cả ghế yêu cầu hoặc không giữ ghế nào.
//
// hold_id được sinh cố định từ (user_id, idempotency_key): hai request trùng key chạy song song
// sẽ cùng hold_id, nên không chặn nhau ở Redis; ở Postgres request đến sau chờ trên unique index
// rồi trả lại hold của request đến trước.
//
// Luồng xử lý:
//  1. Validate input, sắp xếp seat_ids.
//  2. replay: key này đã dùng rồi -> trả lại hold cũ (idempotency), không làm gì thêm.
//  3. Redis TryHold: ghế đã có người giữ -> trả Unavailable ngay, không chạm DB.
//  4. Postgres holdInDB (1 transaction): tạo hold + chuyển ghế sang held.
//  5. Nếu Postgres thất bại -> nhả key Redis vừa set để không giữ ghế "ảo".
func (s *Service) HoldSeats(ctx context.Context, req HoldRequest) (HoldResult, error) {
	seatIDs, err := s.validateHold(req)
	if err != nil {
		// [Go] HoldResult{} là struct với mọi field là zero value (Hold = nil, Unavailable = nil).
		return HoldResult{}, err
	}
	// [Go] Hàm trả 3 giá trị (res, ok, err). ok=true nghĩa là "đã có kết quả, trả luôn".
	if res, ok, err := s.replay(ctx, req, seatIDs); ok || err != nil {
		return res, err
	}

	// Sinh hold_id cố định (UUID v5) từ user + idempotency key, xem holdIDNamespace.
	holdID := uuid.NewSHA1(holdIDNamespace, []byte(req.UserID.String()+":"+req.IdempotencyKey))
	expiresAt := time.Now().Add(s.cfg.HoldTTL)

	// 1. Redis: loại nhanh request tranh ghế đã có người giữ.
	redisHeld := false // true nếu ta đã set key Redis -> phải nhớ dọn nếu bước DB thất bại
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
	// Thất bại (lỗi, hoặc DB báo có ghế đã bị chiếm mà Redis không biết): dọn key Redis của mình.
	if err != nil || len(unavailable) > 0 {
		if redisHeld {
			s.releaseKeys(ctx, req.EventID, seatIDs, holdID)
		}
		if err != nil {
			if isConflict(err) {
				// Xung đột unique nhưng không phải do retry cùng key -> user đang có hold
				// active khác trong event này.
				return HoldResult{}, ErrActiveHoldExists
			}
			return HoldResult{}, err
		}
		return HoldResult{Unavailable: unavailable}, nil
	}
	// Thành công. Nếu trong transaction đã expire hold cũ (quá hạn) của user thì dọn nốt key
	// Redis của hold cũ (release.lua chỉ xóa key còn thuộc hold cũ, an toàn).
	if stale != nil {
		s.releaseKeys(ctx, stale.EventID, stale.SeatIds, stale.ID)
	}
	// [Go] &hold lấy địa chỉ của biến local hold. Trong Go việc này an toàn: compiler tự đưa
	// biến lên heap nếu con trỏ "thoát" ra ngoài hàm (escape analysis), không có dangling pointer.
	return HoldResult{Hold: &hold}, nil
}

// holdInDB làm toàn bộ phần Postgres của HoldSeats trong MỘT transaction:
//
//	a. Nếu user còn hold active cũ đã quá hạn -> expire nó và trả ghế của nó.
//	b. INSERT hold mới.
//	c. UPDATE các ghế sang held (chỉ những ghế còn trống / held đã quá hạn).
//	d. Không lấy đủ ghế -> rollback toàn bộ.
//
// Kết quả: thành công -> hold; thiếu ghế -> unavailable (err = nil); stale = hold cũ đã bị expire
// ở bước a (nếu có), để caller dọn Redis.
//
// [Go] Named return values (hold, unavailable, stale, err): các biến này được khai báo sẵn và
// closure bên trong pgx.BeginFunc có thể gán trực tiếp vào chúng.
func (s *Service) holdInDB(ctx context.Context, req HoldRequest, seatIDs []uuid.UUID, holdID uuid.UUID, expiresAt time.Time) (hold repo.Hold, unavailable []uuid.UUID, stale *repo.Hold, err error) {
	// pgx.BeginFunc: BEGIN, chạy callback, callback trả nil -> COMMIT, trả lỗi -> ROLLBACK.
	// Không cần tự gọi commit/rollback, và không thể quên rollback.
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// WithTx tạo bản Queries chạy mọi câu SQL bên trong transaction tx.
		q := s.q.WithTx(tx)

		// Hold active cũ của user đã quá hạn mà reaper chưa quét: expire luôn để không vướng
		// unique index "1 hold active mỗi user mỗi event". Còn hạn thì từ chối.
		//
		// [Go] Lưu ý: `active, err := ...` ở đây tạo biến err MỚI trong closure (che biến err
		// named return bên ngoài). Đây là "shadowing", hợp lệ trong Go; kết quả cuối vẫn đi ra
		// ngoài qua giá trị return của closure.
		active, err := q.GetActiveHoldForUpdate(ctx, repo.GetActiveHoldForUpdateParams{EventID: req.EventID, UserID: req.UserID})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// Không có hold active nào -> bình thường, đi tiếp. ([Go] case rỗng là hợp lệ.)
		case err != nil:
			return err
		default:
			// Có hold active: thử expire. MarkHoldExpired chỉ thành công nếu nó đã quá hạn;
			// không có dòng nào được cập nhật (ErrNoRows) = hold còn hạn -> từ chối.
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

		// UPDATE có điều kiện: chỉ ghế còn trống (hoặc held đã quá hạn) mới bị cập nhật.
		// Đây là "chốt chặn cuối" chống bán trùng, đúng kể cả khi Redis sai.
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
			// Thiếu ghế -> ghi lại ghế nào thiếu rồi trả lỗi để ROLLBACK (hủy cả INSERT hold).
			unavailable = missing(seatIDs, got)
			return errSeatsTaken
		}
		return nil
	})
	// Biến lỗi "giả" errSeatsTaken thành kết quả bình thường: không lỗi, có danh sách ghế thiếu.
	if errors.Is(err, errSeatsTaken) {
		return repo.Hold{}, unavailable, nil, nil
	}
	return hold, nil, stale, err
}

// replay trả lại hold đã tạo với cùng idempotency key (ok=true), kể cả khi hold đã hết hạn hay
// bị nhả. Cùng key nhưng khác event/ghế là lỗi của client.
//
// Idempotency: client mạng chập chờn có thể gửi lại cùng một request nhiều lần; nhờ key, mọi
// lần gửi lại đều nhận về CÙNG một hold thay vì tạo hold mới hoặc báo "ghế đã bị giữ".
func (s *Service) replay(ctx context.Context, req HoldRequest, seatIDs []uuid.UUID) (HoldResult, bool, error) {
	h, err := s.q.GetHoldByIdempotencyKey(ctx, repo.GetHoldByIdempotencyKeyParams{UserID: req.UserID, IdempotencyKey: req.IdempotencyKey})
	// Không tìm thấy = key mới, chưa dùng bao giờ -> ok=false để HoldSeats làm tiếp.
	if errors.Is(err, pgx.ErrNoRows) {
		return HoldResult{}, false, nil
	}
	if err != nil {
		return HoldResult{}, false, err
	}
	// seatIDs đã được sort trong validateHold; sort cả ghế trong DB để so sánh không phụ thuộc thứ tự.
	if h.EventID != req.EventID || !slices.Equal(sortedSeats(h.SeatIds), seatIDs) {
		return HoldResult{}, false, ErrIdempotencyMismatch
	}
	h = effective(h)
	return HoldResult{Hold: &h}, true, nil
}

// validateHold kiểm tra input và trả về seat_ids đã sắp xếp.
//
// Sắp xếp seat_ids có hai tác dụng: (1) dễ phát hiện ghế trùng (trùng thì nằm cạnh nhau),
// (2) mọi request khóa ghế theo cùng một thứ tự -> tránh deadlock, và so sánh idempotency
// không phụ thuộc thứ tự client gửi.
func (s *Service) validateHold(req HoldRequest) ([]uuid.UUID, error) {
	switch {
	case req.EventID == uuid.Nil:
		return nil, invalidArg("event_id is required")
	case req.UserID == uuid.Nil:
		return nil, invalidArg("user_id is required")
	case req.IdempotencyKey == "":
		return nil, invalidArg("idempotency_key is required")
	case len(req.IdempotencyKey) > maxIdempotencyKeyLen:
		// [Go] len(string) đếm số BYTE (UTF-8), không phải số ký tự.
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
	// [Go] Khai báo hold ở ngoài để closure bên trong gán vào, dùng tiếp sau khi transaction xong.
	var hold repo.Hold
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		var err error
		// Khóa hold trước (UPDATE ... WHERE status='active' AND user_id=...), ghế sau.
		hold, err = q.MarkHoldReleased(ctx, repo.MarkHoldReleasedParams{ID: holdID, UserID: userID})
		if err != nil {
			return err
		}
		_, err = q.ReleaseSeatsByHolds(ctx, []uuid.UUID{holdID})
		return err
	})
	// Không có dòng nào được UPDATE: hold không tồn tại / của người khác / không còn active.
	// Đọc lại hold để biết chính xác lý do và trả lỗi phù hợp.
	if errors.Is(err, pgx.ErrNoRows) {
		h, err := s.GetHold(ctx, holdID)
		switch {
		case err != nil:
			return repo.Hold{}, err // ErrNotFound
		case h.UserID != userID:
			return repo.Hold{}, ErrPermissionDenied
		case h.Status == HoldConfirmed:
			// Đã bán rồi thì không nhả được.
			return repo.Hold{}, ErrHoldNotActive
		default:
			// released / expired: kết quả cuối giống như nhả thành công -> trả lại (idempotent).
			return h, nil
		}
	}
	if err != nil {
		return repo.Hold{}, err
	}
	// Chỉ dọn Redis SAU KHI Postgres đã commit.
	s.releaseKeys(ctx, hold.EventID, hold.SeatIds, hold.ID)
	return hold, nil
}

// ConfirmHold chuyển ghế held -> sold. Idempotent: hold đã confirm thì trả lại như cũ.
func (s *Service) ConfirmHold(ctx context.Context, holdID uuid.UUID) (repo.Hold, error) {
	var hold repo.Hold
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		var err error
		// Chỉ confirm được hold đang active VÀ chưa hết hạn.
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
			// Trả lỗi -> ROLLBACK, hold vẫn active như cũ.
			return ErrHoldExpired
		}
		return nil
	})
	// Không confirm được: đọc lại để biết lý do.
	if errors.Is(err, pgx.ErrNoRows) {
		h, err := s.GetHold(ctx, holdID)
		switch {
		case err != nil:
			return repo.Hold{}, err
		case h.Status == HoldConfirmed:
			return h, nil // đã confirm trước đó -> idempotent
		case h.Status == HoldExpired:
			return repo.Hold{}, ErrHoldExpired
		default:
			return repo.Hold{}, ErrHoldNotActive // đã released
		}
	}
	if err != nil {
		return repo.Hold{}, err
	}
	// Ghi key Redis vĩnh viễn để chặn sớm request sau. Lỗi chỉ log: Postgres vẫn chặn đúng.
	//
	// [Go] context.WithoutCancel(ctx): ctx mới giữ các value của ctx gốc nhưng KHÔNG bị hủy theo.
	// Lý do: DB đã commit rồi, dù client vừa hủy request thì vẫn nên cập nhật Redis cho xong.
	if err := s.locks.MarkSold(context.WithoutCancel(ctx), hold.EventID, hold.SeatIds, hold.ID); err != nil {
		s.log.WarnContext(ctx, "redis mark sold failed", "hold_id", hold.ID, "err", err)
	}
	return hold, nil
}

// GetHold trả hold với trạng thái hiệu lực: active nhưng đã quá hạn thì báo expired.
func (s *Service) GetHold(ctx context.Context, holdID uuid.UUID) (repo.Hold, error) {
	h, err := s.q.GetHold(ctx, holdID)
	// Đổi lỗi của thư viện (pgx.ErrNoRows) sang lỗi nghiệp vụ, để tầng trên không phụ thuộc pgx.
	if errors.Is(err, pgx.ErrNoRows) {
		return repo.Hold{}, ErrNotFound
	}
	if err != nil {
		return repo.Hold{}, err
	}
	return effective(h), nil
}

// InitEventInventory tạo ghế cho event. Idempotent. Trả về tổng số ghế của event.
// (Gọi lại nhiều lần với cùng danh sách ghế không tạo trùng, xem InitSeats trong queries/seats.sql.)
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
	// Trả TỔNG số ghế (không phải số ghế mới thêm) để lần gọi lặp lại vẫn nhận cùng kết quả.
	return s.q.CountSeatsByEvent(ctx, eventID)
}

// ListSeatAvailability trả trạng thái từng ghế của event (đã tính ghế held quá hạn là available).
func (s *Service) ListSeatAvailability(ctx context.Context, eventID uuid.UUID) ([]repo.ListSeatsByEventRow, error) {
	if eventID == uuid.Nil {
		return nil, invalidArg("event_id is required")
	}
	return s.q.ListSeatsByEvent(ctx, eventID)
}

// ExpireDueHolds expire một lô hold quá hạn và trả ghế về available. Reaper gọi định kỳ.
// Method này giúp *Service thỏa interface reaper.Expirer.
func (s *Service) ExpireDueHolds(ctx context.Context, batchSize int32) (int, error) {
	var due []repo.ExpireDueHoldsRow
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		var err error
		// Bước 1: đổi tối đa batchSize hold quá hạn sang expired (khóa hold trước).
		due, err = q.ExpireDueHolds(ctx, batchSize)
		if err != nil || len(due) == 0 {
			return err
		}
		ids := make([]uuid.UUID, len(due))
		for i, h := range due {
			ids[i] = h.ID
		}
		// Bước 2: trả ghế của các hold đó về available (khóa ghế sau).
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
	// WithoutCancel: không bị hủy theo ctx gốc; WithTimeout: nhưng cũng không chờ quá 2s.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if _, err := s.locks.Release(ctx, eventID, seatIDs, holdID); err != nil {
		s.log.WarnContext(ctx, "redis release failed", "hold_id", holdID, "err", err)
	}
}

// effective tính trạng thái "hiệu lực" của hold: trong DB còn ghi active nhưng đã quá hạn
// (reaper chưa kịp quét) thì coi là expired. Không ghi gì vào DB.
//
// [Go] h được truyền theo GIÁ TRỊ (bản copy của struct), nên sửa h.Status ở đây không ảnh
// hưởng biến của caller; ta trả về bản đã sửa.
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
	// [Go] errors.As tìm trong chuỗi lỗi (kể cả lỗi bị wrap) một lỗi có KIỂU *pgconn.PgError,
	// nếu thấy thì gán vào pgErr và trả true. Khác errors.Is (so sánh giá trị), As so sánh kiểu,
	// dùng khi cần đọc thêm thông tin trong lỗi (ở đây là mã lỗi Postgres và tên bảng).
	var pgErr *pgconn.PgError
	// 23505 = unique_violation (mã lỗi chuẩn của Postgres).
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.TableName == "holds"
}

// sortedSeats trả về bản sao đã sắp xếp (không sửa slice gốc của caller).
//
// [Go] uuid.UUID là mảng [16]byte; a[:] chuyển mảng thành slice để bytes.Compare so sánh.
// SortFunc nhận hàm so sánh trả âm/0/dương, giống arr.sort((a, b) => ...) trong JS.
func sortedSeats(ids []uuid.UUID) []uuid.UUID {
	out := slices.Clone(ids)
	slices.SortFunc(out, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	return out
}

// missing trả về các ghế trong want mà không có trong got.
// (O(n*m) nhưng n <= MaxSeatsPerHold rất nhỏ nên không đáng dùng map.)
func missing(want, got []uuid.UUID) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(want)-len(got))
	for _, id := range want {
		if !slices.Contains(got, id) {
			out = append(out, id)
		}
	}
	return out
}
