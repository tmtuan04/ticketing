package service_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/tmtuan04/ticketing/inventory/internal/redisx"
	"github.com/tmtuan04/ticketing/inventory/internal/repo"
	"github.com/tmtuan04/ticketing/inventory/internal/service"
)

// Postgres và Redis thật, dựng bằng testcontainers. `go test -short` bỏ qua.
var (
	pool *pgxpool.Pool
	rdb  *redis.Client
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	code, err := runWithContainers(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func runWithContainers(m *testing.M) (int, error) {
	ctx := context.Background()

	migrations, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil || len(migrations) == 0 {
		return 0, fmt.Errorf("no migrations found: %v", err)
	}
	slices.Sort(migrations)
	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory_db"),
		tcpostgres.WithUsername("app"),
		tcpostgres.WithPassword("app"),
		tcpostgres.WithOrderedInitScripts(migrations...),
		tcpostgres.BasicWaitStrategies(),
	)
	defer func() { _ = testcontainers.TerminateContainer(pg) }()
	if err != nil {
		return 0, err
	}
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return 0, err
	}
	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return 0, err
	}
	pcfg.MaxConns = 20
	if pool, err = pgxpool.NewWithConfig(ctx, pcfg); err != nil {
		return 0, err
	}
	defer pool.Close()

	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	defer func() { _ = testcontainers.TerminateContainer(rc) }()
	if err != nil {
		return 0, err
	}
	uri, err := rc.ConnectionString(ctx)
	if err != nil {
		return 0, err
	}
	opt, err := redis.ParseURL(uri)
	if err != nil {
		return 0, err
	}
	opt.PoolSize = 100
	rdb = redis.NewClient(opt)
	defer rdb.Close()

	return m.Run(), nil
}

func newService(t *testing.T, ttl time.Duration) *service.Service {
	t.Helper()
	if pool == nil {
		t.Skip("integration test: chạy không có -short")
	}
	return newServiceWith(rdb, ttl)
}

func newServiceWith(c *redis.Client, ttl time.Duration) *service.Service {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return service.New(pool, redisx.New(c), service.Config{HoldTTL: ttl, MaxSeatsPerHold: 8}, log)
}

func newEvent(t *testing.T, svc *service.Service, seats int) (uuid.UUID, []uuid.UUID) {
	t.Helper()
	ev := uuid.New()
	ids := make([]uuid.UUID, seats)
	for i := range ids {
		ids[i] = uuid.New()
	}
	n, err := svc.InitEventInventory(context.Background(), ev, ids)
	must(t, err)
	if n != int64(seats) {
		t.Fatalf("init: got %d seats, want %d", n, seats)
	}
	return ev, ids
}

func hold(t *testing.T, svc *service.Service, ev, user uuid.UUID, seats ...uuid.UUID) repo.Hold {
	t.Helper()
	res, err := svc.HoldSeats(context.Background(), service.HoldRequest{
		EventID: ev, UserID: user, SeatIDs: seats, IdempotencyKey: uuid.NewString(),
	})
	must(t, err)
	if res.Hold == nil {
		t.Fatalf("hold: seats unavailable %v", res.Unavailable)
	}
	return *res.Hold
}

// 1000 goroutine cùng lúc tranh 10 ghế, mỗi goroutine xin 2 ghế ngẫu nhiên.
func TestHoldSeats_Contention(t *testing.T) {
	svc := newService(t, time.Minute)
	ctx := context.Background()
	ev, seats := newEvent(t, svc, 10)

	const workers = 1000
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		start   = make(chan struct{})
		winners []repo.Hold
		errs    []error
		losers  int
	)
	for range workers {
		wg.Go(func() {
			pick := rand.Perm(len(seats))[:2]
			<-start
			res, err := svc.HoldSeats(ctx, service.HoldRequest{
				EventID:        ev,
				UserID:         uuid.New(),
				SeatIDs:        []uuid.UUID{seats[pick[0]], seats[pick[1]]},
				IdempotencyKey: uuid.NewString(),
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				errs = append(errs, err)
			case res.Hold != nil:
				winners = append(winners, *res.Hold)
			default:
				losers++
			}
		})
	}
	close(start)
	wg.Wait()

	for _, err := range errs {
		t.Error(err)
	}
	if len(winners) == 0 {
		t.Fatal("không ai giữ được ghế")
	}

	// Mỗi ghế tối đa 1 người giữ.
	owner := map[uuid.UUID]uuid.UUID{}
	for _, h := range winners {
		for _, s := range h.SeatIds {
			if prev, ok := owner[s]; ok {
				t.Fatalf("ghế %s có 2 hold: %s và %s", s, prev, h.ID)
			}
			owner[s] = h.ID
		}
	}

	// Postgres khớp đúng tập người thắng, không có hold mồ côi của người thua.
	rows, err := pool.Query(ctx, `SELECT seat_id, hold_id FROM seat_inventory WHERE event_id = $1 AND status = 'held'`, ev)
	must(t, err)
	dbOwner := map[uuid.UUID]uuid.UUID{}
	for rows.Next() {
		var s, h uuid.UUID
		must(t, rows.Scan(&s, &h))
		dbOwner[s] = h
	}
	must(t, rows.Err())
	if !maps.Equal(owner, dbOwner) {
		t.Fatalf("DB lệch: winners=%v db=%v", owner, dbOwner)
	}
	var holdCount int
	must(t, pool.QueryRow(ctx, `SELECT count(*) FROM holds WHERE event_id = $1`, ev).Scan(&holdCount))
	if holdCount != len(winners) {
		t.Fatalf("holds trong DB = %d, winners = %d", holdCount, len(winners))
	}

	// Redis khớp Postgres: ghế được giữ có key đúng hold_id, ghế trống không còn key rác.
	for _, s := range seats {
		val, err := rdb.Get(ctx, redisx.SeatKey(ev, s)).Result()
		h, held := owner[s]
		switch {
		case held && (err != nil || val != h.String()):
			t.Errorf("ghế %s: redis=%q err=%v, muốn %s", s, val, err, h)
		case !held && !errors.Is(err, redis.Nil):
			t.Errorf("ghế %s trống nhưng redis còn key %q", s, val)
		}
	}
	t.Logf("%d worker: %d giữ được, %d bị từ chối", workers, len(winners), losers)
}

func TestHoldSeats_Idempotency(t *testing.T) {
	svc := newService(t, time.Minute)
	ctx := context.Background()
	ev, seats := newEvent(t, svc, 4)
	user := uuid.New()
	req := service.HoldRequest{EventID: ev, UserID: user, SeatIDs: seats[:2], IdempotencyKey: "retry-1"}

	// Client retry dồn dập cùng key: mọi request nhận cùng một hold, không ai bị báo ghế bận.
	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		ids = map[uuid.UUID]int{}
	)
	for range 20 {
		wg.Go(func() {
			res, err := svc.HoldSeats(ctx, req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Error(err)
				return
			}
			if res.Hold == nil {
				t.Errorf("retry bị báo ghế bận: %v", res.Unavailable)
				return
			}
			ids[res.Hold.ID]++
		})
	}
	wg.Wait()
	if len(ids) != 1 {
		t.Fatalf("muốn 1 hold duy nhất, nhận %v", ids)
	}

	// Đảo thứ tự ghế vẫn là cùng request.
	res, err := svc.HoldSeats(ctx, service.HoldRequest{EventID: ev, UserID: user, SeatIDs: []uuid.UUID{seats[1], seats[0]}, IdempotencyKey: "retry-1"})
	must(t, err)
	if _, ok := ids[res.Hold.ID]; !ok {
		t.Fatalf("đảo thứ tự ghế tạo hold mới %s", res.Hold.ID)
	}

	// Cùng key, khác ghế: lỗi client.
	_, err = svc.HoldSeats(ctx, service.HoldRequest{EventID: ev, UserID: user, SeatIDs: seats[2:], IdempotencyKey: "retry-1"})
	wantErr(t, err, service.ErrIdempotencyMismatch)
}

func TestHoldSeats_OneActiveHoldPerEvent(t *testing.T) {
	svc := newService(t, time.Minute)
	ctx := context.Background()
	ev, seats := newEvent(t, svc, 2)
	user := uuid.New()
	hold(t, svc, ev, user, seats[0])

	_, err := svc.HoldSeats(ctx, service.HoldRequest{EventID: ev, UserID: user, SeatIDs: seats[1:], IdempotencyKey: "second"})
	wantErr(t, err, service.ErrActiveHoldExists)
	// Key Redis của request thất bại phải được dọn, người khác vẫn giữ được ghế đó.
	if n := rdb.Exists(ctx, redisx.SeatKey(ev, seats[1])).Val(); n != 0 {
		t.Fatal("request thất bại để lại key redis")
	}
	hold(t, svc, ev, uuid.New(), seats[1])
}

func TestHoldSeats_Validation(t *testing.T) {
	svc := newService(t, time.Minute)
	ev, user, s := uuid.New(), uuid.New(), uuid.New()
	tooMany := make([]uuid.UUID, 9)
	for i := range tooMany {
		tooMany[i] = uuid.New()
	}
	for name, req := range map[string]service.HoldRequest{
		"thiếu key":   {EventID: ev, UserID: user, SeatIDs: []uuid.UUID{s}},
		"không ghế":   {EventID: ev, UserID: user, IdempotencyKey: "k"},
		"quá 8 ghế":   {EventID: ev, UserID: user, SeatIDs: tooMany, IdempotencyKey: "k"},
		"ghế trùng":   {EventID: ev, UserID: user, SeatIDs: []uuid.UUID{s, s}, IdempotencyKey: "k"},
		"thiếu event": {UserID: user, SeatIDs: []uuid.UUID{s}, IdempotencyKey: "k"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := svc.HoldSeats(context.Background(), req)
			wantErr(t, err, service.ErrInvalidArgument)
		})
	}
}

// Hold hết hạn: reaper trả ghế về available, người khác giữ được.
func TestHoldExpiry_Reaper(t *testing.T) {
	const ttl = time.Second
	svc := newService(t, ttl)
	ctx := context.Background()
	ev, seats := newEvent(t, svc, 2)
	hA := hold(t, svc, ev, uuid.New(), seats...)

	time.Sleep(ttl + 500*time.Millisecond)
	got, err := svc.GetHold(ctx, hA.ID)
	must(t, err)
	if got.Status != service.HoldExpired {
		t.Fatalf("GetHold: status %s, muốn expired", got.Status)
	}

	n, err := svc.ExpireDueHolds(ctx, 100)
	must(t, err)
	if n < 1 {
		t.Fatal("reaper không expire hold nào")
	}
	var available int
	must(t, pool.QueryRow(ctx, `SELECT count(*) FROM seat_inventory WHERE event_id = $1 AND status = 'available' AND hold_id IS NULL`, ev).Scan(&available))
	if available != len(seats) {
		t.Fatalf("sau reaper: %d ghế available, muốn %d", available, len(seats))
	}

	hold(t, svc, ev, uuid.New(), seats...)
	_, err = svc.ConfirmHold(ctx, hA.ID)
	wantErr(t, err, service.ErrHoldExpired)
}

// Hold hết hạn nhưng reaper chưa chạy: người khác vẫn giành được ghế, và chủ hold cũ
// giữ ghế khác được ngay (hold cũ tự expire trong transaction).
func TestHoldExpiry_ReclaimWithoutReaper(t *testing.T) {
	const ttl = time.Second
	svc := newService(t, ttl)
	ctx := context.Background()
	ev, seats := newEvent(t, svc, 2)
	userA, userB := uuid.New(), uuid.New()
	hA := hold(t, svc, ev, userA, seats[0])

	time.Sleep(ttl + 500*time.Millisecond)
	svcLong := newServiceWith(rdb, time.Minute)
	hB := hold(t, svcLong, ev, userB, seats[0])
	hold(t, svcLong, ev, userA, seats[1])

	got, err := svc.GetHold(ctx, hA.ID)
	must(t, err)
	if got.Status != service.HoldExpired {
		t.Fatalf("hold cũ của A: %s, muốn expired", got.Status)
	}
	// Expire hold cũ của A không được nhả ghế đã thuộc về B.
	rows, err := svc.ListSeatAvailability(ctx, ev)
	must(t, err)
	for _, r := range rows {
		if r.Status != service.SeatHeld {
			t.Fatalf("ghế %s: %s, muốn held", r.SeatID, r.Status)
		}
	}
	_, err = svcLong.ConfirmHold(ctx, hB.ID)
	must(t, err)
}

func TestConfirmAndRelease(t *testing.T) {
	svc := newService(t, time.Minute)
	ctx := context.Background()
	ev, seats := newEvent(t, svc, 3)
	userA, userB, userC := uuid.New(), uuid.New(), uuid.New()

	// Confirm: ghế sold, key Redis giữ vĩnh viễn để chặn request sau ngay ở Redis.
	hA := hold(t, svc, ev, userA, seats[0], seats[1])
	confirmed, err := svc.ConfirmHold(ctx, hA.ID)
	must(t, err)
	if confirmed.Status != service.HoldConfirmed {
		t.Fatalf("status %s", confirmed.Status)
	}
	if ttl := rdb.PTTL(ctx, redisx.SeatKey(ev, seats[0])).Val(); ttl != -1 {
		t.Fatalf("key ghế đã bán còn TTL %s", ttl)
	}
	again, err := svc.ConfirmHold(ctx, hA.ID)
	must(t, err)
	if again.ID != hA.ID {
		t.Fatal("confirm lần 2 không idempotent")
	}
	_, err = svc.ReleaseHold(ctx, hA.ID, userA)
	wantErr(t, err, service.ErrHoldNotActive)

	res, err := svc.HoldSeats(ctx, service.HoldRequest{EventID: ev, UserID: userB, SeatIDs: seats[:1], IdempotencyKey: "b"})
	must(t, err)
	if res.Hold != nil || len(res.Unavailable) != 1 {
		t.Fatalf("giữ được ghế đã bán: %+v", res)
	}

	// Release: chỉ chủ hold được nhả; nhả xong ghế trống cả ở DB lẫn Redis.
	hC := hold(t, svc, ev, userC, seats[2])
	_, err = svc.ReleaseHold(ctx, hC.ID, userA)
	wantErr(t, err, service.ErrPermissionDenied)
	released, err := svc.ReleaseHold(ctx, hC.ID, userC)
	must(t, err)
	if released.Status != service.HoldReleased {
		t.Fatalf("status %s", released.Status)
	}
	if rdb.Exists(ctx, redisx.SeatKey(ev, seats[2])).Val() != 0 {
		t.Fatal("release không xóa key redis")
	}
	_, err = svc.ReleaseHold(ctx, hC.ID, userC)
	must(t, err)
	_, err = svc.ConfirmHold(ctx, hC.ID)
	wantErr(t, err, service.ErrHoldNotActive)
	hold(t, svc, ev, userB, seats[2])

	_, err = svc.GetHold(ctx, uuid.New())
	wantErr(t, err, service.ErrNotFound)
}

// Redis sập: mọi thứ vẫn đúng nhờ Postgres, chỉ mất lớp lọc nhanh.
func TestHoldSeats_RedisDown(t *testing.T) {
	newService(t, time.Minute) // skip nếu -short
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 100 * time.Millisecond})
	defer dead.Close()
	svc := newServiceWith(dead, time.Minute)
	ctx := context.Background()
	ev, seats := newEvent(t, svc, 1)

	hold(t, svc, ev, uuid.New(), seats[0])
	res, err := svc.HoldSeats(ctx, service.HoldRequest{EventID: ev, UserID: uuid.New(), SeatIDs: seats, IdempotencyKey: "x"})
	must(t, err)
	if res.Hold != nil || len(res.Unavailable) != 1 || res.Unavailable[0] != seats[0] {
		t.Fatalf("muốn ghế bị chiếm, nhận %+v", res)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func wantErr(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("muốn lỗi %v, nhận %v", target, err)
	}
}
