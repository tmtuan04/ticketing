// Integration test cho package service, chạy với Postgres và Redis THẬT (trong Docker).
//
// Cách chạy:
//
//	go test ./...          # chạy tất cả, cần Docker đang chạy (testcontainers tự dựng container)
//	go test -short ./...   # bỏ qua integration test (không cần Docker)
//	go test -race ./...    # bật race detector, bắt lỗi truy cập dữ liệu đồng thời
//	go test -run Contention -v ./internal/service   # chỉ chạy test có tên khớp "Contention"
//
// [Go] Quy ước test của Go:
//   - File test có đuôi _test.go, chỉ được biên dịch khi chạy `go test`.
//   - Hàm test có dạng func TestXxx(t *testing.T). Không cần framework như Jest.
//   - t.Fatal/Fatalf: báo lỗi và DỪNG test hiện tại; t.Error/Errorf: báo lỗi nhưng chạy tiếp.
//   - `package service_test` (có hậu tố _test) là "black-box test": test nằm ở package riêng,
//     chỉ dùng được những gì service export ra, giống cách code bên ngoài dùng nó.
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
// Hai biến cấp package này được TestMain gán một lần và mọi test dùng chung.
var (
	pool *pgxpool.Pool
	rdb  *redis.Client
)

// TestMain là hook đặc biệt: nếu có, `go test` gọi nó THAY VÌ chạy thẳng các test, để ta
// setup/teardown dùng chung cho cả package (giống beforeAll/afterAll của Jest).
// m.Run() mới thực sự chạy các TestXxx và trả về exit code.
func TestMain(m *testing.M) {
	// Phải parse flag trước thì testing.Short() mới đọc được cờ -short.
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	// Tách ra hàm riêng để các defer (tắt container, đóng pool) chạy xong TRƯỚC os.Exit
	// (os.Exit không chạy defer, cùng lý do như main/run trong cmd/server/main.go).
	code, err := runWithContainers(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

// runWithContainers dựng Postgres + Redis bằng Docker, chạy migration, chạy test, rồi dọn dẹp.
func runWithContainers(m *testing.M) (int, error) {
	ctx := context.Background()

	migrations, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil || len(migrations) == 0 {
		return 0, fmt.Errorf("no migrations found: %v", err)
	}
	// Sắp xếp theo tên để migration chạy đúng thứ tự 000001, 000002, ...
	slices.Sort(migrations)
	// Postgres container chạy các file *.up.sql làm init script -> schema giống hệt production.
	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("inventory_db"),
		tcpostgres.WithUsername("app"),
		tcpostgres.WithPassword("app"),
		tcpostgres.WithOrderedInitScripts(migrations...),
		tcpostgres.BasicWaitStrategies(),
	)
	// Đặt defer TRƯỚC khi kiểm tra err: kể cả khi Run lỗi giữa chừng, container (nếu đã tạo)
	// vẫn được dọn. TerminateContainer an toàn với giá trị nil.
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
	// Giới hạn 20 connection: test 1000 goroutine sẽ phải xếp hàng chờ connection, mô phỏng
	// tải thật và giúp lộ lỗi tranh chấp.
	pcfg.MaxConns = 20
	// [Go] `if pool, err = ...; err != nil`: dùng `=` (không phải `:=`) để gán vào biến pool
	// cấp package; nếu dùng `:=` sẽ tạo biến pool local mới và biến package vẫn nil.
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
	// Pool Redis lớn hơn để 1000 goroutine không phải chờ connection Redis quá lâu.
	opt.PoolSize = 100
	rdb = redis.NewClient(opt)
	defer rdb.Close()

	return m.Run(), nil
}

// newService tạo Service dùng Postgres/Redis chung, với HoldTTL tùy test.
// Chạy với -short thì pool == nil -> bỏ qua (skip) test.
//
// [Go] t.Helper() đánh dấu đây là hàm phụ trợ: khi test lỗi, Go báo dòng code ở hàm TEST gọi
// tới helper, thay vì dòng bên trong helper -> dễ tìm chỗ hỏng.
func newService(t *testing.T, ttl time.Duration) *service.Service {
	t.Helper()
	if pool == nil {
		t.Skip("integration test: chạy không có -short")
	}
	return newServiceWith(rdb, ttl)
}

// newServiceWith cho phép truyền Redis client khác (vd client trỏ tới Redis "chết" trong
// TestHoldSeats_RedisDown). Log bị bỏ đi (io.Discard) để output test gọn.
func newServiceWith(c *redis.Client, ttl time.Duration) *service.Service {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return service.New(pool, redisx.New(c), service.Config{HoldTTL: ttl, MaxSeatsPerHold: 8}, log)
}

// newEvent tạo một event mới với `seats` ghế ngẫu nhiên. Mỗi test dùng event_id riêng nên
// các test không ảnh hưởng dữ liệu của nhau dù dùng chung một DB.
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

// hold là helper "giữ ghế và BẮT BUỘC thành công", fail test ngay nếu không giữ được.
// [Go] `seats ...uuid.UUID` là variadic: gọi hold(t, svc, ev, user, s1, s2) hoặc
// hold(t, svc, ev, user, seats...) đều được.
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
// Đây là test quan trọng nhất: chứng minh không bao giờ có 2 người cùng giữ 1 ghế, và
// Postgres + Redis nhất quán với nhau sau cơn bão request.
func TestHoldSeats_Contention(t *testing.T) {
	svc := newService(t, time.Minute)
	ctx := context.Background()
	ev, seats := newEvent(t, svc, 10)

	const workers = 1000
	// [Go] Nhiều goroutine cùng ghi vào winners/errs/losers -> phải bảo vệ bằng sync.Mutex,
	// nếu không sẽ bị "data race" (go test -race sẽ phát hiện).
	// `start` là channel dùng làm "cổng xuất phát": mọi goroutine chờ ở <-start, close(start)
	// thả tất cả cùng lúc để tối đa hóa tranh chấp.
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
			// rand.Perm(n) = hoán vị ngẫu nhiên 0..n-1; lấy 2 phần tử đầu = 2 ghế khác nhau.
			pick := rand.Perm(len(seats))[:2]
			<-start
			res, err := svc.HoldSeats(ctx, service.HoldRequest{
				EventID:        ev,
				UserID:         uuid.New(),
				SeatIDs:        []uuid.UUID{seats[pick[0]], seats[pick[1]]},
				IdempotencyKey: uuid.NewString(),
			})
			// Khóa mutex trước khi ghi vào biến dùng chung; defer mở khóa khi closure kết thúc.
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
	close(start) // thả tất cả goroutine cùng lúc
	wg.Wait()    // chờ tất cả xong

	for _, err := range errs {
		t.Error(err)
	}
	if len(winners) == 0 {
		t.Fatal("không ai giữ được ghế")
	}

	// Mỗi ghế tối đa 1 người giữ.
	// [Go] map[K]V là bảng băm (giống Map/object trong JS). `map[K]V{}` tạo map rỗng.
	// `prev, ok := owner[s]`: ok=false nếu key chưa có (mẫu "comma ok").
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
	// [Go] Chuỗi trong dấu backtick là "raw string": không xử lý escape, viết được nhiều dòng.
	// Duyệt kết quả: for rows.Next() { rows.Scan(&a, &b) }, cuối cùng kiểm tra rows.Err().
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
	// (redis.Nil là lỗi go-redis trả về khi key không tồn tại.)
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

// Idempotency: gửi lại cùng request (cùng idempotency key) bao nhiêu lần cũng nhận cùng một hold.
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
	// [Go] `for range 20` (Go 1.22+) lặp 20 lần, không cần biến đếm.
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

// Mỗi user chỉ được có 1 hold active trong một event (unique index trong migration).
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

// Input sai phải trả ErrInvalidArgument.
//
// [Go] "Table-driven test": liệt kê các ca test trong một map/slice rồi lặp qua, mỗi ca chạy
// thành sub-test bằng t.Run(name, ...). Output hiển thị từng ca riêng, ví dụ
// TestHoldSeats_Validation/thiếu_key. Đây là phong cách test rất phổ biến trong Go.
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

	// Chờ quá TTL. GetHold phải báo expired ngay cả khi reaper chưa chạy (xem effective()).
	time.Sleep(ttl + 500*time.Millisecond)
	got, err := svc.GetHold(ctx, hA.ID)
	must(t, err)
	if got.Status != service.HoldExpired {
		t.Fatalf("GetHold: status %s, muốn expired", got.Status)
	}

	// Gọi trực tiếp hàm mà reaper gọi định kỳ, thay vì chạy reaper thật (test nhanh, xác định).
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
	// Service thứ hai với TTL dài, để hold mới của B/A không hết hạn giữa chừng test.
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

// Kiểm tra Confirm/Release: chuyển trạng thái đúng, idempotent, đúng quyền, Redis đồng bộ.
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
		// PTTL trả -1 nghĩa là key tồn tại và không có TTL.
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
	// Client trỏ tới cổng 1 (không có gì lắng nghe) -> mọi lệnh Redis đều lỗi kết nối.
	// MaxRetries: -1 tắt retry, DialTimeout ngắn để test chạy nhanh.
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

// must: lỗi bất kỳ -> fail test ngay. Giúp test gọn hơn so với viết if err != nil khắp nơi.
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// wantErr: khẳng định err là (hoặc bọc) lỗi target, so sánh bằng errors.Is.
func wantErr(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("muốn lỗi %v, nhận %v", target, err)
	}
}
