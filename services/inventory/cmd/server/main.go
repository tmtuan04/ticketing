// Điểm khởi động (entrypoint) của inventory service.
//
// Nhiệm vụ của file này chỉ là "lắp ráp" (wiring): đọc config, mở kết nối Postgres/Redis,
// tạo tầng service, dựng gRPC server, chạy reaper nền, rồi chờ tín hiệu tắt để dừng êm.
// Toàn bộ nghiệp vụ nằm trong internal/service.
//
// [Go] Quy ước thư mục: cmd/<tên-binary>/main.go chứa hàm main; code dùng chung đặt trong
// internal/. Package nằm dưới internal/ chỉ được import bởi code trong cùng module
// (Go compiler chặn import từ module khác) -> tương tự "private package".
//
// [Go] Một chương trình chạy được phải có `package main` và hàm `func main()`.
package main

// [Go] import theo nhóm: thư viện chuẩn (standard library) trước, thư viện bên ngoài sau,
// cuối cùng là package nội bộ của project. goimports/gofmt tự sắp xếp trong từng nhóm.
import (
	"context"
	"fmt"
	"log/slog" // logger có cấu trúc (structured logging) có sẵn từ Go 1.21
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool" // driver Postgres + connection pool
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	// [Go] Đặt alias khi import: tên package gốc là grpc_health_v1, ta gọi nó là healthpb.
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	// Code Go sinh ra từ file .proto (thư mục gen/go ở gốc repo).
	inventoryv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/inventory/v1"
	"github.com/tmtuan04/ticketing/inventory/internal/config"
	"github.com/tmtuan04/ticketing/inventory/internal/grpcserver"
	"github.com/tmtuan04/ticketing/inventory/internal/reaper"
	"github.com/tmtuan04/ticketing/inventory/internal/redisx"
	"github.com/tmtuan04/ticketing/inventory/internal/service"
)

// Thời gian tối đa chờ các RPC đang chạy hoàn tất khi tắt server; quá hạn thì cắt ngang.
//
// [Go] time.Duration thực chất là int64 đếm nano giây; 10 * time.Second = 10 giây.
const shutdownTimeout = 10 * time.Second

func main() {
	// Image distroless không có shell/curl: docker healthcheck gọi lại chính binary này.
	// (docker-compose chạy: /inventory healthcheck)
	//
	// [Go] os.Args[0] là đường dẫn binary, os.Args[1:] là các tham số dòng lệnh.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}

	// [Go] Mẫu phổ biến: main() chỉ gọi run() và xử lý lỗi một chỗ. Lý do: os.Exit() thoát
	// ngay lập tức và KHÔNG chạy các lệnh `defer`, nên ta để mọi defer (đóng pool, đóng redis...)
	// nằm trong run(); run() return xong thì các defer đã chạy hết rồi mới os.Exit.
	//
	// [Go] `if err := run(); err != nil {...}`: khai báo biến ngay trong câu if, biến err chỉ
	// tồn tại trong khối if này.
	if err := run(); err != nil {
		slog.Error("inventory stopped", "err", err)
		os.Exit(1)
	}
}

// [Go] Go không có exception. Hàm có thể lỗi sẽ trả về `error` (thường là giá trị trả về cuối
// cùng); caller phải tự kiểm tra `if err != nil`. error == nil nghĩa là thành công.
func run() error {
	// [Go] `:=` là khai báo + gán, kiểu được suy ra tự động. Hàm trả về nhiều giá trị
	// (cfg, err) là chuyện bình thường trong Go.
	cfg, err := config.Load()
	if err != nil {
		// [Go] fmt.Errorf với %w "bọc" (wrap) lỗi gốc, thêm ngữ cảnh "config: ..." nhưng vẫn
		// giữ lỗi gốc bên trong để caller có thể kiểm tra bằng errors.Is / errors.As.
		return fmt.Errorf("config: %w", err)
	}

	// Logger ghi JSON ra stdout (dễ cho Docker/ELK thu thập). slog.SetDefault để các chỗ gọi
	// slog.Info/Error trực tiếp (như trong main) cũng dùng chung cấu hình này.
	//
	// [Go] &slog.HandlerOptions{...} tạo struct rồi lấy con trỏ (&) tới nó.
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	// ctx sẽ bị "hủy" (cancel) khi process nhận Ctrl+C (SIGINT) hoặc SIGTERM (docker stop).
	// Mọi thứ chạy dài (reaper, truy vấn DB...) nhận ctx này và tự dừng khi ctx bị hủy.
	//
	// [Go] context.Context là cơ chế chuẩn của Go để truyền "tín hiệu hủy" và deadline xuyên
	// qua các lời gọi hàm/goroutine. Quy ước: ctx luôn là tham số đầu tiên của hàm.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// [Go] `defer` hoãn lời gọi đến lúc hàm bao quanh (run) return, bất kể return ở đâu hay
	// return vì lỗi. Các defer chạy theo thứ tự ngược (LIFO). Dùng để dọn tài nguyên,
	// tương tự finally trong JS/Java.
	defer stop()

	// --- Postgres ---------------------------------------------------------------
	// pgxpool quản lý một pool nhiều connection, an toàn khi dùng đồng thời từ nhiều goroutine.
	// pgxpool.New chưa thật sự kết nối (lazy), nên phải Ping bên dưới để biết DB có sống không.
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()

	// Ping với timeout 5s để không treo vô hạn nếu DB không phản hồi.
	// [Go] context.WithTimeout tạo ctx con, tự hủy sau 5s; cancel() giải phóng timer sớm.
	// Luôn phải gọi cancel (thường bằng defer), nếu không `go vet` sẽ cảnh báo rò rỉ.
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("postgres ping: %w", err)
	}

	// --- Redis ------------------------------------------------------------------
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		// Redis chỉ là lớp lọc nhanh: thiếu Redis thì chậm hơn nhưng vẫn đúng.
		// Vì vậy chỉ cảnh báo chứ KHÔNG return lỗi như Postgres.
		//
		// [Go] slog nhận cặp key-value xen kẽ: "err", err -> {"err": "..."} trong log JSON.
		log.Warn("redis unavailable, holds will go straight to postgres", "err", err)
	}

	// --- Tầng nghiệp vụ -----------------------------------------------------------
	// Dependency injection thủ công: tạo dependency rồi truyền vào constructor.
	// Go không có DI container như NestJS; truyền tay thế này là cách phổ biến nhất.
	svc := service.New(pool, redisx.New(rdb), service.Config{
		HoldTTL:         cfg.HoldTTL,
		MaxSeatsPerHold: cfg.MaxSeatsPerHold,
	}, log)

	// --- gRPC server ------------------------------------------------------------
	// Mở cổng TCP (mặc định :50051) trước, rồi mới giao cho gRPC server phục vụ.
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	// Interceptor = middleware của gRPC: chạy bao quanh mọi RPC (log, bắt panic...).
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(grpcserver.UnaryInterceptor(log)))
	// Đăng ký implementation của InventoryService (định nghĩa trong .proto) vào server.
	inventoryv1.RegisterInventoryServiceServer(srv, grpcserver.New(svc, log))
	// Health service chuẩn của gRPC: để docker healthcheck / load balancer hỏi "còn sống không".
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	reflection.Register(srv) // cho grpcurl gọi thử mà không cần file .proto

	// --- Reaper chạy nền ------------------------------------------------------------
	// [Go] sync.WaitGroup dùng để chờ một nhóm goroutine kết thúc. wg.Go(f) (Go 1.25+) chạy f
	// trong goroutine mới và tự Add/Done; wg.Wait() chặn cho tới khi mọi goroutine đã xong.
	//
	// [Go] Goroutine là "luồng nhẹ" do Go runtime quản lý; tạo hàng nghìn cái cũng rẻ.
	// `func() { ... }` là hàm ẩn danh (closure), dùng được biến bên ngoài như ctx, cfg, log.
	var wg sync.WaitGroup
	wg.Go(func() { reaper.New(svc, cfg.ReaperInterval, cfg.ReaperBatchSize, log).Run(ctx) })

	// srv.Serve chặn (block) cho tới khi server dừng, nên chạy nó trong goroutine riêng,
	// còn goroutine chính đi xuống dưới chờ tín hiệu tắt.
	//
	// [Go] Channel (chan) là "ống" để các goroutine gửi/nhận giá trị cho nhau.
	// make(chan error, 1) tạo channel có buffer 1 ô: goroutine gửi lỗi vào sẽ không bị kẹt
	// ngay cả khi không còn ai nhận (tránh rò rỉ goroutine).
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }() // [Go] `ch <- v` là gửi v vào channel
	log.Info("inventory listening", "addr", lis.Addr().String(), "hold_ttl", cfg.HoldTTL.String())

	// Chờ một trong hai sự kiện: nhận tín hiệu tắt (ctx bị hủy) HOẶC server tự chết vì lỗi.
	//
	// [Go] `select` chờ đồng thời nhiều thao tác channel, case nào sẵn sàng trước thì chạy.
	// ctx.Done() trả về channel bị đóng khi ctx bị hủy. `<-ch` là nhận từ channel.
	select {
	case <-ctx.Done():
		// Nhận SIGINT/SIGTERM -> xuống dưới để tắt êm.
	case err := <-serveErr:
		// Server lỗi: hủy ctx để reaper dừng, chờ reaper thoát rồi báo lỗi.
		stop()
		wg.Wait()
		return fmt.Errorf("grpc serve: %w", err)
	}

	// --- Graceful shutdown ------------------------------------------------------------
	log.Info("shutting down")
	// Báo health = NOT_SERVING trước, để healthcheck/LB ngừng gửi request mới tới instance này.
	hs.Shutdown()

	// GracefulStop: ngừng nhận kết nối mới và chờ các RPC đang chạy làm xong. Nó có thể chờ
	// mãi nếu có RPC treo, nên chạy trong goroutine và đặt giới hạn shutdownTimeout.
	//
	// [Go] chan struct{} là channel "chỉ để báo hiệu", không mang dữ liệu (struct{} chiếm 0 byte).
	// close(ch) đánh thức MỌI goroutine đang chờ `<-ch`.
	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
		// Dừng êm thành công.
	case <-time.After(shutdownTimeout):
		// [Go] time.After trả về channel sẽ nhận một giá trị sau khoảng thời gian cho trước.
		log.Warn("graceful stop timed out, forcing")
		srv.Stop() // cắt ngang mọi kết nối
	}
	// Đợi reaper thoát (nó đã thấy ctx bị hủy) trước khi các defer đóng pool/redis.
	wg.Wait()
	return nil
}

// healthcheck tự gọi gRPC Health.Check tới chính server đang chạy trong cùng container.
// Trả về exit code: 0 = khỏe, 1 = không khỏe (đúng quy ước của Docker HEALTHCHECK).
func healthcheck() int {
	addr := os.Getenv("GRPC_ADDR")
	if addr == "" {
		addr = ":50051"
	}
	// ":50051" -> host "", port "50051". Chỉ cần port vì luôn gọi vào localhost.
	// [Go] Dùng `_` để bỏ qua giá trị trả về không cần đến (Go báo lỗi biến khai báo mà không dùng).
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 1
	}
	// insecure: không dùng TLS, vì chỉ gọi nội bộ trong container.
	conn, err := grpc.NewClient("localhost:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 1
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	// [Go] Các method GetXxx() sinh từ protobuf an toàn khi resp == nil (trả giá trị zero),
	// nên gọi resp.GetStatus() không bị panic kể cả khi có lỗi.
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return 1
	}
	return 0
}
