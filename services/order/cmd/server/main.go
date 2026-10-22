// Điểm khởi động của order service: đọc config, mở Postgres, kết nối gRPC tới inventory và
// catalog, dựng gRPC server, chạy outbox relay nền rồi chờ tín hiệu tắt. Cấu trúc giống hệt inventory/cmd/server
// (xem các ghi chú [Go] bên đó); ở đây chỉ chú thích phần khác.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	catalogv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/catalog/v1"
	inventoryv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/inventory/v1"
	orderv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/order/v1"
	"github.com/tmtuan04/ticketing/order/internal/config"
	"github.com/tmtuan04/ticketing/order/internal/grpcserver"
	"github.com/tmtuan04/ticketing/order/internal/outbox"
	"github.com/tmtuan04/ticketing/order/internal/service"
)

const shutdownTimeout = 10 * time.Second

func main() {
	// Image distroless không có curl: docker healthcheck gọi "/order healthcheck".
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	if err := run(); err != nil {
		slog.Error("order stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("postgres ping: %w", err)
	}

	// Kết nối gRPC tới service khác. grpc.NewClient không kết nối ngay (lazy): lần gọi đầu mới
	// dial, và tự kết nối lại khi mất. Vì vậy order vẫn khởi động được khi inventory chưa lên.
	inventoryConn, err := dial(cfg.InventoryGRPCURL)
	if err != nil {
		return fmt.Errorf("inventory client: %w", err)
	}
	defer inventoryConn.Close()
	catalogConn, err := dial(cfg.CatalogGRPCURL)
	if err != nil {
		return fmt.Errorf("catalog client: %w", err)
	}
	defer catalogConn.Close()

	svc := service.New(pool,
		inventoryv1.NewInventoryServiceClient(inventoryConn),
		catalogv1.NewCatalogServiceClient(catalogConn),
		service.Config{PaymentTimeout: cfg.PaymentTimeout, HoldBuffer: cfg.HoldBuffer, CallTimeout: cfg.CallTimeout},
		log)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(grpcserver.UnaryInterceptor(log)))
	orderv1.RegisterOrderServiceServer(srv, grpcserver.New(svc, log))
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	reflection.Register(srv)

	// Kafka client cho relay. Giống grpc.NewClient: không kết nối ngay, Redpanda chưa lên thì relay
	// chỉ backoff rồi thử lại, order vẫn nhận CreateOrder (message nằm chờ trong outbox).
	producer, err := outbox.NewProducer(cfg.KafkaBrokers, cfg.DeliveryTimeout)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	defer producer.Close()

	var wg sync.WaitGroup
	wg.Go(func() { outbox.New(pool, producer, cfg.OutboxInterval, cfg.OutboxBatchSize, log).Run(ctx) })

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	log.Info("order listening", "addr", lis.Addr().String(),
		"payment_timeout", cfg.PaymentTimeout.String(), "hold_buffer", cfg.HoldBuffer.String())

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		stop()
		wg.Wait()
		return fmt.Errorf("grpc serve: %w", err)
	}

	log.Info("shutting down")
	hs.Shutdown()
	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(shutdownTimeout):
		log.Warn("graceful stop timed out, forcing")
		srv.Stop()
	}
	// Chờ relay dừng (lô đang dở bị ROLLBACK, lần chạy sau publish lại) trước khi các defer đóng
	// producer và pool.
	wg.Wait()
	return nil
}

// dial tạo client connection nội bộ (không TLS) có interceptor chép x-request-id.
func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(grpcserver.PropagateRequestID()),
	)
}

// healthcheck gọi gRPC Health.Check tới chính server trong container (exit 0 = khỏe).
func healthcheck() int {
	addr := os.Getenv("GRPC_ADDR")
	if addr == "" {
		addr = ":50053"
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 1
	}
	conn, err := grpc.NewClient("localhost:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 1
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return 1
	}
	return 0
}
