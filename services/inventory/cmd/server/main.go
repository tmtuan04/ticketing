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
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	inventoryv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/inventory/v1"
	"github.com/tmtuan04/ticketing/inventory/internal/config"
	"github.com/tmtuan04/ticketing/inventory/internal/grpcserver"
	"github.com/tmtuan04/ticketing/inventory/internal/reaper"
	"github.com/tmtuan04/ticketing/inventory/internal/redisx"
	"github.com/tmtuan04/ticketing/inventory/internal/service"
)

const shutdownTimeout = 10 * time.Second

func main() {
	// Image distroless không có shell/curl: docker healthcheck gọi lại chính binary này.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	if err := run(); err != nil {
		slog.Error("inventory stopped", "err", err)
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

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		// Redis chỉ là lớp lọc nhanh: thiếu Redis thì chậm hơn nhưng vẫn đúng.
		log.Warn("redis unavailable, holds will go straight to postgres", "err", err)
	}

	svc := service.New(pool, redisx.New(rdb), service.Config{
		HoldTTL:         cfg.HoldTTL,
		MaxSeatsPerHold: cfg.MaxSeatsPerHold,
	}, log)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(grpcserver.UnaryInterceptor(log)))
	inventoryv1.RegisterInventoryServiceServer(srv, grpcserver.New(svc, log))
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	reflection.Register(srv) // cho grpcurl gọi thử mà không cần file .proto

	var wg sync.WaitGroup
	wg.Go(func() { reaper.New(svc, cfg.ReaperInterval, cfg.ReaperBatchSize, log).Run(ctx) })

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	log.Info("inventory listening", "addr", lis.Addr().String(), "hold_ttl", cfg.HoldTTL.String())

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
	wg.Wait()
	return nil
}

func healthcheck() int {
	addr := os.Getenv("GRPC_ADDR")
	if addr == "" {
		addr = ":50051"
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
