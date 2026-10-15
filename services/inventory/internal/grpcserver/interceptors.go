package grpcserver

import (
	"context"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// UnaryInterceptor bắt panic thành codes.Internal và log mỗi request kèm x-request-id
// do gateway truyền qua metadata.
func UnaryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		reqLog := log.With("method", info.FullMethod)
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if ids := md.Get("x-request-id"); len(ids) > 0 {
				reqLog = reqLog.With("request_id", ids[0])
			}
		}

		defer func() {
			if p := recover(); p != nil {
				reqLog.ErrorContext(ctx, "panic", "panic", p, "stack", string(debug.Stack()))
				err = status.Error(codes.Internal, "internal error")
			}
			code := status.Code(err)
			level := slog.LevelInfo
			switch {
			case code == codes.Internal || code == codes.Unknown:
				level = slog.LevelError
			case strings.HasPrefix(info.FullMethod, "/grpc.health."):
				level = slog.LevelDebug // docker healthcheck gọi mỗi 5s
			}
			reqLog.Log(ctx, level, "rpc", "code", code.String(), "duration_ms", time.Since(start).Milliseconds())
		}()

		return handler(ctx, req)
	}
}
