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
//
// "Unary" = RPC kiểu một request -> một response (khác với streaming). Interceptor là
// middleware của gRPC: nhận request, gọi `handler` (method thật như CreateOrder), rồi xử lý
// kết quả. Tương tự Interceptor trong NestJS.
//
// [Go] Đây là "hàm trả về hàm": UnaryInterceptor(log) nhận logger rồi trả về một closure có
// đúng chữ ký grpc.UnaryServerInterceptor. Closure "nhớ" biến log để dùng mỗi lần được gọi.
func UnaryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	// [Go] `any` là alias của interface{} = "kiểu bất kỳ" (giống unknown trong TS).
	//
	// [Go] (resp any, err error) là "named return values": hai biến kết quả được đặt tên.
	// Nhờ có tên, khối defer bên dưới có thể GHI ĐÈ err sau khi handler đã return
	// (đây là cách duy nhất để defer thay đổi giá trị trả về của hàm).
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		// log.With tạo logger con luôn kèm sẵn field "method" (vd "/ticketing.order.v1.OrderService/CreateOrder").
		reqLog := log.With("method", info.FullMethod)
		// Metadata gRPC ~ HTTP headers. Gateway (NestJS) gửi x-request-id để nối log hai bên.
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if ids := md.Get("x-request-id"); len(ids) > 0 {
				reqLog = reqLog.With("request_id", ids[0])
			}
		}

		// Chạy SAU khi handler return (hoặc panic): bắt panic và ghi log kết quả.
		defer func() {
			// [Go] panic ~ exception không được bắt; nếu không recover, cả process sẽ chết.
			// recover() chỉ có tác dụng bên trong hàm defer: trả về giá trị đã panic (khác nil)
			// và chặn panic lan tiếp. Ở đây biến panic thành lỗi Internal để server sống tiếp.
			if p := recover(); p != nil {
				reqLog.ErrorContext(ctx, "panic", "panic", p, "stack", string(debug.Stack()))
				// Gán vào named return err -> client nhận Internal thay vì mất kết nối.
				err = status.Error(codes.Internal, "internal error")
			}
			// status.Code(nil) == codes.OK, nên dòng này đúng cả khi thành công.
			code := status.Code(err)
			level := slog.LevelInfo
			// [Go] `switch {` không có biểu thức = chuỗi if/else-if gọn hơn; case đầu tiên đúng
			// sẽ chạy và tự dừng (Go KHÔNG fall through như C/JS, không cần break).
			switch {
			case code == codes.Internal || code == codes.Unknown:
				level = slog.LevelError
			case strings.HasPrefix(info.FullMethod, "/grpc.health."):
				level = slog.LevelDebug // docker healthcheck gọi mỗi 5s
			}
			reqLog.Log(ctx, level, "rpc", "code", code.String(), "duration_ms", time.Since(start).Milliseconds())
		}()

		// Gọi method thật. Kết quả gán vào named returns (resp, err) rồi defer ở trên mới chạy.
		return handler(ctx, req)
	}
}

// PropagateRequestID là interceptor phía CLIENT: chép x-request-id của request đang xử lý sang
// lời gọi xuống inventory/catalog, để log của cả chuỗi gateway -> order -> inventory nối được với
// nhau bằng một id.
//
// [Go] metadata.FromIncomingContext đọc metadata request ĐẾN; AppendToOutgoingContext gắn
// metadata vào request ĐI. Hai chiều tách riêng nên phải tự chép.
func PropagateRequestID() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if ids := md.Get("x-request-id"); len(ids) > 0 {
				ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", ids[0])
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
