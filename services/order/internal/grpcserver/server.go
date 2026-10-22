// Package grpcserver là tầng gRPC của order: parse request, gọi service, map lỗi sang status
// code và chuyển kết quả sang message proto. Cùng khuôn như inventory/internal/grpcserver.
package grpcserver

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/order/v1"
	"github.com/tmtuan04/ticketing/order/internal/repo"
	"github.com/tmtuan04/ticketing/order/internal/service"
)

type Server struct {
	orderv1.UnimplementedOrderServiceServer
	svc *service.Service
	log *slog.Logger
}

func New(svc *service.Service, log *slog.Logger) *Server {
	return &Server{svc: svc, log: log}
}

// CreateOrder tạo đơn từ hold. Gateway truyền user_id lấy từ JWT, không phải từ body của client.
func (s *Server) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	userID, err := parseID("user_id", req.GetUserId())
	if err != nil {
		return nil, err
	}
	holdID, err := parseID("hold_id", req.GetHoldId())
	if err != nil {
		return nil, err
	}
	o, err := s.svc.CreateOrder(ctx, service.CreateRequest{UserID: userID, HoldID: holdID, IdempotencyKey: req.GetIdempotencyKey()})
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &orderv1.CreateOrderResponse{Order: orderToPB(o)}, nil
}

// GetOrder đọc đơn. Kiểm tra quyền (đơn của ai) do gateway làm, giống GetHold.
func (s *Server) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	id, err := parseID("order_id", req.GetOrderId())
	if err != nil {
		return nil, err
	}
	o, err := s.svc.GetOrder(ctx, id)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &orderv1.GetOrderResponse{Order: orderToPB(o)}, nil
}

// toStatus map lỗi nghiệp vụ sang gRPC code; lỗi không lường trước thì log và ẩn chi tiết.
// Gateway map tiếp sang HTTP (NotFound -> 404, AlreadyExists/FailedPrecondition -> 409,
// Unavailable -> 503...).
func (s *Server) toStatus(ctx context.Context, err error) error {
	var code codes.Code
	switch {
	case errors.Is(err, service.ErrInvalidArgument), errors.Is(err, service.ErrIdempotencyMismatch):
		code = codes.InvalidArgument
	case errors.Is(err, service.ErrNotFound), errors.Is(err, service.ErrHoldNotFound):
		code = codes.NotFound
	case errors.Is(err, service.ErrPermissionDenied):
		code = codes.PermissionDenied
	case errors.Is(err, service.ErrOrderExists):
		code = codes.AlreadyExists
	case errors.Is(err, service.ErrHoldNotActive):
		code = codes.FailedPrecondition
	case errors.Is(err, service.ErrUnavailable):
		// Chi tiết lỗi của service phía sau chỉ ghi log, không trả cho client.
		s.log.WarnContext(ctx, "upstream error", "err", err)
		return status.Error(codes.Unavailable, service.ErrUnavailable.Error())
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	default:
		s.log.ErrorContext(ctx, "internal error", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
	return status.Error(code, err.Error())
}

func parseID(field, s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, status.Errorf(codes.InvalidArgument, "%s must be a UUID", field)
	}
	return id, nil
}

func orderToPB(o repo.Order) *orderv1.Order {
	seats := make([]string, len(o.SeatIds))
	for i, id := range o.SeatIds {
		seats[i] = id.String()
	}
	return &orderv1.Order{
		Id:              o.ID.String(),
		UserId:          o.UserID.String(),
		EventId:         o.EventID.String(),
		HoldId:          o.HoldID.String(),
		SeatIds:         seats,
		Amount:          o.Amount,
		Currency:        o.Currency,
		Status:          statusToPB(o.Status),
		PaymentDeadline: timestamppb.New(o.PaymentDeadline),
		FailureReason:   o.FailureReason,
		CreatedAt:       timestamppb.New(o.CreatedAt),
		UpdatedAt:       timestamppb.New(o.UpdatedAt),
	}
}

// statusToPB: DB lưu text ("awaiting_payment"), proto dùng enum.
func statusToPB(s string) orderv1.OrderStatus {
	switch s {
	case service.StatusPending:
		return orderv1.OrderStatus_ORDER_STATUS_PENDING
	case service.StatusAwaitingPayment:
		return orderv1.OrderStatus_ORDER_STATUS_AWAITING_PAYMENT
	case service.StatusPaid:
		return orderv1.OrderStatus_ORDER_STATUS_PAID
	case service.StatusConfirmed:
		return orderv1.OrderStatus_ORDER_STATUS_CONFIRMED
	case service.StatusCancelled:
		return orderv1.OrderStatus_ORDER_STATUS_CANCELLED
	case service.StatusRefunding:
		return orderv1.OrderStatus_ORDER_STATUS_REFUNDING
	case service.StatusRefunded:
		return orderv1.OrderStatus_ORDER_STATUS_REFUNDED
	default:
		return orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
}
