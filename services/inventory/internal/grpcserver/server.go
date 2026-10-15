// Package grpcserver nối InventoryService (proto) với tầng service: parse/validate id,
// chuyển kiểu, và map lỗi nghiệp vụ sang gRPC status.
package grpcserver

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	inventoryv1 "github.com/tmtuan04/ticketing/gen/go/ticketing/inventory/v1"
	"github.com/tmtuan04/ticketing/inventory/internal/repo"
	"github.com/tmtuan04/ticketing/inventory/internal/service"
)

type Server struct {
	inventoryv1.UnimplementedInventoryServiceServer
	svc *service.Service
	log *slog.Logger
}

func New(svc *service.Service, log *slog.Logger) *Server {
	return &Server{svc: svc, log: log}
}

func (s *Server) InitEventInventory(ctx context.Context, req *inventoryv1.InitEventInventoryRequest) (*inventoryv1.InitEventInventoryResponse, error) {
	eventID, err := parseID("event_id", req.GetEventId())
	if err != nil {
		return nil, err
	}
	seatIDs, err := parseIDs("seat_ids", req.GetSeatIds())
	if err != nil {
		return nil, err
	}
	n, err := s.svc.InitEventInventory(ctx, eventID, seatIDs)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &inventoryv1.InitEventInventoryResponse{SeatCount: int32(n)}, nil
}

func (s *Server) HoldSeats(ctx context.Context, req *inventoryv1.HoldSeatsRequest) (*inventoryv1.HoldSeatsResponse, error) {
	eventID, err := parseID("event_id", req.GetEventId())
	if err != nil {
		return nil, err
	}
	userID, err := parseID("user_id", req.GetUserId())
	if err != nil {
		return nil, err
	}
	seatIDs, err := parseIDs("seat_ids", req.GetSeatIds())
	if err != nil {
		return nil, err
	}
	res, err := s.svc.HoldSeats(ctx, service.HoldRequest{
		EventID:        eventID,
		UserID:         userID,
		SeatIDs:        seatIDs,
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	if res.Hold == nil {
		return &inventoryv1.HoldSeatsResponse{Result: &inventoryv1.HoldSeatsResponse_Unavailable{
			Unavailable: &inventoryv1.SeatsUnavailable{SeatIds: idStrings(res.Unavailable)},
		}}, nil
	}
	return &inventoryv1.HoldSeatsResponse{Result: &inventoryv1.HoldSeatsResponse_Hold{Hold: holdToPB(*res.Hold)}}, nil
}

func (s *Server) ReleaseHold(ctx context.Context, req *inventoryv1.ReleaseHoldRequest) (*inventoryv1.ReleaseHoldResponse, error) {
	holdID, err := parseID("hold_id", req.GetHoldId())
	if err != nil {
		return nil, err
	}
	userID, err := parseID("user_id", req.GetUserId())
	if err != nil {
		return nil, err
	}
	h, err := s.svc.ReleaseHold(ctx, holdID, userID)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &inventoryv1.ReleaseHoldResponse{Hold: holdToPB(h)}, nil
}

func (s *Server) ConfirmHold(ctx context.Context, req *inventoryv1.ConfirmHoldRequest) (*inventoryv1.ConfirmHoldResponse, error) {
	holdID, err := parseID("hold_id", req.GetHoldId())
	if err != nil {
		return nil, err
	}
	h, err := s.svc.ConfirmHold(ctx, holdID)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &inventoryv1.ConfirmHoldResponse{Hold: holdToPB(h)}, nil
}

func (s *Server) GetHold(ctx context.Context, req *inventoryv1.GetHoldRequest) (*inventoryv1.GetHoldResponse, error) {
	holdID, err := parseID("hold_id", req.GetHoldId())
	if err != nil {
		return nil, err
	}
	h, err := s.svc.GetHold(ctx, holdID)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &inventoryv1.GetHoldResponse{Hold: holdToPB(h)}, nil
}

func (s *Server) ListSeatAvailability(ctx context.Context, req *inventoryv1.ListSeatAvailabilityRequest) (*inventoryv1.ListSeatAvailabilityResponse, error) {
	eventID, err := parseID("event_id", req.GetEventId())
	if err != nil {
		return nil, err
	}
	rows, err := s.svc.ListSeatAvailability(ctx, eventID)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	seats := make([]*inventoryv1.SeatAvailability, len(rows))
	for i, r := range rows {
		seats[i] = &inventoryv1.SeatAvailability{SeatId: r.SeatID.String(), Status: seatStatusToPB(r.Status)}
	}
	return &inventoryv1.ListSeatAvailabilityResponse{Seats: seats}, nil
}

// toStatus map lỗi nghiệp vụ sang gRPC code; lỗi không lường trước thì log và ẩn chi tiết.
func (s *Server) toStatus(ctx context.Context, err error) error {
	var code codes.Code
	switch {
	case errors.Is(err, service.ErrInvalidArgument), errors.Is(err, service.ErrIdempotencyMismatch):
		code = codes.InvalidArgument
	case errors.Is(err, service.ErrNotFound):
		code = codes.NotFound
	case errors.Is(err, service.ErrPermissionDenied):
		code = codes.PermissionDenied
	case errors.Is(err, service.ErrActiveHoldExists):
		code = codes.AlreadyExists
	case errors.Is(err, service.ErrHoldNotActive), errors.Is(err, service.ErrHoldExpired):
		code = codes.FailedPrecondition
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

func parseIDs(field string, ss []string) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(ss))
	for i, s := range ss {
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s[%d] must be a UUID", field, i)
		}
		ids[i] = id
	}
	return ids, nil
}

func idStrings(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

func holdToPB(h repo.Hold) *inventoryv1.Hold {
	return &inventoryv1.Hold{
		Id:        h.ID.String(),
		EventId:   h.EventID.String(),
		UserId:    h.UserID.String(),
		SeatIds:   idStrings(h.SeatIds),
		Status:    holdStatusToPB(h.Status),
		ExpiresAt: timestamppb.New(h.ExpiresAt),
		CreatedAt: timestamppb.New(h.CreatedAt),
	}
}

func holdStatusToPB(s string) inventoryv1.HoldStatus {
	switch s {
	case service.HoldActive:
		return inventoryv1.HoldStatus_HOLD_STATUS_ACTIVE
	case service.HoldReleased:
		return inventoryv1.HoldStatus_HOLD_STATUS_RELEASED
	case service.HoldConfirmed:
		return inventoryv1.HoldStatus_HOLD_STATUS_CONFIRMED
	case service.HoldExpired:
		return inventoryv1.HoldStatus_HOLD_STATUS_EXPIRED
	}
	return inventoryv1.HoldStatus_HOLD_STATUS_UNSPECIFIED
}

func seatStatusToPB(s string) inventoryv1.SeatStatus {
	switch s {
	case service.SeatAvailable:
		return inventoryv1.SeatStatus_SEAT_STATUS_AVAILABLE
	case service.SeatHeld:
		return inventoryv1.SeatStatus_SEAT_STATUS_HELD
	case service.SeatSold:
		return inventoryv1.SeatStatus_SEAT_STATUS_SOLD
	}
	return inventoryv1.SeatStatus_SEAT_STATUS_UNSPECIFIED
}
