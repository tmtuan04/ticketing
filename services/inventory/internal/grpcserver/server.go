// Package grpcserver nối InventoryService (proto) với tầng service: parse/validate id,
// chuyển kiểu, và map lỗi nghiệp vụ sang gRPC status.
//
// Vai trò giống Controller trong NestJS: chỉ lo "vỏ" giao tiếp (kiểu proto, string UUID,
// gRPC status code), không chứa nghiệp vụ. Nghiệp vụ nằm ở package service.
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

// Server implement interface inventoryv1.InventoryServiceServer (sinh từ .proto).
//
// [Go] Dòng đầu là "embedding": nhúng struct UnimplementedInventoryServiceServer vào Server
// (không đặt tên field). Server được "thừa hưởng" mọi method của nó; các method đó trả lỗi
// Unimplemented. Nhờ vậy khi .proto thêm RPC mới mà ta chưa viết, code vẫn compile được.
// Go không có kế thừa (class/extends); embedding là cách "tái sử dụng" thay thế.
type Server struct {
	inventoryv1.UnimplementedInventoryServiceServer
	svc *service.Service
	log *slog.Logger
}

// New là constructor. [Go] Go không có constructor đặc biệt; quy ước là hàm New/NewXxx trả
// về con trỏ tới struct đã khởi tạo.
func New(svc *service.Service, log *slog.Logger) *Server {
	return &Server{svc: svc, log: log}
}

// Mọi handler bên dưới đều theo cùng một khuôn:
//  1. Parse string -> uuid.UUID (sai định dạng -> InvalidArgument ngay, không gọi service).
//  2. Gọi service.
//  3. Lỗi -> toStatus (map sang gRPC code); thành công -> chuyển kết quả sang kiểu proto.
//
// [Go] `func (s *Server) InitEventInventory(...)` là METHOD: phần (s *Server) gọi là
// "receiver", tương đương `this` trong JS/TS. Receiver là con trỏ (*Server) để không copy
// struct mỗi lần gọi. Go tự hiểu Server có method này => tự thỏa interface, không cần
// viết "implements" (interface trong Go được thỏa mãn NGẦM).

// InitEventInventory tạo dữ liệu ghế cho một event (gọi từ catalog khi publish event).
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

// HoldSeats giữ ghế cho user. Ghế bị người khác chiếm KHÔNG phải lỗi: response trả về
// nhánh `unavailable` của oneof (xem proto/ticketing/inventory/v1/inventory.proto).
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
	// res.Hold == nil nghĩa là không giữ được vì có ghế đã bị chiếm.
	if res.Hold == nil {
		// [Go] `oneof` trong proto sinh ra thành một interface field (Result) với mỗi nhánh là
		// một struct wrapper riêng: HoldSeatsResponse_Unavailable / HoldSeatsResponse_Hold.
		return &inventoryv1.HoldSeatsResponse{Result: &inventoryv1.HoldSeatsResponse_Unavailable{
			Unavailable: &inventoryv1.SeatsUnavailable{SeatIds: idStrings(res.Unavailable)},
		}}, nil
	}
	// [Go] *res.Hold: res.Hold là con trỏ (*repo.Hold), dấu * lấy giá trị struct mà nó trỏ tới.
	return &inventoryv1.HoldSeatsResponse{Result: &inventoryv1.HoldSeatsResponse_Hold{Hold: holdToPB(*res.Hold)}}, nil
}

// ReleaseHold: user chủ động nhả ghế đang giữ. Cần user_id để kiểm tra quyền sở hữu hold.
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

// ConfirmHold chốt bán ghế (gọi sau khi thanh toán thành công). Không cần user_id vì do hệ
// thống nội bộ gọi, không phải người dùng.
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

// ExtendHold kéo dài hạn hold (Order gọi khi tạo đơn). Cần user_id như ReleaseHold vì Order gọi
// thay mặt user, không được gia hạn hold của người khác.
func (s *Server) ExtendHold(ctx context.Context, req *inventoryv1.ExtendHoldRequest) (*inventoryv1.ExtendHoldResponse, error) {
	holdID, err := parseID("hold_id", req.GetHoldId())
	if err != nil {
		return nil, err
	}
	userID, err := parseID("user_id", req.GetUserId())
	if err != nil {
		return nil, err
	}
	// Duration của proto là {seconds, nanos}; CheckValid bắt cả nil và giá trị ngoài khoảng.
	if err := req.GetTtl().CheckValid(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "ttl must be a valid duration")
	}
	h, err := s.svc.ExtendHold(ctx, holdID, userID, req.GetTtl().AsDuration())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &inventoryv1.ExtendHoldResponse{Hold: holdToPB(h)}, nil
}

// GetHold đọc một hold theo id.
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

// ListSeatAvailability trả trạng thái mọi ghế của event (để vẽ sơ đồ ghế).
func (s *Server) ListSeatAvailability(ctx context.Context, req *inventoryv1.ListSeatAvailabilityRequest) (*inventoryv1.ListSeatAvailabilityResponse, error) {
	eventID, err := parseID("event_id", req.GetEventId())
	if err != nil {
		return nil, err
	}
	rows, err := s.svc.ListSeatAvailability(ctx, eventID)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	// [Go] make([]T, n) tạo slice có sẵn n phần tử (giá trị zero) -> gán theo chỉ số, không
	// cần append, tránh cấp phát lại bộ nhớ nhiều lần.
	seats := make([]*inventoryv1.SeatAvailability, len(rows))
	// [Go] `for i, r := range rows` duyệt slice: i là chỉ số, r là bản copy của phần tử.
	for i, r := range rows {
		seats[i] = &inventoryv1.SeatAvailability{SeatId: r.SeatID.String(), Status: seatStatusToPB(r.Status)}
	}
	return &inventoryv1.ListSeatAvailabilityResponse{Seats: seats}, nil
}

// toStatus map lỗi nghiệp vụ sang gRPC code; lỗi không lường trước thì log và ẩn chi tiết.
//
// Gateway NestJS lại map tiếp gRPC code -> HTTP status (InvalidArgument -> 400,
// NotFound -> 404, AlreadyExists/FailedPrecondition -> 409...).
func (s *Server) toStatus(ctx context.Context, err error) error {
	var code codes.Code
	switch {
	// [Go] errors.Is(err, target) kiểm tra err có phải target HOẶC có bọc (wrap bằng %w)
	// target ở bên trong không. Vì service trả lỗi kiểu fmt.Errorf("%w: ...", ErrInvalidArgument),
	// so sánh `err == ErrInvalidArgument` sẽ sai, phải dùng errors.Is.
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
		// Client đã hủy request (vd gateway timeout / người dùng đóng tab).
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	default:
		// Lỗi lạ (DB sập, bug...): log đầy đủ ở server nhưng không lộ chi tiết nội bộ ra client.
		s.log.ErrorContext(ctx, "internal error", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
	// Lỗi nghiệp vụ: message an toàn để trả cho client.
	return status.Error(code, err.Error())
}

// parseID chuyển chuỗi UUID từ request sang uuid.UUID; sai thì trả lỗi InvalidArgument kèm
// tên field để client biết sửa chỗ nào.
func parseID(field, s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		// uuid.Nil = 00000000-0000-0000-0000-000000000000, giá trị "rỗng" của UUID.
		return uuid.Nil, status.Errorf(codes.InvalidArgument, "%s must be a UUID", field)
	}
	return id, nil
}

// parseIDs giống parseID nhưng cho một danh sách; báo cả vị trí phần tử sai.
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

// idStrings chuyển []uuid.UUID -> []string để đưa vào message proto (proto dùng string cho UUID).
func idStrings(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

// holdToPB chuyển model DB (repo.Hold) sang message proto (inventoryv1.Hold).
// "PB" = protobuf. time.Time phải bọc bằng timestamppb để thành google.protobuf.Timestamp.
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

// holdStatusToPB: trong DB status lưu dạng text ("active"...), trong proto là enum.
//
// [Go] `switch s { case X: ... }` so sánh s với từng case; không cần `break`.
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
	// Giá trị lạ (không nên xảy ra vì DB có CHECK constraint).
	return inventoryv1.HoldStatus_HOLD_STATUS_UNSPECIFIED
}

// seatStatusToPB: tương tự holdStatusToPB, cho trạng thái ghế.
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
