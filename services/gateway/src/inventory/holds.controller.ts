import {
  Body,
  ConflictException,
  Controller,
  Delete,
  ForbiddenException,
  Get,
  Param,
  ParseUUIDPipe,
  Post,
} from '@nestjs/common';
import type { Metadata } from '@grpc/grpc-js';
import { ArrayMaxSize, ArrayNotEmpty, ArrayUnique, IsArray, IsUUID } from 'class-validator';
import { CurrentUser, GrpcMeta, IdempotencyKey, Public, RateLimit } from '../common/decorators';
import type { AuthUser } from '../common/request';
import { InventoryClient } from './inventory.client';
import { HoldStatus, SeatStatus, type Hold, type Timestamp } from './inventory.pb';

export class HoldSeatsDto {
  // Giới hạn số ghế mỗi hold (MAX_SEATS_PER_HOLD) do inventory kiểm tra; ở đây chỉ chặn payload quá lớn.
  @IsArray()
  @ArrayNotEmpty()
  @ArrayMaxSize(50)
  @ArrayUnique()
  @IsUUID('all', { each: true })
  seatIds: string[];
}

const uuid = new ParseUUIDPipe();

@Controller()
export class HoldsController {
  constructor(private readonly inventory: InventoryClient) {}

  @Public()
  @Get('events/:eventId/seats')
  async listSeats(@Param('eventId', uuid) eventId: string, @GrpcMeta() md: Metadata) {
    const res = await this.inventory.listSeatAvailability({ eventId }, md);
    return {
      eventId,
      seats: res.seats.map((s) => ({ seatId: s.seatId, status: SEAT_STATUS[s.status] ?? 'unknown' })),
    };
  }

  /** Giữ ghế. Gửi lại cùng Idempotency-Key trả về hold cũ thay vì tạo hold mới. */
  @RateLimit('hold')
  @Post('events/:eventId/holds')
  async hold(
    @Param('eventId', uuid) eventId: string,
    @Body() dto: HoldSeatsDto,
    @IdempotencyKey() idempotencyKey: string,
    @CurrentUser() user: AuthUser,
    @GrpcMeta() md: Metadata,
  ) {
    const res = await this.inventory.holdSeats(
      { eventId, userId: user.id, seatIds: dto.seatIds, idempotencyKey },
      md,
    );
    if (res.hold) return toHoldView(res.hold);
    throw new ConflictException({
      statusCode: 409,
      error: 'Conflict',
      message: 'some seats are not available',
      unavailableSeatIds: res.unavailable?.seatIds ?? [],
    });
  }

  @Get('holds/:holdId')
  async getHold(
    @Param('holdId', uuid) holdId: string,
    @CurrentUser() user: AuthUser,
    @GrpcMeta() md: Metadata,
  ) {
    const { hold } = await this.inventory.getHold({ holdId }, md);
    if (hold!.userId !== user.id && user.role !== 'admin') {
      throw new ForbiddenException('hold belongs to another user');
    }
    return toHoldView(hold!);
  }

  /** Nhả hold. Idempotent: hold đã nhả/hết hạn trả lại trạng thái hiện tại. */
  @Delete('holds/:holdId')
  async releaseHold(
    @Param('holdId', uuid) holdId: string,
    @CurrentUser() user: AuthUser,
    @GrpcMeta() md: Metadata,
  ) {
    const { hold } = await this.inventory.releaseHold({ holdId, userId: user.id }, md);
    return toHoldView(hold!);
  }
}

const HOLD_STATUS: Partial<Record<HoldStatus, string>> = {
  [HoldStatus.HOLD_STATUS_ACTIVE]: 'active',
  [HoldStatus.HOLD_STATUS_RELEASED]: 'released',
  [HoldStatus.HOLD_STATUS_CONFIRMED]: 'confirmed',
  [HoldStatus.HOLD_STATUS_EXPIRED]: 'expired',
};

const SEAT_STATUS: Partial<Record<SeatStatus, string>> = {
  [SeatStatus.SEAT_STATUS_AVAILABLE]: 'available',
  [SeatStatus.SEAT_STATUS_HELD]: 'held',
  [SeatStatus.SEAT_STATUS_SOLD]: 'sold',
};

function toHoldView(h: Hold) {
  return {
    id: h.id,
    eventId: h.eventId,
    userId: h.userId,
    seatIds: h.seatIds,
    status: HOLD_STATUS[h.status] ?? 'unknown',
    expiresAt: toIso(h.expiresAt),
    createdAt: toIso(h.createdAt),
  };
}

/** Timestamp từ proto-loader có seconds dạng string (longs: String). */
function toIso(ts: Timestamp | undefined): string | null {
  if (!ts) return null;
  return new Date(Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6)).toISOString();
}
