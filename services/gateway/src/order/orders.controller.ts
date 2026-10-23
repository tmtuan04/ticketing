import { Body, Controller, ForbiddenException, Get, Param, ParseUUIDPipe, Post } from '@nestjs/common';
import type { Metadata } from '@grpc/grpc-js';
import { IsUUID } from 'class-validator';
import { CurrentUser, GrpcMeta, IdempotencyKey, RateLimit } from '../common/decorators';
import { toIso } from '../common/proto';
import type { AuthUser } from '../common/request';
import { OrderClient } from './order.client';
import { OrderStatus, type Order } from './order.pb';

export class CreateOrderDto {
  @IsUUID()
  holdId: string;
}

const uuid = new ParseUUIDPipe();

@Controller('orders')
export class OrdersController {
  constructor(private readonly orders: OrderClient) {}

  /**
   * Tạo đơn từ hold của user, trả về ngay ở trạng thái awaiting_payment (thanh toán chạy bất đồng
   * bộ sau đó). Giá do order tính từ catalog, client chỉ gửi holdId. Gửi lại cùng Idempotency-Key
   * trả về đơn cũ; hold đã có đơn với key khác => 409.
   */
  @RateLimit('order')
  @Post()
  async create(
    @Body() dto: CreateOrderDto,
    @IdempotencyKey() idempotencyKey: string,
    @CurrentUser() user: AuthUser,
    @GrpcMeta() md: Metadata,
  ) {
    const { order } = await this.orders.createOrder(
      { userId: user.id, holdId: dto.holdId, idempotencyKey },
      md,
    );
    return toOrderView(order!);
  }

  /** Xem đơn: chủ đơn hoặc admin. Client poll route này để biết kết quả thanh toán. */
  @Get(':orderId')
  async get(
    @Param('orderId', uuid) orderId: string,
    @CurrentUser() user: AuthUser,
    @GrpcMeta() md: Metadata,
  ) {
    const { order } = await this.orders.getOrder({ orderId }, md);
    if (order!.userId !== user.id && user.role !== 'admin') {
      throw new ForbiddenException('order belongs to another user');
    }
    return toOrderView(order!);
  }
}

const ORDER_STATUS: Partial<Record<OrderStatus, string>> = {
  [OrderStatus.ORDER_STATUS_PENDING]: 'pending',
  [OrderStatus.ORDER_STATUS_AWAITING_PAYMENT]: 'awaiting_payment',
  [OrderStatus.ORDER_STATUS_PAID]: 'paid',
  [OrderStatus.ORDER_STATUS_CONFIRMED]: 'confirmed',
  [OrderStatus.ORDER_STATUS_CANCELLED]: 'cancelled',
  [OrderStatus.ORDER_STATUS_REFUNDING]: 'refunding',
  [OrderStatus.ORDER_STATUS_REFUNDED]: 'refunded',
};

function toOrderView(o: Order) {
  return {
    id: o.id,
    userId: o.userId,
    eventId: o.eventId,
    holdId: o.holdId,
    seatIds: o.seatIds,
    // int64 tới dạng string; tổng giá vài ghế nằm xa giới hạn 2^53 nên đổi sang number an toàn
    amount: Number(o.amount),
    currency: o.currency,
    status: ORDER_STATUS[o.status] ?? 'unknown',
    paymentDeadline: toIso(o.paymentDeadline),
    failureReason: o.failureReason || null,
    createdAt: toIso(o.createdAt),
    updatedAt: toIso(o.updatedAt),
  };
}
