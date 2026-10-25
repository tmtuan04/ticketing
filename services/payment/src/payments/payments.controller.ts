import {
  BadRequestException,
  Controller,
  Get,
  HttpCode,
  Inject,
  Param,
  ParseUUIDPipe,
  Post,
  Req,
  UnauthorizedException,
  type RawBodyRequest,
} from '@nestjs/common';
import type { Request } from 'express';
import { APP_CONFIG, type AppConfig } from '../config';
import type { PaymentIntent } from '../generated/prisma/client';
import { UUID_RE } from './events';
import { PaymentsService, type ProviderEvent } from './payments.service';
import { SIGNATURE_HEADER, verifyWebhook } from './webhook-signature';

@Controller('payments')
export class PaymentsController {
  constructor(
    private readonly payments: PaymentsService,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {}

  /**
   * Intent của một đơn. Intent được tạo bất đồng bộ (order -> outbox -> Redpanda -> payment) nên
   * ngay sau POST /orders có thể còn 404: client poll tới khi có rồi chuyển sang cổng thanh toán.
   * Không yêu cầu đăng nhập: order_id là UUID khó đoán, và đây là cổng giả lập cho môi trường dev.
   */
  @Get('orders/:orderId')
  async getByOrder(@Param('orderId', new ParseUUIDPipe()) orderId: string) {
    return toIntentView(await this.payments.getByOrder(orderId));
  }

  /**
   * Webhook cổng thanh toán gọi khi charge có kết quả. Xác minh HMAC trên body thô trước khi tin
   * bất cứ thứ gì trong body. Trả 2xx (kể cả khi trùng) để cổng ngừng gửi lại; lỗi 5xx thì cổng retry.
   */
  @Post('webhook')
  @HttpCode(200)
  async webhook(@Req() req: RawBodyRequest<Request>) {
    const raw = req.rawBody;
    const { secret, toleranceSec } = this.config.webhook;
    if (!raw || !verifyWebhook(secret, raw, req.header(SIGNATURE_HEADER), toleranceSec)) {
      throw new UnauthorizedException('invalid webhook signature');
    }
    const result = await this.payments.handleProviderEvent(parseProviderEvent(req.body));
    return { received: true, result };
  }
}

function parseProviderEvent(body: unknown): ProviderEvent {
  const e = body as Partial<ProviderEvent> | undefined;
  if (
    typeof e?.id !== 'string' ||
    !e.id ||
    (e.type !== 'charge.succeeded' && e.type !== 'charge.failed') ||
    typeof e.data?.payment_intent_id !== 'string' ||
    !UUID_RE.test(e.data.payment_intent_id)
  ) {
    throw new BadRequestException('malformed webhook event');
  }
  return e as ProviderEvent;
}

export function toIntentView(i: PaymentIntent) {
  return {
    id: i.id,
    orderId: i.orderId,
    userId: i.userId,
    // BigInt không JSON hóa được; tổng tiền một đơn nằm xa giới hạn 2^53
    amount: Number(i.amount),
    currency: i.currency,
    status: i.status,
    paymentDeadline: i.paymentDeadline.toISOString(),
    failureReason: i.failureReason || null,
    createdAt: i.createdAt.toISOString(),
    updatedAt: i.updatedAt.toISOString(),
  };
}
