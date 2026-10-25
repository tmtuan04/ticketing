import { Body, Controller, HttpCode, Param, ParseUUIDPipe, Post } from '@nestjs/common';
import { IsIn, IsInt, IsOptional, Min } from 'class-validator';
import { IdempotencyKey } from '../common/decorators';
import type { ChargeOutcome, MockCharge } from '../generated/prisma/client';
import { MockGatewayService } from './mock-gateway.service';

export class PayDto {
  /** timeout: cổng nhận tiền nhưng không bao giờ báo kết quả (để order tự hủy khi quá hạn) */
  @IsIn(['success', 'fail', 'timeout'])
  outcome: ChargeOutcome;

  /** chờ bao lâu rồi mới gọi webhook, giới hạn bởi MOCK_GATEWAY_MAX_DELAY */
  @IsOptional()
  @IsInt()
  @Min(0)
  delayMs?: number;
}

@Controller('mock-gateway')
export class MockGatewayController {
  constructor(private readonly gateway: MockGatewayService) {}

  /**
   * "Trả tiền" cho một intent. Trả 202 ngay: kết quả đến payment service qua webhook, client poll
   * GET /payments/orders/:orderId (hoặc GET /orders/:id ở gateway) để biết kết quả.
   */
  @Post('pay/:intentId')
  @HttpCode(202)
  async pay(
    @Param('intentId', new ParseUUIDPipe()) intentId: string,
    @Body() dto: PayDto,
    @IdempotencyKey() idempotencyKey: string,
  ) {
    const charge = await this.gateway.createCharge({
      intentId,
      idempotencyKey,
      outcome: dto.outcome,
      delayMs: dto.delayMs ?? 0,
    });
    return toChargeView(charge);
  }
}

function toChargeView(c: MockCharge) {
  return {
    id: c.id,
    intentId: c.intentId,
    outcome: c.outcome,
    amount: Number(c.amount),
    currency: c.currency,
    webhookDelivered: c.deliveredAt !== null,
    createdAt: c.createdAt.toISOString(),
  };
}
