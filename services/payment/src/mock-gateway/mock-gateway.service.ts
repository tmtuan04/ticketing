import {
  BadRequestException,
  ConflictException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import { PollLoop } from '../common/poll-loop';
import { APP_CONFIG, type AppConfig } from '../config';
import { Prisma, type ChargeOutcome, type MockCharge } from '../generated/prisma/client';
import { signWebhook, SIGNATURE_HEADER } from '../payments/webhook-signature';
import { PrismaService } from '../prisma/prisma.service';

const DELIVERY_BATCH = 20;
/** Worker giữ charge trong khoảng này khi đang gửi; phải lớn hơn deliveryTimeout. */
const DELIVERY_LEASE = '30 seconds';
const MAX_RETRY_BACKOFF_MS = 30_000;

export interface CreateChargeInput {
  intentId: string;
  idempotencyKey: string;
  outcome: ChargeOutcome;
  delayMs: number;
}

interface DueCharge {
  id: string;
  intent_id: string;
  outcome: ChargeOutcome;
  amount: bigint;
  currency: string;
  attempts: number;
}

/**
 * Cổng thanh toán giả lập, đóng vai bên thứ ba (kiểu Stripe/VNPay): client "trả tiền" ở đây, rồi
 * cổng báo kết quả cho payment service qua webhook có chữ ký HMAC.
 *
 * Gửi webhook bền vững: charge lưu trong DB kèm thời điểm gửi (`deliver_at`), worker poll và gửi,
 * lỗi thì retry với backoff. Restart giữa chừng không mất webhook; gửi lặp (mất phản hồi 2xx) luôn
 * dùng cùng id sự kiện nên phía nhận chống trùng được.
 */
@Injectable()
export class MockGatewayService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly log = new Logger(MockGatewayService.name);
  private readonly loop: PollLoop;

  constructor(
    private readonly prisma: PrismaService,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {
    this.loop = new PollLoop(
      'webhook delivery',
      () => this.deliverDue().then(() => undefined),
      config.mockGateway.pollIntervalMs,
      this.log,
    );
  }

  onApplicationBootstrap() {
    this.loop.start();
  }

  async onApplicationShutdown() {
    await this.loop.stop();
  }

  /**
   * Tạo charge cho intent. Mỗi intent tối đa một charge: gửi lại cùng Idempotency-Key trả về charge
   * cũ, key khác => 409. Kết quả không áp ngay mà đến qua webhook sau `delayMs`.
   */
  async createCharge(input: CreateChargeInput): Promise<MockCharge> {
    if (input.delayMs > this.config.mockGateway.maxDelayMs) {
      throw new BadRequestException(`delayMs must be at most ${this.config.mockGateway.maxDelayMs}`);
    }
    const existing = await this.prisma.mockCharge.findUnique({ where: { intentId: input.intentId } });
    if (existing) return this.replay(existing, input.idempotencyKey);

    const intent = await this.prisma.paymentIntent.findUnique({ where: { id: input.intentId } });
    if (!intent) throw new NotFoundException('payment intent not found');
    if (intent.status !== 'requires_payment') {
      throw new ConflictException(`payment intent is already ${intent.status}`);
    }
    try {
      return await this.prisma.mockCharge.create({
        data: {
          intentId: intent.id,
          idempotencyKey: input.idempotencyKey,
          outcome: input.outcome,
          amount: intent.amount,
          currency: intent.currency,
          deliverAt: new Date(Date.now() + input.delayMs),
        },
      });
    } catch (err) {
      // Hai request cùng lúc cho một intent: request thua đọc lại charge của request thắng.
      if (err instanceof Prisma.PrismaClientKnownRequestError && err.code === 'P2002') {
        const winner = await this.prisma.mockCharge.findUniqueOrThrow({ where: { intentId: intent.id } });
        return this.replay(winner, input.idempotencyKey);
      }
      throw err;
    }
  }

  private replay(charge: MockCharge, idempotencyKey: string) {
    if (charge.idempotencyKey !== idempotencyKey) {
      throw new ConflictException('payment intent already has a charge');
    }
    return charge;
  }

  /**
   * Gửi webhook cho các charge đến hạn, trả về số charge đã xử lý. Exported để test gọi trực tiếp.
   *
   * Nhận việc bằng lease thay vì giữ transaction trong lúc gọi HTTP: một câu UPDATE vừa khóa
   * (SKIP LOCKED) vừa đẩy deliver_at lên DELIVERY_LEASE, replica khác sẽ không thấy charge này
   * cho tới khi lease hết (process chết giữa chừng => lease hết hạn, charge được gửi lại).
   */
  async deliverDue(): Promise<number> {
    const due = await this.prisma.$queryRaw<DueCharge[]>`
      UPDATE mock_charges
      SET deliver_at = now() + ${DELIVERY_LEASE}::interval, attempts = attempts + 1
      WHERE id IN (
        SELECT id FROM mock_charges
        WHERE delivered_at IS NULL AND outcome <> 'timeout' AND deliver_at <= now()
        ORDER BY deliver_at
        LIMIT ${DELIVERY_BATCH}
        FOR UPDATE SKIP LOCKED
      )
      RETURNING id::text, intent_id::text, outcome, amount, currency, attempts`;
    await Promise.all(due.map((c) => this.deliver(c)));
    return due.length;
  }

  private async deliver(c: DueCharge) {
    const succeeded = c.outcome === 'success';
    const body = JSON.stringify({
      // id cố định theo charge: lần gửi lại là CÙNG một sự kiện, phía nhận chống trùng theo id này
      id: `evt_${c.id}`,
      type: succeeded ? 'charge.succeeded' : 'charge.failed',
      created: Math.floor(Date.now() / 1000),
      data: {
        charge_id: c.id,
        payment_intent_id: c.intent_id,
        amount: Number(c.amount),
        currency: c.currency,
        failure_reason: succeeded ? '' : 'card_declined',
      },
    });
    let error: string;
    try {
      const res = await fetch(this.config.webhook.url, {
        method: 'POST',
        headers: { 'content-type': 'application/json', [SIGNATURE_HEADER]: signWebhook(this.config.webhook.secret, body) },
        body,
        signal: AbortSignal.timeout(this.config.mockGateway.deliveryTimeoutMs),
      });
      if (res.ok) {
        await this.prisma.mockCharge.update({
          where: { id: c.id },
          data: { deliveredAt: new Date(), lastError: '' },
        });
        return;
      }
      error = `HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`;
    } catch (err) {
      error = String(err);
    }
    // Như cổng thật: retry mãi với backoff tăng dần (500ms, 1s, 2s... tối đa 30s).
    const backoffMs = Math.min(500 * 2 ** (c.attempts - 1), MAX_RETRY_BACKOFF_MS);
    this.log.warn({ msg: 'webhook delivery failed', charge_id: c.id, attempts: c.attempts, err: error, retry_in_ms: backoffMs });
    await this.prisma.mockCharge.update({
      where: { id: c.id },
      data: { deliverAt: new Date(Date.now() + backoffMs), lastError: error },
    });
  }
}
