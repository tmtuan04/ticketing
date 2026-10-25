import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import type { PaymentIntent } from '../generated/prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import {
  parsePaymentRequested,
  TOPIC_PAYMENT_EVENTS,
  TYPE_PAYMENT_FAILED,
  TYPE_PAYMENT_REQUESTED,
  TYPE_PAYMENT_SUCCEEDED,
  type PaymentRequested,
  type PaymentResult,
} from './events';

export interface IncomingCommand {
  messageId: string;
  topic: string;
  type: string | undefined;
  value: Buffer | null;
}

/** Sự kiện cổng thanh toán gửi qua webhook (đã xác minh chữ ký). */
export interface ProviderEvent {
  id: string;
  type: 'charge.succeeded' | 'charge.failed';
  data: { payment_intent_id: string; failure_reason?: string };
}

export type HandleResult = 'processed' | 'duplicate' | 'ignored';

@Injectable()
export class PaymentsService {
  private readonly log = new Logger(PaymentsService.name);

  constructor(private readonly prisma: PrismaService) {}

  /**
   * Xử lý một lệnh từ payment.commands. Inbox + thay đổi nghiệp vụ nằm trong CÙNG transaction:
   * message_id đã có trong inbox thì bỏ qua (Redpanda giao at-least-once, relay có thể publish lại).
   * Payload sai contract ném InvalidMessageError; lỗi DB ném ra để consumer không commit offset.
   */
  async handleCommand(cmd: IncomingCommand): Promise<HandleResult> {
    // Parse trước transaction: payload sai thì retry cũng vô ích (poison message, DLQ ở M6).
    let requested: PaymentRequested | undefined;
    if (cmd.type === TYPE_PAYMENT_REQUESTED) requested = parsePaymentRequested(cmd.value);

    return this.prisma.$transaction(async (tx) => {
      const fresh = await tx.$executeRaw`
        INSERT INTO inbox (message_id, topic) VALUES (${cmd.messageId}::uuid, ${cmd.topic})
        ON CONFLICT DO NOTHING`;
      if (!fresh) return 'duplicate';

      if (!requested) {
        // vd payment.refund_requested trước khi M6 làm: vẫn ghi inbox để không xử lý lại mãi
        this.log.warn({ msg: 'ignoring unknown command type', type: cmd.type, message_id: cmd.messageId });
        return 'ignored';
      }
      // Idempotent theo order_id: order gửi lại payment.requested (message_id mới) vẫn chỉ một intent.
      const { count } = await tx.paymentIntent.createMany({
        data: {
          orderId: requested.order_id,
          userId: requested.user_id,
          amount: BigInt(requested.amount),
          currency: requested.currency,
          paymentDeadline: new Date(requested.payment_deadline),
        },
        skipDuplicates: true,
      });
      this.log.log({
        msg: count ? 'payment intent created' : 'payment intent already exists',
        order_id: requested.order_id,
        message_id: cmd.messageId,
      });
      return 'processed';
    });
  }

  /**
   * Áp kết quả thanh toán từ webhook. Một transaction: ghi webhook_events (chống trùng theo id sự
   * kiện của cổng) + đổi trạng thái intent + outbox payment.succeeded|failed. Cổng gửi lại cùng sự
   * kiện (timeout, mất phản hồi...) thì chỉ có tác dụng một lần.
   */
  async handleProviderEvent(evt: ProviderEvent): Promise<HandleResult> {
    return this.prisma.$transaction(async (tx) => {
      const fresh = await tx.$executeRaw`
        INSERT INTO webhook_events (provider_event_id, type) VALUES (${evt.id}, ${evt.type})
        ON CONFLICT DO NOTHING`;
      if (!fresh) return 'duplicate';

      const succeeded = evt.type === 'charge.succeeded';
      const failureReason = succeeded ? '' : evt.data.failure_reason || 'payment_failed';
      // Chỉ chuyển từ requires_payment. UPDATE có điều kiện là atomic, không cần SELECT FOR UPDATE.
      const { count } = await tx.paymentIntent.updateMany({
        where: { id: evt.data.payment_intent_id, status: 'requires_payment' },
        data: { status: succeeded ? 'succeeded' : 'failed', failureReason },
      });
      const intent = await tx.paymentIntent.findUnique({ where: { id: evt.data.payment_intent_id } });
      if (!intent) throw new NotFoundException('payment intent not found');
      if (!count) {
        // Intent đã ở trạng thái cuối (vd cổng gửi cả succeeded lẫn failed): giữ kết quả đầu tiên.
        this.log.warn({
          msg: 'webhook for finalized intent ignored',
          intent_id: intent.id,
          status: intent.status,
          event_type: evt.type,
        });
        return 'ignored';
      }

      const payload: PaymentResult = {
        order_id: intent.orderId,
        payment_intent_id: intent.id,
        amount: Number(intent.amount),
        currency: intent.currency,
        failure_reason: failureReason,
        occurred_at: new Date().toISOString(),
      };
      await tx.outboxMessage.create({
        data: {
          messageId: randomUUID(),
          aggregateId: intent.orderId,
          topic: TOPIC_PAYMENT_EVENTS,
          key: intent.orderId,
          type: succeeded ? TYPE_PAYMENT_SUCCEEDED : TYPE_PAYMENT_FAILED,
          payload: { ...payload },
        },
      });
      this.log.log({ msg: `payment ${intent.status}`, intent_id: intent.id, order_id: intent.orderId });
      return 'processed';
    });
  }

  async getByOrder(orderId: string): Promise<PaymentIntent> {
    const intent = await this.prisma.paymentIntent.findUnique({ where: { orderId } });
    if (!intent) throw new NotFoundException('payment intent not found');
    return intent;
  }
}
