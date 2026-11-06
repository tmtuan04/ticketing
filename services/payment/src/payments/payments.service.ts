import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import type { PaymentIntent, Prisma } from '../generated/prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import {
  InvalidMessageError,
  parsePaymentRequested,
  parseRefundRequested,
  TOPIC_PAYMENT_EVENTS,
  TYPE_PAYMENT_FAILED,
  TYPE_PAYMENT_REFUND_REQUESTED,
  TYPE_PAYMENT_REFUNDED,
  TYPE_PAYMENT_REQUESTED,
  TYPE_PAYMENT_SUCCEEDED,
  type PaymentRefundRequested,
  type PaymentRequested,
  type PaymentResult,
} from './events';

/** Client Prisma bên trong $transaction. */
type Tx = Prisma.TransactionClient;

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
    // Parse trước transaction: payload sai thì retry cũng vô ích, consumer đẩy sang DLQ.
    let requested: PaymentRequested | undefined;
    let refund: PaymentRefundRequested | undefined;
    if (cmd.type === TYPE_PAYMENT_REQUESTED) requested = parsePaymentRequested(cmd.value);
    if (cmd.type === TYPE_PAYMENT_REFUND_REQUESTED) refund = parseRefundRequested(cmd.value);

    return this.prisma.$transaction(async (tx) => {
      const fresh = await tx.$executeRaw`
        INSERT INTO inbox (message_id, topic) VALUES (${cmd.messageId}::uuid, ${cmd.topic})
        ON CONFLICT DO NOTHING`;
      if (!fresh) return 'duplicate';

      if (refund) return this.refund(tx, refund, cmd.messageId);
      if (!requested) {
        // type lạ: vẫn ghi inbox để không xử lý lại mãi
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
   * Hoàn tiền cho intent đã thu (succeeded -> refunded) và báo lại order qua outbox payment.refunded.
   * Cổng giả lập hoàn tiền ngay, không có webhook riêng cho refund. Chạy trong transaction của
   * handleCommand, cùng với inbox.
   */
  private async refund(tx: Tx, cmd: PaymentRefundRequested, messageId: string): Promise<HandleResult> {
    // UPDATE có điều kiện: chỉ intent đã succeeded mới hoàn được, và chỉ hoàn một lần.
    const { count } = await tx.paymentIntent.updateMany({
      where: { orderId: cmd.order_id, status: 'succeeded' },
      data: { status: 'refunded', refundReason: cmd.reason, refundedAt: new Date() },
    });
    const intent = await tx.paymentIntent.findUnique({ where: { orderId: cmd.order_id } });
    // Order chỉ yêu cầu hoàn tiền sau khi nhận payment.succeeded, nên không có intent là message sai.
    if (!intent) throw new InvalidMessageError(`no payment intent for order ${cmd.order_id}`);
    if (!count) {
      this.log.warn({ msg: 'refund ignored', order_id: intent.orderId, status: intent.status, message_id: messageId });
      return 'ignored';
    }
    await this.enqueueResult(tx, intent, TYPE_PAYMENT_REFUNDED, '');
    this.log.log({ msg: 'payment refunded', intent_id: intent.id, order_id: intent.orderId, reason: cmd.reason });
    return 'processed';
  }

  /** Ghi outbox một message payment.events (key = order_id). */
  private async enqueueResult(tx: Tx, intent: PaymentIntent, type: string, failureReason: string) {
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
        type,
        payload: { ...payload },
      },
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

      await this.enqueueResult(tx, intent, succeeded ? TYPE_PAYMENT_SUCCEEDED : TYPE_PAYMENT_FAILED, failureReason);
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
