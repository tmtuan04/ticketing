// Contract của các message Payment trao đổi với Order qua Redpanda. Payload là JSON snake_case,
// khớp với struct tag bên Go (services/order/internal/service/events.go). message_id và type đi
// trong header, xem kafka.module.ts.

/** Lệnh Order gửi cho Payment. Key = order_id. */
export const TOPIC_PAYMENT_COMMANDS = 'payment.commands';
/** Kết quả Payment gửi lại cho Order. Key = order_id để mọi sự kiện của một đơn vào cùng partition. */
export const TOPIC_PAYMENT_EVENTS = 'payment.events';

/** Message xử lý không được (sai contract, lỗi lặp lại) của topic T nằm ở T + DLQ_SUFFIX. */
export const DLQ_SUFFIX = '.dlq';

export const TYPE_PAYMENT_REQUESTED = 'payment.requested';
export const TYPE_PAYMENT_REFUND_REQUESTED = 'payment.refund_requested';
export const TYPE_PAYMENT_SUCCEEDED = 'payment.succeeded';
export const TYPE_PAYMENT_FAILED = 'payment.failed';
export const TYPE_PAYMENT_REFUNDED = 'payment.refunded';

export interface PaymentRequested {
  order_id: string;
  user_id: string;
  amount: number;
  currency: string;
  payment_deadline: string;
}

/** Order gửi khi tiền đã thu nhưng đơn không giữ được ghế (hold hết hạn, đơn đã bị hủy). */
export interface PaymentRefundRequested {
  order_id: string;
  payment_intent_id: string;
  amount: number;
  currency: string;
  /** hold_expired | paid_after_cancel */
  reason: string;
}

/** Payload chung của payment.succeeded, payment.failed và payment.refunded. */
export interface PaymentResult {
  order_id: string;
  payment_intent_id: string;
  amount: number;
  currency: string;
  /** rỗng khi succeeded */
  failure_reason: string;
  occurred_at: string;
}

export const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Message sai contract: xử lý lại bao nhiêu lần cũng vẫn sai, không được retry. */
export class InvalidMessageError extends Error {}

/** Parse và kiểm tra payload payment.requested; sai thì ném InvalidMessageError. */
export function parsePaymentRequested(value: Buffer | null): PaymentRequested {
  const p = parseObject(value);
  const errs = [...uuids(p, 'order_id', 'user_id'), ...money(p)];
  if (typeof p.payment_deadline !== 'string' || Number.isNaN(Date.parse(p.payment_deadline))) {
    errs.push('payment_deadline must be an RFC 3339 timestamp');
  }
  if (errs.length) throw new InvalidMessageError(errs.join('; '));
  return p as unknown as PaymentRequested;
}

/** Parse và kiểm tra payload payment.refund_requested; sai thì ném InvalidMessageError. */
export function parseRefundRequested(value: Buffer | null): PaymentRefundRequested {
  const p = parseObject(value);
  const errs = [...uuids(p, 'order_id', 'payment_intent_id'), ...money(p)];
  if (typeof p.reason !== 'string' || !p.reason) errs.push('reason must be a non-empty string');
  if (errs.length) throw new InvalidMessageError(errs.join('; '));
  return p as unknown as PaymentRefundRequested;
}

function parseObject(value: Buffer | null): Record<string, unknown> {
  let p: unknown;
  try {
    p = JSON.parse(value?.toString() ?? '');
  } catch {
    throw new InvalidMessageError('payload is not valid JSON');
  }
  if (typeof p !== 'object' || p === null) throw new InvalidMessageError('payload must be an object');
  return p as Record<string, unknown>;
}

function uuids(p: Record<string, unknown>, ...keys: string[]): string[] {
  return keys.filter((k) => typeof p[k] !== 'string' || !UUID_RE.test(p[k])).map((k) => `${k} must be a uuid`);
}

function money(p: Record<string, unknown>): string[] {
  const errs: string[] = [];
  if (typeof p.amount !== 'number' || !Number.isSafeInteger(p.amount) || p.amount < 0) {
    errs.push('amount must be a non-negative integer');
  }
  if (typeof p.currency !== 'string' || !/^[A-Z]{3}$/.test(p.currency)) {
    errs.push('currency must be a 3-letter ISO code');
  }
  return errs;
}
