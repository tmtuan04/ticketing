// Contract của các message Payment trao đổi với Order qua Redpanda. Payload là JSON snake_case,
// khớp với struct tag bên Go (services/order/internal/service/events.go). message_id và type đi
// trong header, xem kafka.module.ts.

/** Lệnh Order gửi cho Payment. Key = order_id. */
export const TOPIC_PAYMENT_COMMANDS = 'payment.commands';
/** Kết quả Payment gửi lại cho Order. Key = order_id để mọi sự kiện của một đơn vào cùng partition. */
export const TOPIC_PAYMENT_EVENTS = 'payment.events';

export const TYPE_PAYMENT_REQUESTED = 'payment.requested';
export const TYPE_PAYMENT_SUCCEEDED = 'payment.succeeded';
export const TYPE_PAYMENT_FAILED = 'payment.failed';

export interface PaymentRequested {
  order_id: string;
  user_id: string;
  amount: number;
  currency: string;
  payment_deadline: string;
}

/** Payload chung của payment.succeeded và payment.failed. */
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
  let p: Record<string, unknown>;
  try {
    p = JSON.parse(value?.toString() ?? '');
  } catch {
    throw new InvalidMessageError('payload is not valid JSON');
  }
  if (typeof p !== 'object' || p === null) throw new InvalidMessageError('payload must be an object');

  const errs: string[] = [];
  for (const k of ['order_id', 'user_id'] as const) {
    if (typeof p[k] !== 'string' || !UUID_RE.test(p[k])) errs.push(`${k} must be a uuid`);
  }
  if (typeof p.amount !== 'number' || !Number.isSafeInteger(p.amount) || p.amount < 0) {
    errs.push('amount must be a non-negative integer');
  }
  if (typeof p.currency !== 'string' || !/^[A-Z]{3}$/.test(p.currency)) {
    errs.push('currency must be a 3-letter ISO code');
  }
  if (typeof p.payment_deadline !== 'string' || Number.isNaN(Date.parse(p.payment_deadline))) {
    errs.push('payment_deadline must be an RFC 3339 timestamp');
  }
  if (errs.length) throw new InvalidMessageError(errs.join('; '));
  return p as unknown as PaymentRequested;
}
