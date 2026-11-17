import type { IntentView, OrderStatus, OrderView } from '../api';
import { fmtTime } from '../lib';
import type { OrderTrack } from '../session';

// ---------- Timeline các bước Saga, suy ra từ trạng thái order + intent quan sát được ----------

type StepState = 'done' | 'active' | 'pending' | 'failed' | 'skipped';
type Step = { key: string; title: string; service: string; detail: string; state: StepState; at?: string | number | null };

export const FAILURE_TEXT: Record<string, string> = {
  payment_failed: 'thanh toán thất bại',
  payment_timeout: 'quá hạn thanh toán (timeout worker)',
  hold_expired: 'tiền về khi hold đã hết hạn',
  paid_after_cancel: 'tiền về sau khi đơn đã huỷ',
};

export function buildSteps(order: OrderView | null, intent: IntentView | null, track: OrderTrack): Step[] {
  const st = order?.status;
  const reason = order?.failureReason ?? '';
  const timedOut = st === 'cancelled' && reason === 'payment_timeout';
  const steps: Step[] = [];

  steps.push({
    key: 'hold',
    title: 'Giữ ghế',
    service: 'gateway → inventory',
    detail: 'HoldSeats: Redis Lua lọc nhanh, Postgres UPDATE có điều kiện chốt chặn.',
    state: 'done',
  });

  steps.push({
    key: 'order',
    title: 'Tạo đơn (1 transaction)',
    service: 'gateway → order',
    detail: 'GetHold → tính tiền từ catalog → ExtendHold → INSERT orders + order_saga_log + outbox(payment.requested).',
    state: order ? 'done' : 'active',
    at: order?.createdAt,
  });

  steps.push({
    key: 'intent',
    title: 'Outbox relay → Redpanda → Payment tạo intent',
    service: 'order relay → payment.commands → payment',
    detail: intent
      ? `Intent ${intent.id.slice(0, 8)} (inbox chống trùng, UNIQUE order_id).`
      : 'Bất đồng bộ: GET /payments/orders/:id còn 404 cho tới khi consumer của payment xử lý xong.',
    state: intent ? 'done' : order ? 'active' : 'pending',
    at: intent?.createdAt,
  });

  const paid = track.payment;
  steps.push({
    key: 'charge',
    title: 'Trả tiền ở cổng giả lập',
    service: 'client → mock-gateway (202)',
    detail: paid
      ? `outcome=${paid.outcome}${paid.delayMs ? `, delayMs=${paid.delayMs}` : ''}. Cổng chỉ ghi mock_charges rồi trả 202.`
      : timedOut
        ? 'Không trả tiền trước payment_deadline.'
        : 'Chờ bạn chọn kết quả thanh toán.',
    state: paid ? 'done' : timedOut ? 'skipped' : intent ? 'active' : 'pending',
    at: paid?.at,
  });

  const webhookDone = intent && intent.status !== 'requires_payment';
  steps.push({
    key: 'webhook',
    title: 'Webhook HMAC → intent + outbox(payment.events)',
    service: 'mock-gateway → payment',
    detail: webhookDone
      ? `Intent → ${intent!.status}. Chữ ký HMAC-SHA256 trên body thô, dedup bằng webhook_events.`
      : paid?.outcome === 'timeout'
        ? 'outcome=timeout: cổng không bao giờ gọi webhook, đơn sẽ bị timeout worker huỷ.'
        : 'Worker của cổng giả lập gửi webhook (retry tới khi 2xx).',
    state: webhookDone
      ? intent!.status === 'failed'
        ? 'failed'
        : 'done'
      : paid?.outcome === 'timeout' || timedOut
        ? 'skipped'
        : paid
          ? 'active'
          : 'pending',
    at: webhookDone ? intent!.updatedAt : null,
  });

  // Bước chốt của Saga ở order
  let final: Step;
  if (st === 'confirmed') {
    final = {
      key: 'saga',
      title: 'Order: ConfirmHold → confirmed',
      service: 'payment.events → order → inventory',
      detail: 'inbox + SELECT order FOR UPDATE + ConfirmHold (held → sold) + outbox(order.confirmed), cùng 1 transaction.',
      state: 'done',
    };
  } else if (st === 'cancelled') {
    final = {
      key: 'saga',
      title: timedOut ? 'Timeout worker: ReleaseHold → cancelled' : 'Order: ReleaseHold → cancelled',
      service: timedOut ? 'order expiry worker → inventory' : 'payment.events → order → inventory',
      detail: `Bù trừ: nhả ghế, đơn cancelled (${FAILURE_TEXT[reason] ?? reason}). Ghế về available.`,
      state: 'failed',
    };
  } else if (st === 'refunding' || st === 'refunded') {
    final = {
      key: 'saga',
      title: 'Order: không xác nhận được ghế → refunding',
      service: 'payment.events → order',
      detail: `Tiền đã thu nhưng ${FAILURE_TEXT[reason] ?? reason} → outbox(payment.refund_requested).`,
      state: 'failed',
    };
  } else {
    final = {
      key: 'saga',
      title: 'Order nhận payment.events',
      service: 'payment.events → order → inventory',
      detail: 'Thành công → ConfirmHold → confirmed. Thất bại → ReleaseHold → cancelled. Quá hạn → timeout worker huỷ.',
      state: webhookDone ? 'active' : 'pending',
    };
  }
  steps.push({ ...final, at: final.state === 'pending' || final.state === 'active' ? null : lastAt(track, st) });

  if (st === 'refunding' || st === 'refunded') {
    steps.push({
      key: 'refund',
      title: 'Payment hoàn tiền → order refunded',
      service: 'payment.commands → payment → payment.events → order',
      detail: 'Bù trừ cho bước thu tiền: intent succeeded → refunded, outbox(payment.refunded), đơn → refunded.',
      state: st === 'refunded' ? 'done' : 'active',
      at: st === 'refunded' ? (intent?.refundedAt ?? lastAt(track, 'refunded')) : null,
    });
  }
  return steps;
}

function lastAt(track: OrderTrack, status: OrderStatus | undefined) {
  return [...track.transitions].reverse().find((t) => t.status === status)?.at ?? null;
}

export function SagaTimeline({ steps }: { steps: Step[] }) {
  return (
    <ol className="timeline">
      {steps.map((s) => (
        <li key={s.key} className={`step ${s.state}`}>
          <span className="step-dot" />
          <div className="step-body">
            <div className="step-title">{s.title}</div>
            <div className="step-service">{s.service}</div>
            <div className="step-detail">{s.detail}</div>
          </div>
          <span className="step-time">{s.at ? fmtTime(s.at) : ''}</span>
        </li>
      ))}
    </ol>
  );
}

// ---------- Máy trạng thái của order, tô các trạng thái/cạnh đã đi qua ----------

const NODES: { id: OrderStatus; x: number; y: number }[] = [
  { id: 'awaiting_payment', x: 2, y: 80 },
  { id: 'confirmed', x: 330, y: 8 },
  { id: 'cancelled', x: 330, y: 152 },
  { id: 'refunding', x: 610, y: 80 },
  { id: 'refunded', x: 846, y: 80 },
];
const EDGES: { from: OrderStatus; to: OrderStatus; label: string }[] = [
  { from: 'awaiting_payment', to: 'confirmed', label: 'succeeded' },
  { from: 'awaiting_payment', to: 'cancelled', label: 'failed / timeout' },
  { from: 'awaiting_payment', to: 'refunding', label: 'hold_expired' },
  { from: 'cancelled', to: 'refunding', label: 'paid_after_cancel' },
  { from: 'refunding', to: 'refunded', label: 'refunded' },
];
const W = 150;
const H = 38;

export function StateMachine({ track, current }: { track: OrderTrack; current: OrderStatus | undefined }) {
  const visited = new Set(track.transitions.map((t) => t.status));
  const walked = new Set<string>();
  for (let i = 1; i < track.transitions.length; i++) {
    walked.add(`${track.transitions[i - 1].status}>${track.transitions[i].status}`);
  }
  const node = (id: OrderStatus) => NODES.find((n) => n.id === id)!;

  return (
    <svg className="statemachine" viewBox="0 0 1000 194" role="img" aria-label="Máy trạng thái đơn hàng">
      <defs>
        <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto">
          <path d="M0,0 L10,5 L0,10 z" className="sm-arrowhead" />
        </marker>
        <marker id="arrow-on" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto">
          <path d="M0,0 L10,5 L0,10 z" className="sm-arrowhead on" />
        </marker>
      </defs>
      {EDGES.map((e) => {
        const a = node(e.from);
        const b = node(e.to);
        const x1 = a.x + W;
        const y1 = a.y + H / 2;
        const x2 = b.x;
        const y2 = b.y + H / 2;
        const on = walked.has(`${e.from}>${e.to}`);
        return (
          <g key={`${e.from}-${e.to}`} className={`sm-edge ${on ? 'on' : ''}`}>
            <line x1={x1} y1={y1} x2={x2 - 2} y2={y2} markerEnd={`url(#${on ? 'arrow-on' : 'arrow'})`} />
            <text x={(x1 + x2) / 2} y={(y1 + y2) / 2 - 5} textAnchor="middle">
              {e.label}
            </text>
          </g>
        );
      })}
      {NODES.map((n) => (
        <g key={n.id} className={`sm-node ${visited.has(n.id) ? 'visited' : ''} ${current === n.id ? 'current' : ''} st-${n.id}`}>
          <rect x={n.x} y={n.y} width={W} height={H} />
          <text x={n.x + W / 2} y={n.y + H / 2 + 5} textAnchor="middle">
            {n.id}
          </text>
        </g>
      ))}
    </svg>
  );
}
