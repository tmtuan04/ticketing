import { useCallback, useEffect, useState } from 'react';
import { api, errorText, newKey, type ChargeView, type IntentView, type OrderView, type SeatView } from '../api';
import { buildSteps, FAILURE_TEXT, SagaTimeline, StateMachine } from '../components/SagaView';
import { fmtCountdown, fmtDateTime, money, navigate, seatLabel, useInterval, useNow } from '../lib';
import { loadTrack, saveTrack, type OrderTrack, type PaymentAttempt } from '../session';

const STATUS_TEXT: Record<string, string> = {
  awaiting_payment: 'Chờ thanh toán',
  confirmed: 'Đã xác nhận',
  cancelled: 'Đã huỷ',
  refunding: 'Đang hoàn tiền',
  refunded: 'Đã hoàn tiền',
};

export function OrderPage({ orderId }: { orderId: string }) {
  const [order, setOrder] = useState<OrderView | null>(null);
  const [intent, setIntent] = useState<IntentView | null>(null);
  const [track, setTrack] = useState<OrderTrack>(() => loadTrack(orderId));
  const [seats, setSeats] = useState<Map<string, SeatView>>(new Map());
  const [outcome, setOutcome] = useState<PaymentAttempt['outcome']>('success');
  const [delayMs, setDelayMs] = useState(0);
  const [charge, setCharge] = useState<ChargeView | null>(null);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<{ kind: 'ok' | 'err' | 'info'; text: string } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const now = useNow();

  const updateTrack = useCallback(
    (fn: (t: OrderTrack) => OrderTrack) => {
      setTrack((prev) => {
        const next = fn(prev);
        saveTrack(orderId, next);
        return next;
      });
    },
    [orderId],
  );

  const pollOrder = useCallback(async () => {
    try {
      const o = await api.orders.get(orderId, 'poll');
      setOrder(o);
      setError(null);
      updateTrack((t) =>
        t.transitions.at(-1)?.status === o.status ? t : { ...t, transitions: [...t.transitions, { status: o.status, at: Date.now() }] },
      );
    } catch (err) {
      setError(errorText(err));
    }
  }, [orderId, updateTrack]);

  const pollIntent = useCallback(async () => {
    const i = await api.payment.intentOf(orderId);
    if (i) {
      setIntent(i);
      updateTrack((t) => (t.intentSeenAt ? t : { ...t, intentSeenAt: Date.now() }));
    }
  }, [orderId, updateTrack]);

  useEffect(() => {
    setTrack(loadTrack(orderId));
    setIntent(null);
    setCharge(null);
    setMsg(null);
    pollOrder();
    pollIntent().catch(() => {});
  }, [orderId, pollOrder, pollIntent]);

  useEffect(() => {
    if (!order) return;
    api.events.seats(order.eventId).then(
      (r) => setSeats(new Map(r.seats.map((s) => [s.seatId, s]))),
      () => {},
    );
  }, [order?.eventId]);

  // Trạng thái cuối thì poll thưa lại (đơn cancelled vẫn có thể sang refunding nếu tiền về muộn)
  const terminal = order?.status === 'confirmed' || order?.status === 'refunded';
  useInterval(pollOrder, terminal ? 5000 : 1000);
  useInterval(() => pollIntent().catch(() => {}), intent && terminal ? 5000 : 1000);

  async function payWith(key: string) {
    if (!intent) return;
    setBusy(true);
    setMsg(null);
    try {
      const c = await api.payment.pay(intent.id, outcome, delayMs, key);
      setCharge(c);
      const reused = track.payment?.idempotencyKey === key;
      if (!reused) updateTrack((t) => ({ ...t, payment: { idempotencyKey: key, outcome, delayMs, at: Date.now() } }));
      setMsg({
        kind: 'ok',
        text: reused
          ? `Cùng key: cổng trả lại charge #${c.id.slice(0, 8)} cũ, không thu lần hai.`
          : `202: đã ghi charge #${c.id.slice(0, 8)}, kết quả về qua webhook.`,
      });
    } catch (err) {
      setMsg({ kind: 'err', text: errorText(err) });
    } finally {
      setBusy(false);
    }
  }

  const steps = buildSteps(order, intent, track);
  const deadlineLeft = order?.paymentDeadline ? new Date(order.paymentDeadline).getTime() - now : 0;
  const canPay = intent?.status === 'requires_payment' && !track.payment;

  return (
    <div>
      <button className="back" onClick={() => navigate('/orders')}>
        ← Đơn của tôi
      </button>
      <div className="page-head">
        <div>
          <div className="eyebrow mono">{orderId}</div>
          <h1>Đơn #{orderId.slice(0, 8)}</h1>
        </div>
        {order && <span className={`status big st-${order.status}`}>{STATUS_TEXT[order.status] ?? order.status}</span>}
      </div>
      {error && <div className="alert err">{error}</div>}

      <div className="layout-2">
        <div>
          <div className="card">
            <h3>Saga</h3>
            <StateMachine track={track} current={order?.status} />
            <SagaTimeline steps={steps} />
          </div>
          <details className="card">
            <summary>Lệnh soi DB và log</summary>
            <pre className="code">{`docker compose -f deploy/docker-compose.yml exec postgres psql -U app -d order_db \\
  -c "select step, status, created_at from order_saga_log where order_id='${orderId}' order by id;" \\
  -c "select topic, type, published_at from outbox where key='${orderId}' order by id;"

docker compose -f deploy/docker-compose.yml exec postgres psql -U app -d payment_db \\
  -c "select status, failure_reason, refund_reason from payment_intents where order_id='${orderId}';"

docker compose -f deploy/docker-compose.yml logs order payment | grep ${orderId}`}</pre>
            <p className="tiny muted">
              Message trên Redpanda: <a href="http://localhost:9080" target="_blank" rel="noreferrer">Console</a>, topic{' '}
              <code>payment.commands</code>, <code>payment.events</code>, <code>order.events</code>, key = order id.
            </p>
          </details>
        </div>

        <aside className="side">
          {order && (
            <div className="card">
              <h3>Chi tiết</h3>
              <dl className="kv">
                <dt>Số tiền</dt>
                <dd>
                  <span className="amount">{money(order.amount, order.currency)}</span>
                </dd>
                <dt>Ghế</dt>
                <dd className="chips">
                  {order.seatIds.map((id) => (
                    <span key={id} className="chip">
                      {seats.get(id) ? seatLabel(seats.get(id)!) : id.slice(0, 8)}
                    </span>
                  ))}
                </dd>
                <dt>Hạn thanh toán</dt>
                <dd>
                  {fmtDateTime(order.paymentDeadline)}
                  {order.status === 'awaiting_payment' && <div className="countdown small">{fmtCountdown(deadlineLeft)}</div>}
                </dd>
                {order.failureReason && (
                  <>
                    <dt>Lý do</dt>
                    <dd>
                      <code>{order.failureReason}</code> · {FAILURE_TEXT[order.failureReason] ?? ''}
                    </dd>
                  </>
                )}
                <dt>Intent</dt>
                <dd>
                  {intent ? (
                    <>
                      <span className={`status st-${intent.status}`}>{intent.status}</span>{' '}
                      <span className="mono tiny muted">#{intent.id.slice(0, 8)}</span>
                    </>
                  ) : (
                    <span className="muted">chưa tới (đang qua Redpanda)</span>
                  )}
                </dd>
              </dl>
              <button className="link tiny" onClick={() => navigate(`/events/${order.eventId}`)}>
                Mở sơ đồ ghế
              </button>
            </div>
          )}

          {intent && (
            <div className="card">
              <h3>Thanh toán (cổng giả lập)</h3>
              <div className="outcomes">
                {(['success', 'fail', 'timeout'] as const).map((o) => (
                  <label key={o} className={`outcome ${outcome === o ? 'active' : ''}`}>
                    <input type="radio" name="outcome" checked={outcome === o} onChange={() => setOutcome(o)} disabled={!canPay} />
                    {o === 'success' ? 'Thành công' : o === 'fail' ? 'Thất bại' : 'Không phản hồi'}
                    <small>{o}</small>
                  </label>
                ))}
              </div>
              <label className="field">
                Trễ webhook (ms)
                <input
                  type="number"
                  min={0}
                  max={60000}
                  step={500}
                  value={delayMs}
                  disabled={!canPay}
                  onChange={(e) => setDelayMs(Number(e.target.value))}
                />
              </label>
              <div className="btn-col">
                <button className="btn primary" disabled={busy || !canPay} onClick={() => payWith(newKey())}>
                  Trả {money(intent.amount, intent.currency)}
                </button>
                {track.payment && (
                  <>
                    <button className="link tiny" disabled={busy} onClick={() => payWith(track.payment!.idempotencyKey)}>
                      Trả lại với cùng key
                    </button>
                    <button className="link tiny" disabled={busy} onClick={() => payWith(newKey())}>
                      Trả lại với key mới
                    </button>
                  </>
                )}
              </div>
              {charge && (
                <div className="tiny muted">
                  charge {charge.id.slice(0, 8)} · outcome {charge.outcome} · webhook {charge.webhookDelivered ? 'đã gửi' : 'chưa gửi'}
                </div>
              )}
              <p className="tiny muted">
                Nhánh hoàn tiền: dựng stack với <code>PAYMENT_TIMEOUT=20s</code>, đợi đơn bị huỷ rồi trả thành công.
              </p>
            </div>
          )}
          {msg && <div className={`alert ${msg.kind}`}>{msg.text}</div>}
        </aside>
      </div>
    </div>
  );
}
