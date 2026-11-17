import { useCallback, useEffect, useState } from 'react';
import { api, ApiError, errorText, newKey, type EventView, type HoldView, type SeatView } from '../api';
import { SeatMap } from '../components/SeatMap';
import { fmtCountdown, fmtDateTime, money, navigate, seatLabel, useInterval, useNow } from '../lib';
import { addOrder, loadHold, saveHold, useSession } from '../session';

export function EventPage({ eventId }: { eventId: string }) {
  const session = useSession();
  const userId = session?.user.id;
  const [event, setEvent] = useState<EventView | null>(null);
  const [seats, setSeats] = useState<SeatView[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [rejected, setRejected] = useState<Set<string>>(new Set());
  const [hold, setHold] = useState<HoldView | null>(null);
  const [holdKey, setHoldKey] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<{ kind: 'ok' | 'err' | 'info'; text: string } | null>(null);
  const now = useNow();

  const loadSeats = useCallback(async () => {
    const r = await api.events.seats(eventId);
    setSeats(r.seats);
  }, [eventId]);

  useEffect(() => {
    api.events.get(eventId).then(setEvent, (e) => setMsg({ kind: 'err', text: errorText(e) }));
    loadSeats().catch(() => {});
  }, [eventId, loadSeats]);

  // Khôi phục hold đang active của user cho event này (nếu còn)
  useEffect(() => {
    setHold(null);
    if (!userId) return;
    const saved = loadHold(userId, eventId);
    if (!saved) return;
    api.holds.get(saved.id).then(
      (h) => {
        if (h.status === 'active') {
          setHold(h);
          setHoldKey(saved.idempotencyKey);
        } else saveHold(userId, eventId, null);
      },
      () => saveHold(userId, eventId, null),
    );
  }, [userId, eventId]);

  useInterval(() => loadSeats().catch(() => {}), autoRefresh ? 2000 : null);

  const tierById = new Map(event?.tiers.map((t) => [t.id, t]) ?? []);
  const seatById = new Map(seats.map((s) => [s.seatId, s]));
  const total = [...selected].reduce((sum, id) => sum + (tierById.get(seatById.get(id)?.tierId ?? '')?.priceAmount ?? 0), 0);
  const currency = event?.tiers[0]?.currency ?? 'VND';
  const holdLeft = hold?.expiresAt ? new Date(hold.expiresAt).getTime() - now : 0;
  const holdExpired = hold !== null && holdLeft <= 0;

  function toggle(s: SeatView) {
    if (hold) return;
    const next = new Set(selected);
    if (next.has(s.seatId)) next.delete(s.seatId);
    else next.add(s.seatId);
    setSelected(next);
    setRejected(new Set());
  }

  async function doHold(key: string, seatIds: string[]) {
    if (!userId) return navigate('/login');
    setBusy(true);
    setMsg(null);
    try {
      const h = await api.holds.create(eventId, seatIds, key);
      setHold(h);
      setHoldKey(key);
      saveHold(userId, eventId, { id: h.id, idempotencyKey: key });
      setSelected(new Set());
      setMsg({
        kind: 'ok',
        text:
          key === holdKey
            ? `Cùng Idempotency-Key: server trả lại đúng hold #${h.id.slice(0, 8)}, không giữ thêm ghế.`
            : 'Đã giữ ghế.',
      });
    } catch (err) {
      if (err instanceof ApiError && err.status === 409 && err.body?.unavailableSeatIds?.length) {
        const ids: string[] = err.body.unavailableSeatIds;
        setRejected(new Set(ids));
        setSelected(new Set([...selected].filter((id) => !ids.includes(id))));
        setMsg({
          kind: 'err',
          text: `Ghế ${ids.map((id) => (seatById.get(id) ? seatLabel(seatById.get(id)!) : id)).join(', ')} vừa có người khác giữ (409).`,
        });
      } else {
        setMsg({ kind: 'err', text: errorText(err) });
      }
    } finally {
      setBusy(false);
      loadSeats().catch(() => {});
    }
  }

  async function release() {
    if (!hold || !userId) return;
    setBusy(true);
    try {
      const h = await api.holds.release(hold.id);
      setMsg({ kind: 'info', text: `Đã nhả ghế (hold ${h.status}).` });
      setHold(null);
      saveHold(userId, eventId, null);
    } catch (err) {
      setMsg({ kind: 'err', text: errorText(err) });
    } finally {
      setBusy(false);
      loadSeats().catch(() => {});
    }
  }

  async function createOrder() {
    if (!hold || !userId) return;
    setBusy(true);
    try {
      const o = await api.orders.create(hold.id, newKey());
      addOrder(userId, { id: o.id, eventId, createdAt: o.createdAt ?? new Date().toISOString() });
      saveHold(userId, eventId, null);
      navigate(`/orders/${o.id}`);
    } catch (err) {
      setMsg({ kind: 'err', text: errorText(err) });
      setBusy(false);
    }
  }

  return (
    <div>
      <button className="back" onClick={() => navigate('/events')}>
        ← Tất cả sự kiện
      </button>
      <div className="page-head">
        <div>
          <div className="eyebrow">
            {event?.venue?.name} · {fmtDateTime(event?.startsAt)}
          </div>
          <h1>{event?.name ?? '…'}</h1>
        </div>
        <label className="toggle">
          <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
          Tự làm mới
        </label>
      </div>

      <div className="layout-2">
        <div className="card">
          <SeatMap
            seats={seats}
            tiers={event?.tiers}
            selected={selected}
            mine={new Set(hold?.seatIds ?? [])}
            rejected={rejected}
            onToggle={hold ? undefined : toggle}
          />
        </div>

        <aside className="side">
          {!hold && (
            <div className="card">
              <h3>Ghế đã chọn</h3>
              {selected.size === 0 ? (
                <p className="muted">Chưa chọn ghế nào.</p>
              ) : (
                <>
                  <div className="chips">
                    {[...selected].map((id) => (
                      <span className="chip" key={id}>
                        {seatById.get(id) ? seatLabel(seatById.get(id)!) : id.slice(0, 8)}
                      </span>
                    ))}
                  </div>
                  <div className="total">
                    <span>Tạm tính</span>
                    <b>{money(total, currency)}</b>
                  </div>
                </>
              )}
              <button className="btn primary block" disabled={busy || selected.size === 0} onClick={() => doHold(newKey(), [...selected])}>
                {session ? 'Giữ ghế' : 'Đăng nhập để giữ ghế'}
              </button>
            </div>
          )}

          {hold && (
            <div className={`stub ${holdExpired ? 'expired' : ''}`}>
              <div className="stub-top">
                <div>
                  <div className="eyebrow">Giữ chỗ còn</div>
                  <div className="countdown">{fmtCountdown(holdLeft)}</div>
                </div>
                <span className="mono tiny muted">#{hold.id.slice(0, 8)}</span>
              </div>
              <div className="stub-perf" />
              <div className="chips">
                {hold.seatIds.map((id) => (
                  <span className="chip mine" key={id}>
                    {seatById.get(id) ? seatLabel(seatById.get(id)!) : id.slice(0, 8)}
                  </span>
                ))}
              </div>
              <div className="muted tiny">
                {holdExpired ? 'Đã hết hạn. Reaper sẽ trả ghế trong vài giây.' : `Hết hạn lúc ${fmtDateTime(hold.expiresAt)}`}
              </div>
              <div className="btn-col">
                <button className="btn primary" disabled={busy || holdExpired} onClick={createOrder}>
                  Tạo đơn
                </button>
                <button className="btn" disabled={busy} onClick={release}>
                  Nhả ghế
                </button>
                <button
                  className="link tiny"
                  disabled={busy || !holdKey}
                  onClick={() => holdKey && doHold(holdKey, hold.seatIds)}
                  title="Gửi lại POST /holds với cùng Idempotency-Key"
                >
                  Gửi lại request giữ ghế (cùng key)
                </button>
              </div>
            </div>
          )}

          {msg && <div className={`alert ${msg.kind}`}>{msg.text}</div>}
        </aside>
      </div>
    </div>
  );
}
