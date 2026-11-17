import { useCallback, useEffect, useState, type FormEvent } from 'react';
import { api, errorText, type EventView } from '../api';
import { fmtDateTime, money, navigate } from '../lib';
import { useSession } from '../session';

type TierRow = { name: string; priceAmount: number };
type BlockRow = { section: string; rowCount: number; seatsPerRow: number; tier: number };

const inDays = (d: number) => {
  const t = new Date(Date.now() + d * 86400_000);
  t.setMinutes(0, 0, 0);
  return t.toISOString().slice(0, 16);
};

export function AdminPage() {
  const session = useSession();
  const [name, setName] = useState('Flash Sale Concert');
  const [description, setDescription] = useState('Demo: nhiều người tranh cùng một ghế');
  const [venue, setVenue] = useState('Arena Hà Nội');
  const [startsAt, setStartsAt] = useState(inDays(30));
  const [tiers, setTiers] = useState<TierRow[]>([
    { name: 'VIP', priceAmount: 2_000_000 },
    { name: 'Thường', priceAmount: 800_000 },
  ]);
  const [blocks, setBlocks] = useState<BlockRow[]>([
    { section: 'A', rowCount: 3, seatsPerRow: 10, tier: 0 },
    { section: 'B', rowCount: 4, seatsPerRow: 12, tier: 1 },
  ]);
  const [publish, setPublish] = useState(true);
  const [steps, setSteps] = useState<{ text: string; ok: boolean | null }[]>([]);
  const [busy, setBusy] = useState(false);
  const [events, setEvents] = useState<EventView[]>([]);
  const [listError, setListError] = useState<string | null>(null);

  const isAdmin = session?.user.role === 'admin';

  const loadEvents = useCallback(() => {
    if (!isAdmin) return;
    api.admin.listEvents().then(
      (r) => setEvents(r.events),
      (e) => setListError(errorText(e)),
    );
  }, [isAdmin]);
  useEffect(loadEvents, [loadEvents]);

  if (!isAdmin) {
    return (
      <div className="card empty">
        Trang này cần tài khoản admin.{' '}
        <button className="link" onClick={() => navigate('/login')}>
          Đăng nhập admin@ticketing.local
        </button>
      </div>
    );
  }

  async function create(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    const log: { text: string; ok: boolean | null }[] = [];
    const push = (text: string, ok: boolean | null = null) => {
      log.push({ text, ok });
      setSteps([...log]);
    };
    const done = (ok: boolean, text?: string) => {
      log[log.length - 1] = { text: text ?? log[log.length - 1].text, ok };
      setSteps([...log]);
    };
    try {
      push('POST /admin/venues');
      const v = await api.admin.createVenue(venue, '');
      done(true, `Venue ${v.name}`);

      push('POST /admin/events (draft, kèm hạng vé)');
      const ev = await api.admin.createEvent({
        name,
        description,
        venueId: v.id,
        startsAt: new Date(startsAt).toISOString(),
        tiers: tiers.map((t) => ({ name: t.name, priceAmount: t.priceAmount, currency: 'VND' })),
      });
      done(true, `Event ${ev.name} (draft)`);

      push('POST /admin/events/:id/seats (generate_series, idempotent)');
      const seats = await api.admin.generateSeats(
        ev.id,
        // tên hạng vé là duy nhất trong event (catalog kiểm tra), nên ghép theo tên thay vì thứ tự trả về
        blocks.map((b) => ({
          section: b.section,
          rowCount: b.rowCount,
          seatsPerRow: b.seatsPerRow,
          tierId: ev.tiers.find((t) => t.name === tiers[b.tier].name)!.id,
        })),
      );
      done(true, `Sinh ${seats.seatCount} ghế`);

      if (publish) {
        push('POST /admin/events/:id/publish → catalog gọi inventory.InitEventInventory');
        await api.admin.publish(ev.id);
        done(true, 'Đã publish: inventory có bản ghi cho từng ghế');
      }
      loadEvents();
    } catch (err) {
      done(false, `${log[log.length - 1]?.text}: ${errorText(err)}`);
    } finally {
      setBusy(false);
    }
  }

  async function doPublish(id: string) {
    try {
      await api.admin.publish(id);
      loadEvents();
    } catch (err) {
      setListError(errorText(err));
    }
  }

  const seatTotal = blocks.reduce((s, b) => s + b.rowCount * b.seatsPerRow, 0);

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">admin</div>
          <h1>Quản trị sự kiện</h1>
        </div>
      </div>
      <div className="layout-2">
        <form className="card form" onSubmit={create}>
          <h3>Sự kiện mới</h3>
          <div className="form-grid">
            <label>
              Tên event
              <input value={name} onChange={(e) => setName(e.target.value)} required />
            </label>
            <label>
              Venue
              <input value={venue} onChange={(e) => setVenue(e.target.value)} required />
            </label>
            <label>
              Bắt đầu
              <input type="datetime-local" value={startsAt} onChange={(e) => setStartsAt(e.target.value)} required />
            </label>
            <label>
              Mô tả
              <input value={description} onChange={(e) => setDescription(e.target.value)} />
            </label>
          </div>

          <h4>Hạng vé (VND)</h4>
          {tiers.map((t, i) => (
            <div className="inline-row tier-row" key={i}>
              <input
                value={t.name}
                onChange={(e) => setTiers(tiers.map((x, j) => (j === i ? { ...x, name: e.target.value } : x)))}
                required
              />
              <input
                type="number"
                min={0}
                step={1000}
                value={t.priceAmount}
                onChange={(e) => setTiers(tiers.map((x, j) => (j === i ? { ...x, priceAmount: Number(e.target.value) } : x)))}
              />
              <button
                type="button"
                className="btn ghost small"
                disabled={tiers.length === 1}
                onClick={() => {
                  setTiers(tiers.filter((_, j) => j !== i));
                  setBlocks(blocks.map((b) => ({ ...b, tier: Math.min(b.tier, tiers.length - 2) })));
                }}
              >
                Xoá
              </button>
            </div>
          ))}
          <button type="button" className="btn ghost small" onClick={() => setTiers([...tiers, { name: `Hạng ${tiers.length + 1}`, priceAmount: 500_000 }])}>
            + Hạng vé
          </button>

          <h4>Khu ghế</h4>
          <div className="inline-row head">
            <span>Khu</span>
            <span>Số hàng</span>
            <span>Ghế/hàng</span>
            <span>Hạng</span>
            <span />
          </div>
          {blocks.map((b, i) => (
            <div className="inline-row" key={i}>
              <input value={b.section} onChange={(e) => setBlocks(blocks.map((x, j) => (j === i ? { ...x, section: e.target.value } : x)))} required />
              <input
                type="number"
                min={1}
                max={500}
                value={b.rowCount}
                onChange={(e) => setBlocks(blocks.map((x, j) => (j === i ? { ...x, rowCount: Number(e.target.value) } : x)))}
              />
              <input
                type="number"
                min={1}
                max={500}
                value={b.seatsPerRow}
                onChange={(e) => setBlocks(blocks.map((x, j) => (j === i ? { ...x, seatsPerRow: Number(e.target.value) } : x)))}
              />
              <select value={b.tier} onChange={(e) => setBlocks(blocks.map((x, j) => (j === i ? { ...x, tier: Number(e.target.value) } : x)))}>
                {tiers.map((t, j) => (
                  <option key={j} value={j}>
                    {t.name}
                  </option>
                ))}
              </select>
              <button type="button" className="btn ghost small" disabled={blocks.length === 1} onClick={() => setBlocks(blocks.filter((_, j) => j !== i))}>
                Xoá
              </button>
            </div>
          ))}
          <button
            type="button"
            className="btn ghost small"
            onClick={() => setBlocks([...blocks, { section: String.fromCharCode(65 + blocks.length), rowCount: 2, seatsPerRow: 10, tier: 0 }])}
          >
            + Khu
          </button>

          <label className="toggle">
            <input type="checkbox" checked={publish} onChange={(e) => setPublish(e.target.checked)} /> Publish ngay ({seatTotal} ghế)
          </label>
          <button className="btn primary" disabled={busy}>
            {busy ? 'Đang tạo…' : 'Tạo event'}
          </button>

          {steps.length > 0 && (
            <ol className="progress">
              {steps.map((s, i) => (
                <li key={i} className={s.ok === null ? 'run' : s.ok ? 'ok' : 'err'}>
                  {s.text}
                </li>
              ))}
            </ol>
          )}
        </form>

        <aside className="side">
          <div className="card">
            <h3>Tất cả (gồm draft)</h3>
            {listError && <div className="alert err">{listError}</div>}
            {events.length === 0 && <p className="muted">Chưa có event.</p>}
            <ul className="event-list">
              {events.map((e) => (
                <li key={e.id}>
                  <div>
                    <b>{e.name}</b>
                    <div>
                      <span className={`status st-${e.status}`}>{e.status}</span>
                    </div>
                    <div className="muted tiny">
                      {fmtDateTime(e.startsAt)} · {e.tiers.map((t) => `${t.name} ${money(t.priceAmount, t.currency)}`).join(' · ')}
                    </div>
                  </div>
                  {e.status === 'draft' ? (
                    <button className="btn small" onClick={() => doPublish(e.id)}>
                      Publish
                    </button>
                  ) : (
                    <button className="btn small ghost" onClick={() => navigate(`/events/${e.id}`)}>
                      Mở
                    </button>
                  )}
                </li>
              ))}
            </ul>
          </div>
        </aside>
      </div>
    </div>
  );
}
