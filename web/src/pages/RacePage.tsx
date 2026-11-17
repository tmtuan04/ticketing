import { useCallback, useEffect, useState } from 'react';
import { api, ApiError, errorText, newKey, type EventView, type SeatView } from '../api';
import { SeatMap } from '../components/SeatMap';
import { navigate, seatLabel, useInterval } from '../lib';
import { loadBots, saveBots, type Bot } from '../session';

type Result = {
  bot: string;
  status: number;
  ms: number;
  holdId?: string;
  unavailable?: string[];
  error?: string;
  token?: string;
};

const BOT_PASSWORD = 'bot-password-123';
const TOKEN_MAX_AGE = 10 * 60 * 1000; // access token sống 15 phút, làm mới sớm hơn

/**
 * Nhiều tài khoản bot cùng bắn POST /holds vào cùng một nhóm ghế trong cùng một tick: chỉ một
 * request được 201, phần còn lại nhận 409. Đây là bài toán trung tâm của project, nhìn bằng mắt.
 */
export function RacePage() {
  const [events, setEvents] = useState<EventView[]>([]);
  const [eventId, setEventId] = useState('');
  const [seats, setSeats] = useState<SeatView[]>([]);
  const [target, setTarget] = useState<Set<string>>(new Set());
  const [botCount, setBotCount] = useState(5);
  const [bots, setBots] = useState<(Bot & { tokenAt?: number })[]>(() => loadBots());
  const [results, setResults] = useState<Result[] | null>(null);
  const [phase, setPhase] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const event = events.find((e) => e.id === eventId);
  const seatById = new Map(seats.map((s) => [s.seatId, s]));

  useEffect(() => {
    api.events.list().then(
      (r) => {
        setEvents(r.events);
        if (r.events[0]) setEventId((id) => id || r.events[0].id);
      },
      (e) => setError(errorText(e)),
    );
  }, []);

  const loadSeats = useCallback(async () => {
    if (!eventId) return;
    const r = await api.events.seats(eventId);
    setSeats(r.seats);
  }, [eventId]);

  useEffect(() => {
    setTarget(new Set());
    setResults(null);
    loadSeats().catch(() => {});
  }, [loadSeats]);
  useInterval(() => loadSeats().catch(() => {}), eventId ? 2000 : null);

  function persist(next: (Bot & { tokenAt?: number })[]) {
    setBots(next);
    saveBots(next);
  }

  /** Đăng ký/đăng nhập đủ số bot. Tuần tự để thấy tiến độ; tốn quota bucket auth (10/phút/IP). */
  async function prepareBots(n: number) {
    const list = [...bots];
    const suffix = Math.random().toString(36).slice(2, 7);
    for (let i = 0; i < n; i++) {
      let b = list[i] ?? { name: `bot-${i + 1}`, email: `bot-${i + 1}-${suffix}@race.local`, password: BOT_PASSWORD };
      if (b.token && b.tokenAt && Date.now() - b.tokenAt < TOKEN_MAX_AGE) continue;
      setPhase(`Chuẩn bị ${b.name} (${i + 1}/${n})…`);
      let token: string;
      try {
        token = (await api.auth.login(b.email, b.password, b.name)).accessToken;
      } catch (err) {
        if (!(err instanceof ApiError) || err.status !== 401) throw err;
        token = (await api.auth.register(b.email, b.password, b.name)).accessToken;
      }
      b = { ...b, token, tokenAt: Date.now() };
      list[i] = b;
      persist([...list]);
    }
    return list.slice(0, n);
  }

  /** Nhả hold của các bot thắng ở lượt trước (mỗi user chỉ được 1 hold active mỗi event). */
  async function cleanup(prev: Result[] | null) {
    const winners = prev?.filter((r) => r.holdId && r.token) ?? [];
    if (winners.length === 0) return;
    setPhase('Nhả hold của lượt trước…');
    await Promise.allSettled(winners.map((w) => api.holds.release(w.holdId!, w.token!, w.bot)));
  }

  async function race() {
    setError(null);
    try {
      await cleanup(results);
      setResults(null);
      const ready = await prepareBots(botCount);
      setPhase(`${ready.length} bot cùng bắn POST /events/:id/holds…`);
      const seatIds = [...target];
      // Tạo mọi promise trong cùng một tick để các request đi gần như đồng thời
      const out = await Promise.all(
        ready.map(async (b): Promise<Result> => {
          const start = performance.now();
          try {
            const h = await api.holds.create(eventId, seatIds, newKey(), b.token!, b.name);
            return { bot: b.name, status: 201, ms: performance.now() - start, holdId: h.id, token: b.token };
          } catch (err) {
            const e = err as ApiError;
            return {
              bot: b.name,
              status: e.status ?? 0,
              ms: performance.now() - start,
              unavailable: e.body?.unavailableSeatIds,
              error: e.message,
            };
          }
        }),
      );
      setResults(out);
      await loadSeats();
    } catch (err) {
      setError(errorText(err));
    } finally {
      setPhase(null);
    }
  }

  async function releaseAll() {
    await cleanup(results);
    setResults(null);
    setPhase(null);
    await loadSeats();
  }

  function toggle(s: SeatView) {
    const next = new Set(target);
    if (next.has(s.seatId)) next.delete(s.seatId);
    else next.add(s.seatId);
    setTarget(next);
  }

  const winners = results?.filter((r) => r.status === 201) ?? [];
  const losers = results?.filter((r) => r.status === 409) ?? [];
  const others = results?.filter((r) => r.status !== 201 && r.status !== 409) ?? [];
  const winnerSeats = new Set(winners.length ? [...target] : []);

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">kiểm tra chống bán vượt</div>
          <h1>Đua ghế</h1>
          <p>Nhiều bot cùng gửi POST /holds cho một nhóm ghế trong cùng một lúc. Đúng thì chỉ một bot được giữ.</p>
        </div>
      </div>

      {events.length === 0 && !error && (
        <div className="card empty">
          Chưa có event đã publish.{' '}
          <button className="link" onClick={() => navigate('/admin')}>
            Tạo event ở trang Quản trị
          </button>
        </div>
      )}

      {events.length > 0 && (
        <div className="layout-2">
          <div className="card">
            <div className="row-gap">
              <label className="field inline">
                Event
                <select value={eventId} onChange={(e) => setEventId(e.target.value)}>
                  {events.map((e) => (
                    <option key={e.id} value={e.id}>
                      {e.name}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <SeatMap seats={seats} tiers={event?.tiers} selected={target} mine={winnerSeats} onToggle={toggle} />
          </div>

          <aside className="side">
            <div className="card">
              <h3>Ghế mục tiêu</h3>
              <div className="chips">
                {target.size === 0 && <span className="muted">Bấm chọn trên sơ đồ.</span>}
                {[...target].map((id) => (
                  <span key={id} className="chip">
                    {seatById.get(id) ? seatLabel(seatById.get(id)!) : id.slice(0, 8)}
                  </span>
                ))}
              </div>
              <label className="field">
                Số bot: <b>{botCount}</b>
                <input type="range" min={2} max={10} value={botCount} onChange={(e) => setBotCount(Number(e.target.value))} />
              </label>
              <button className="btn primary block" disabled={!!phase || target.size === 0} onClick={race}>
                Cho bot đua
              </button>
              {results && winners.length > 0 && (
                <button className="btn ghost block" disabled={!!phase} onClick={releaseAll}>
                  Nhả ghế của bot thắng
                </button>
              )}
              {phase && <div className="alert info">{phase}</div>}
              {error && <div className="alert err">{error}</div>}
              <p className="tiny muted">
                Lần đầu mỗi bot phải đăng ký, tốn quota <code>auth</code> 10/phút/IP. Cần nhiều hơn thì{' '}
                <code>RATE_LIMIT_AUTH=100 make up</code>.
              </p>
            </div>

            {results && (
              <div className="card">
                <h3>Kết quả</h3>
                <div className="race-summary">
                  <div className="stat ok">
                    <b>{winners.length}</b>
                    <span>201 thắng</span>
                  </div>
                  <div className="stat warn">
                    <b>{losers.length}</b>
                    <span>409 thua</span>
                  </div>
                  <div className="stat">
                    <b>{others.length}</b>
                    <span>khác</span>
                  </div>
                </div>
                <div className="bots">
                  {results
                    .slice()
                    .sort((a, b) => a.ms - b.ms)
                    .map((r) => (
                      <div key={r.bot} className={`bot ${r.status === 201 ? 'win' : r.status === 409 ? 'lose' : 'other'}`}>
                        <span className="bot-name">{r.bot}</span>
                        <span className="badge">{r.status || 'ERR'}</span>
                        <span className="muted">{Math.round(r.ms)} ms</span>
                        <span className="tiny">
                          {r.holdId
                            ? `hold ${r.holdId.slice(0, 8)}`
                            : r.unavailable?.length
                              ? `bị chiếm: ${r.unavailable.map((id) => (seatById.get(id) ? seatLabel(seatById.get(id)!) : '?')).join(', ')}`
                              : r.error}
                        </span>
                      </div>
                    ))}
                </div>
                <p className="tiny">
                  {results.length} request cùng lúc, {winners.length} hold được tạo.{' '}
                  {winners.length <= 1 ? 'Không bán vượt.' : 'Có bán vượt!'}
                  {others.some((r) => r.status === 429) && ' Có 429: bot vượt rate limit bucket hold (10/phút/user).'}
                </p>
              </div>
            )}
          </aside>
        </div>
      )}
    </div>
  );
}
