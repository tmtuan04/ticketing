import { useEffect, useState } from 'react';
import { api, errorText, type EventView } from '../api';
import { money, navigate } from '../lib';
import { useSession } from '../session';

function dateParts(iso: string | null) {
  if (!iso) return { d: '--', m: '---', t: '' };
  const dt = new Date(iso);
  return {
    d: String(dt.getDate()).padStart(2, '0'),
    m: `th${dt.getMonth() + 1} ${String(dt.getFullYear()).slice(2)}`,
    t: dt.toLocaleTimeString('vi-VN', { hour: '2-digit', minute: '2-digit', hour12: false }),
  };
}

export function EventsPage() {
  const session = useSession();
  const [events, setEvents] = useState<EventView[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.events
      .list()
      .then((r) => setEvents(r.events))
      .catch((e) => setError(errorText(e)));
  }, []);

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">{events ? `${events.length} sự kiện` : '…'}</div>
          <h1>Đang mở bán</h1>
        </div>
        {session?.user.role === 'admin' && (
          <button className="btn" onClick={() => navigate('/admin')}>
            Tạo sự kiện
          </button>
        )}
      </div>
      {error && <div className="alert err">{error}</div>}
      {events === null && !error && <p className="muted">Đang tải…</p>}
      {events?.length === 0 && (
        <div className="card empty">
          Chưa có sự kiện nào được publish.{' '}
          {session?.user.role === 'admin' ? (
            <button className="link" onClick={() => navigate('/admin')}>
              Tạo một cái
            </button>
          ) : (
            'Đăng nhập admin để tạo.'
          )}
        </div>
      )}
      {events && events.length > 0 && (
        <div className="listing">
          {events.map((e) => {
            const { d, m, t } = dateParts(e.startsAt);
            const min = Math.min(...e.tiers.map((x) => x.priceAmount));
            return (
              <button className="listing-row" key={e.id} onClick={() => navigate(`/events/${e.id}`)}>
                <div className="date-block">
                  <span className="d">{d}</span>
                  <span className="m">{m}</span>
                </div>
                <div>
                  <div className="listing-name">{e.name}</div>
                  <div className="listing-meta">
                    {e.venue?.name} · {t}
                    {e.description && <> · {e.description}</>}
                  </div>
                </div>
                <div className="listing-price">
                  {e.tiers.length > 0 && (
                    <>
                      <small>từ</small>
                      {money(min, e.tiers[0].currency)}
                    </>
                  )}
                </div>
                <span className="listing-go">Chọn ghế</span>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
