import { useEffect, useState } from 'react';
import { api } from './api';
import { ApiLogPanel } from './components/ApiLogPanel';
import { navigate, useInterval, useRoute } from './lib';
import { AdminPage } from './pages/AdminPage';
import { EventPage } from './pages/EventPage';
import { EventsPage } from './pages/EventsPage';
import { LoginPage } from './pages/LoginPage';
import { OrderPage } from './pages/OrderPage';
import { OrdersPage } from './pages/OrdersPage';
import { RacePage } from './pages/RacePage';
import { getSession, setSession, useSession } from './session';

const NAV = [
  { path: 'events', label: 'Sự kiện' },
  { path: 'orders', label: 'Đơn của tôi' },
  { path: 'race', label: 'Đua ghế' },
  { path: 'admin', label: 'Quản trị' },
];

export function App() {
  const route = useRoute();
  const session = useSession();
  const [section = 'events', id] = route;

  let page;
  if (section === 'login') page = <LoginPage />;
  else if (section === 'events' && id) page = <EventPage eventId={id} key={id} />;
  else if (section === 'orders' && id) page = <OrderPage orderId={id} key={id} />;
  else if (section === 'orders') page = <OrdersPage />;
  else if (section === 'race') page = <RacePage />;
  else if (section === 'admin') page = <AdminPage />;
  else page = <EventsPage />;

  async function logout() {
    const s = getSession();
    setSession(null);
    if (s) await api.auth.logout(s.refreshToken).catch(() => {});
    navigate('/events');
  }

  return (
    <>
      <header className="topbar">
        <button className="brand" onClick={() => navigate('/events')}>
          Ticketing <span>flash sale · dev</span>
        </button>
        <nav>
          {NAV.map((n) => (
            <button key={n.path} className={section === n.path ? 'active' : ''} onClick={() => navigate(`/${n.path}`)}>
              {n.label}
            </button>
          ))}
        </nav>
        <div className="topbar-right">
          <Health />
          <a className="console-link" href="http://localhost:9080" target="_blank" rel="noreferrer" title="Redpanda Console">
            Redpanda Console
          </a>
          {session ? (
            <span className="user">
              <span className="user-email">{session.user.email}</span>
              {session.user.role === 'admin' && <span className="badge warn">admin</span>}
              <button className="btn small" onClick={logout}>
                Đăng xuất
              </button>
            </span>
          ) : (
            <button className="btn small" onClick={() => navigate('/login')}>
              Đăng nhập
            </button>
          )}
        </div>
      </header>
      <main className="container">{page}</main>
      <ApiLogPanel />
    </>
  );
}

/** Đèn trạng thái của gateway (:3000) và payment (:3001), gọi /healthz mỗi 5 giây. */
function Health() {
  const [state, setState] = useState<{ gateway?: boolean; payment?: boolean }>({});
  const check = async () => {
    const [g, p] = await Promise.allSettled([api.health.gateway(), api.health.payment()]);
    setState({ gateway: g.status === 'fulfilled', payment: p.status === 'fulfilled' });
  };
  useInterval(check, 5000);
  useEffect(() => {
    void check();
  }, []);
  return (
    <span className="health">
      <span className={`dot ${state.gateway === undefined ? '' : state.gateway ? 'up' : 'down'}`} /> :3000
      <span className={`dot ${state.payment === undefined ? '' : state.payment ? 'up' : 'down'}`} /> :3001
    </span>
  );
}
