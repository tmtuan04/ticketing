import { useEffect, useState } from 'react';
import { api, errorText, type OrderView } from '../api';
import { fmtDateTime, money, navigate } from '../lib';
import { loadOrders, useSession } from '../session';

/** Gateway không có API liệt kê đơn, nên danh sách lấy từ các id đơn trình duyệt này đã tạo. */
export function OrdersPage() {
  const session = useSession();
  const [orders, setOrders] = useState<(OrderView | { id: string; error: string })[] | null>(null);

  useEffect(() => {
    if (!session) return;
    const saved = loadOrders(session.user.id);
    Promise.all(
      saved.map((o) => api.orders.get(o.id, 'poll').catch((e) => ({ id: o.id, error: errorText(e) }))),
    ).then(setOrders);
  }, [session?.user.id]);

  if (!session) {
    return (
      <div className="card empty">
        <button className="link" onClick={() => navigate('/login')}>
          Đăng nhập
        </button>{' '}
        để xem đơn.
      </div>
    );
  }

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">tạo từ trình duyệt này</div>
          <h1>Đơn của tôi</h1>
        </div>
      </div>
      {orders === null && <p className="muted">Đang tải…</p>}
      {orders?.length === 0 && (
        <div className="card empty">
          Chưa có đơn nào.{' '}
          <button className="link" onClick={() => navigate('/events')}>
            Chọn ghế để mua
          </button>
        </div>
      )}
      {orders && orders.length > 0 && (
        <div className="card">
          <table className="table">
            <thead>
              <tr>
                <th>Đơn</th>
                <th>Trạng thái</th>
                <th>Số tiền</th>
                <th>Số ghế</th>
                <th>Tạo lúc</th>
              </tr>
            </thead>
            <tbody>
              {orders.map((o) =>
                'error' in o ? (
                  <tr key={o.id}>
                    <td className="mono">{o.id.slice(0, 8)}</td>
                    <td colSpan={4} className="muted">
                      {o.error}
                    </td>
                  </tr>
                ) : (
                  <tr key={o.id} className="clickable" onClick={() => navigate(`/orders/${o.id}`)}>
                    <td className="mono">#{o.id.slice(0, 8)}</td>
                    <td>
                      <span className={`status st-${o.status}`}>{o.status}</span>
                      {o.failureReason && <span className="muted tiny"> {o.failureReason}</span>}
                    </td>
                    <td className="num">{money(o.amount, o.currency)}</td>
                    <td className="num">{o.seatIds.length}</td>
                    <td className="num">{fmtDateTime(o.createdAt)}</td>
                  </tr>
                ),
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
