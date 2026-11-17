import { useState, useSyncExternalStore } from 'react';
import { apiLog } from '../api';
import { fmtTime } from '../lib';

/** Ngăn kéo dưới đáy trang: mọi lời gọi REST kèm status, độ trễ và x-request-id để grep log. */
export function ApiLogPanel() {
  const entries = useSyncExternalStore(apiLog.subscribe, apiLog.get);
  const [open, setOpen] = useState(false);
  const [hidePoll, setHidePoll] = useState(true);
  const shown = hidePoll ? entries.filter((e) => e.actor !== 'poll' && e.actor !== 'health') : entries;
  const last = shown[0];

  return (
    <div className={`apilog ${open ? 'open' : ''}`}>
      <div className="apilog-bar" onClick={() => setOpen(!open)}>
        <b>{open ? '▾' : '▸'} Nhật ký API</b>
        <span className="muted">{shown.length} lời gọi</span>
        {last && !open && (
          <span className="apilog-last">
            <StatusBadge status={last.status} /> {last.method} {last.path} · {Math.round(last.ms)} ms
          </span>
        )}
      </div>
      {open && (
        <div className="apilog-body">
          <div className="apilog-tools">
            <label>
              <input type="checkbox" checked={hidePoll} onChange={(e) => setHidePoll(e.target.checked)} /> Ẩn lời gọi
              polling
            </label>
            <button className="btn small ghost" onClick={() => apiLog.clear()}>
              Xoá
            </button>
            <span className="muted">
              Grep log: <code>docker compose -f deploy/docker-compose.yml logs gateway order inventory | grep &lt;request-id&gt;</code>
            </span>
          </div>
          <table className="table compact">
            <thead>
              <tr>
                <th>Giờ</th>
                <th>Ai</th>
                <th>Service</th>
                <th>Request</th>
                <th>Status</th>
                <th>ms</th>
                <th>x-request-id</th>
                <th>Idempotency-Key</th>
              </tr>
            </thead>
            <tbody>
              {shown.map((e) => (
                <tr key={e.seq}>
                  <td>{fmtTime(e.at)}</td>
                  <td>{e.actor}</td>
                  <td>{e.service === 'gateway' ? ':3000' : ':3001'}</td>
                  <td className="mono">
                    {e.method} {e.path}
                  </td>
                  <td>
                    <StatusBadge status={e.status} />
                  </td>
                  <td>{Math.round(e.ms)}</td>
                  <td className="mono">
                    {e.requestId ? (
                      <button className="link mono" title="Copy" onClick={() => navigator.clipboard?.writeText(e.requestId!)}>
                        {e.requestId}
                      </button>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td className="mono muted">{e.idempotencyKey?.slice(0, 8) ?? ''}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export function StatusBadge({ status }: { status: number }) {
  const cls = status === 0 ? 'err' : status < 300 ? 'ok' : status < 500 ? 'warn' : 'err';
  return <span className={`badge ${cls}`}>{status || 'ERR'}</span>;
}
