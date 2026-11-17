import { useState, type FormEvent } from 'react';
import { api, errorText } from '../api';
import { navigate } from '../lib';
import { setSession } from '../session';

export function LoginPage() {
  const [mode, setMode] = useState<'login' | 'register'>('login');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const t = mode === 'login' ? await api.auth.login(email, password) : await api.auth.register(email, password);
      setSession({ accessToken: t.accessToken, refreshToken: t.refreshToken, user: t.user! });
      navigate(t.user?.role === 'admin' ? '/admin' : '/events');
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="narrow">
      <div className="card">
        <div className="tabs">
          <button className={mode === 'login' ? 'active' : ''} onClick={() => setMode('login')}>
            Đăng nhập
          </button>
          <button className={mode === 'register' ? 'active' : ''} onClick={() => setMode('register')}>
            Đăng ký
          </button>
        </div>
        <form onSubmit={submit} className="form">
          <label>
            Email
            <input type="email" required value={email} onChange={(e) => setEmail(e.target.value)} autoFocus />
          </label>
          <label>
            Mật khẩu {mode === 'register' && <span className="muted">(tối thiểu 8 ký tự)</span>}
            <input
              type="password"
              required
              minLength={mode === 'register' ? 8 : 1}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </label>
          {error && <div className="alert err">{error}</div>}
          <button className="btn primary" disabled={busy}>
            {busy ? 'Đang gửi…' : mode === 'login' ? 'Đăng nhập' : 'Tạo tài khoản'}
          </button>
        </form>
        <div className="hint">
          Admin dev:{' '}
          <button
            className="link"
            onClick={() => {
              setMode('login');
              setEmail('admin@ticketing.local');
              setPassword('admin12345');
            }}
          >
            admin@ticketing.local / admin12345
          </button>
          <br />
          Gặp 429 là chạm rate limit auth (10/phút/IP), đợi một phút.
        </div>
      </div>
    </div>
  );
}
