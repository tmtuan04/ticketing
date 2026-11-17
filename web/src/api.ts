// Client REST cho gateway (/api -> :3000) và payment (/pay -> :3001), kèm nhật ký mọi lời gọi
// để giao diện hiển thị status, độ trễ và x-request-id (grep được trong log của các service).

import { getSession, setSession, type Session } from './session';

// ---------- Kiểu dữ liệu trả về (khớp với các hàm to*View ở gateway/payment) ----------

export type User = { id: string; email: string; role: 'user' | 'admin'; createdAt: string };
export type TokenPair = {
  accessToken: string;
  refreshToken: string;
  tokenType: string;
  expiresIn: number;
  user?: User;
};
export type Tier = { id: string; name: string; priceAmount: number; currency: string };
export type EventView = {
  id: string;
  name: string;
  description: string;
  venue: { id: string; name: string; address: string } | null;
  startsAt: string | null;
  status: 'draft' | 'published' | 'unknown';
  tiers: Tier[];
};
export type SeatStatus = 'available' | 'held' | 'sold' | 'unknown';
export type SeatView = {
  seatId: string;
  section: string;
  row: number;
  number: number;
  tierId: string;
  status: SeatStatus;
};
export type HoldView = {
  id: string;
  eventId: string;
  userId: string;
  seatIds: string[];
  status: 'active' | 'released' | 'confirmed' | 'expired' | 'unknown';
  expiresAt: string | null;
  createdAt: string | null;
};
export type OrderStatus =
  | 'pending'
  | 'awaiting_payment'
  | 'paid'
  | 'confirmed'
  | 'cancelled'
  | 'refunding'
  | 'refunded'
  | 'unknown';
export type OrderView = {
  id: string;
  userId: string;
  eventId: string;
  holdId: string;
  seatIds: string[];
  amount: number;
  currency: string;
  status: OrderStatus;
  paymentDeadline: string | null;
  failureReason: string | null;
  createdAt: string | null;
  updatedAt: string | null;
};
export type IntentView = {
  id: string;
  orderId: string;
  userId: string;
  amount: number;
  currency: string;
  status: 'requires_payment' | 'succeeded' | 'failed' | 'refunded';
  paymentDeadline: string;
  failureReason: string | null;
  refundReason: string | null;
  refundedAt: string | null;
  createdAt: string;
  updatedAt: string;
};
export type ChargeView = {
  id: string;
  intentId: string;
  outcome: 'success' | 'fail' | 'timeout';
  amount: number;
  currency: string;
  webhookDelivered: boolean;
  createdAt: string;
};

// ---------- Nhật ký API ----------

export type ApiLogEntry = {
  seq: number;
  at: number;
  actor: string;
  service: 'gateway' | 'payment';
  method: string;
  path: string;
  status: number;
  ms: number;
  requestId: string | null;
  idempotencyKey?: string;
};

const MAX_LOG = 200;
let seq = 0;
let log: ApiLogEntry[] = [];
const logListeners = new Set<() => void>();

export const apiLog = {
  get: () => log,
  subscribe(fn: () => void) {
    logListeners.add(fn);
    return () => logListeners.delete(fn);
  },
  clear() {
    log = [];
    logListeners.forEach((fn) => fn());
  },
};

function record(e: Omit<ApiLogEntry, 'seq' | 'at'>) {
  log = [{ ...e, seq: ++seq, at: Date.now() }, ...log].slice(0, MAX_LOG);
  logListeners.forEach((fn) => fn());
}

// ---------- Lời gọi HTTP ----------

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly body: any,
    readonly requestId: string | null,
  ) {
    const msg = Array.isArray(body?.message) ? body.message.join('; ') : body?.message;
    super(msg || `HTTP ${status}`);
  }
}

type CallOptions = {
  body?: unknown;
  /** Token dùng cho lời gọi này. Không truyền thì dùng phiên đăng nhập hiện tại. null = không gửi. */
  token?: string | null;
  idempotencyKey?: string;
  /** tên hiển thị trong nhật ký, ví dụ "bot-3" */
  actor?: string;
};

async function raw<T>(
  service: 'gateway' | 'payment',
  method: string,
  path: string,
  opts: CallOptions,
): Promise<T> {
  const headers: Record<string, string> = {};
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json';
  if (opts.token) headers.Authorization = `Bearer ${opts.token}`;
  if (opts.idempotencyKey) headers['Idempotency-Key'] = opts.idempotencyKey;

  const base = service === 'gateway' ? '/api' : '/pay';
  const start = performance.now();
  let res: Response;
  try {
    res = await fetch(base + path, {
      method,
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
    });
  } catch (err) {
    record({ actor: opts.actor ?? 'bạn', service, method, path, status: 0, ms: performance.now() - start, requestId: null });
    throw new ApiError(0, { message: `Không kết nối được ${service}: ${(err as Error).message}` }, null);
  }
  const ms = performance.now() - start;
  const requestId = res.headers.get('x-request-id');
  record({
    actor: opts.actor ?? 'bạn',
    service,
    method,
    path,
    status: res.status,
    ms,
    requestId,
    idempotencyKey: opts.idempotencyKey,
  });

  const text = await res.text();
  let data: any = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = { message: text };
    }
  }
  if (!res.ok) throw new ApiError(res.status, data, requestId);
  return data as T;
}

let refreshing: Promise<Session | null> | null = null;

/** Đổi refresh token lấy cặp mới (rotation). Nhiều lời gọi cùng gặp 401 chỉ refresh một lần. */
function refreshSession(): Promise<Session | null> {
  const s = getSession();
  if (!s) return Promise.resolve(null);
  refreshing ??= raw<TokenPair>('gateway', 'POST', '/auth/refresh', {
    body: { refreshToken: s.refreshToken },
    token: null,
  })
    .then((t) => {
      const next = { ...s, accessToken: t.accessToken, refreshToken: t.refreshToken };
      setSession(next);
      return next;
    })
    .catch(() => {
      setSession(null);
      return null;
    })
    .finally(() => {
      refreshing = null;
    });
  return refreshing;
}

/** Gọi gateway. Dùng phiên hiện tại; gặp 401 thì refresh một lần rồi thử lại. */
async function gw<T>(method: string, path: string, opts: CallOptions = {}): Promise<T> {
  const useSession = opts.token === undefined;
  const token = useSession ? getSession()?.accessToken : opts.token;
  try {
    return await raw<T>('gateway', method, path, { ...opts, token });
  } catch (err) {
    if (useSession && err instanceof ApiError && err.status === 401 && getSession()) {
      const s = await refreshSession();
      if (s) return raw<T>('gateway', method, path, { ...opts, token: s.accessToken });
    }
    throw err;
  }
}

const pay = <T>(method: string, path: string, opts: CallOptions = {}) =>
  raw<T>('payment', method, path, { ...opts, token: null });

export const newKey = () => crypto.randomUUID();

// ---------- API ----------

export const api = {
  health: {
    gateway: () => raw<unknown>('gateway', 'GET', '/healthz', { token: null, actor: 'health' }),
    payment: () => raw<unknown>('payment', 'GET', '/healthz', { token: null, actor: 'health' }),
  },

  auth: {
    register: (email: string, password: string, actor?: string) =>
      gw<TokenPair>('POST', '/auth/register', { body: { email, password }, token: null, actor }),
    login: (email: string, password: string, actor?: string) =>
      gw<TokenPair>('POST', '/auth/login', { body: { email, password }, token: null, actor }),
    logout: (refreshToken: string) =>
      gw<void>('POST', '/auth/logout', { body: { refreshToken }, token: null }),
    me: () => gw<User>('GET', '/auth/me'),
  },

  events: {
    list: () => gw<{ events: EventView[]; nextPageToken: string | null }>('GET', '/events?pageSize=100', { token: null }),
    get: (id: string) => gw<EventView>('GET', `/events/${id}`, { token: null }),
    seats: (id: string) =>
      gw<{ eventId: string; seats: SeatView[] }>('GET', `/events/${id}/seats`, { token: null, actor: 'poll' }),
  },

  holds: {
    create: (eventId: string, seatIds: string[], idempotencyKey: string, token?: string, actor?: string) =>
      gw<HoldView>('POST', `/events/${eventId}/holds`, { body: { seatIds }, idempotencyKey, token, actor }),
    get: (id: string) => gw<HoldView>('GET', `/holds/${id}`),
    release: (id: string, token?: string, actor?: string) => gw<HoldView>('DELETE', `/holds/${id}`, { token, actor }),
  },

  orders: {
    create: (holdId: string, idempotencyKey: string) =>
      gw<OrderView>('POST', '/orders', { body: { holdId }, idempotencyKey }),
    get: (id: string, actor?: string) => gw<OrderView>('GET', `/orders/${id}`, { actor }),
  },

  admin: {
    createVenue: (name: string, address: string) =>
      gw<{ id: string; name: string; address: string }>('POST', '/admin/venues', { body: { name, address } }),
    createEvent: (body: {
      name: string;
      description?: string;
      venueId: string;
      startsAt: string;
      tiers: { name: string; priceAmount: number; currency?: string }[];
    }) => gw<EventView>('POST', '/admin/events', { body }),
    listEvents: () => gw<{ events: EventView[] }>('GET', '/admin/events?pageSize=100'),
    generateSeats: (
      eventId: string,
      blocks: { section: string; rowCount: number; seatsPerRow: number; tierId: string }[],
    ) => gw<{ eventId: string; seatCount: number }>('POST', `/admin/events/${eventId}/seats`, { body: { blocks } }),
    publish: (eventId: string) => gw<EventView>('POST', `/admin/events/${eventId}/publish`),
  },

  payment: {
    /** Intent tạo bất đồng bộ qua Redpanda: 404 nghĩa là chưa tới, trả null để client poll tiếp. */
    intentOf: async (orderId: string) => {
      try {
        return await pay<IntentView>('GET', `/payments/orders/${orderId}`, { actor: 'poll' });
      } catch (err) {
        if (err instanceof ApiError && err.status === 404) return null;
        throw err;
      }
    },
    pay: (intentId: string, outcome: ChargeView['outcome'], delayMs: number, idempotencyKey: string) =>
      pay<ChargeView>('POST', `/mock-gateway/pay/${intentId}`, { body: { outcome, delayMs }, idempotencyKey }),
  },
};

export function errorText(err: unknown): string {
  if (err instanceof ApiError) return `${err.status || 'Mạng'}: ${err.message}`;
  return (err as Error)?.message ?? String(err);
}
