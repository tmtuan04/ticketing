// Trạng thái phía trình duyệt: phiên đăng nhập, hold/đơn của user, tài khoản bot cho màn "Đua ghế".
// Backend không có API "danh sách đơn của tôi" hay saga log, nên giao diện tự nhớ id đơn và lịch sử
// trạng thái quan sát được qua polling trong localStorage.

import { useSyncExternalStore } from 'react';
import type { OrderStatus, User } from './api';

export type Session = { accessToken: string; refreshToken: string; user: User };

function read<T>(key: string, fallback: T): T {
  try {
    const v = localStorage.getItem(key);
    return v ? (JSON.parse(v) as T) : fallback;
  } catch {
    return fallback;
  }
}

function write(key: string, value: unknown) {
  try {
    if (value === null || value === undefined) localStorage.removeItem(key);
    else localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // private mode / bị chặn: chỉ mất phần ghi nhớ, giao diện vẫn chạy
  }
}

// ---------- Phiên đăng nhập ----------

const SESSION_KEY = 'ticketing.session';
let session: Session | null = read<Session | null>(SESSION_KEY, null);
const listeners = new Set<() => void>();

export const getSession = () => session;

export function setSession(s: Session | null) {
  session = s;
  write(SESSION_KEY, s);
  listeners.forEach((fn) => fn());
}

export function useSession() {
  return useSyncExternalStore(
    (fn) => {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },
    () => session,
  );
}

// ---------- Hold đang giữ (theo user + event) ----------

export type SavedHold = { id: string; idempotencyKey: string };
const holdKey = (userId: string, eventId: string) => `ticketing.hold.${userId}.${eventId}`;
export const loadHold = (userId: string, eventId: string) => read<SavedHold | null>(holdKey(userId, eventId), null);
export const saveHold = (userId: string, eventId: string, h: SavedHold | null) => write(holdKey(userId, eventId), h);

// ---------- Đơn của tôi ----------

export type SavedOrder = { id: string; eventId: string; createdAt: string };
const ordersKey = (userId: string) => `ticketing.orders.${userId}`;
export const loadOrders = (userId: string) => read<SavedOrder[]>(ordersKey(userId), []);
export function addOrder(userId: string, o: SavedOrder) {
  const list = loadOrders(userId).filter((x) => x.id !== o.id);
  write(ordersKey(userId), [o, ...list].slice(0, 50));
}

// ---------- Theo dõi một đơn: lịch sử trạng thái quan sát được + lần trả tiền ----------

export type Transition = { status: OrderStatus; at: number };
export type PaymentAttempt = {
  idempotencyKey: string;
  outcome: 'success' | 'fail' | 'timeout';
  delayMs: number;
  at: number;
};
export type OrderTrack = { transitions: Transition[]; intentSeenAt?: number; payment?: PaymentAttempt };

const trackKey = (orderId: string) => `ticketing.track.${orderId}`;
export const loadTrack = (orderId: string) => read<OrderTrack>(trackKey(orderId), { transitions: [] });
export const saveTrack = (orderId: string, t: OrderTrack) => write(trackKey(orderId), t);

// ---------- Bot cho màn "Đua ghế" ----------

export type Bot = { name: string; email: string; password: string; token?: string };
const BOTS_KEY = 'ticketing.bots';
export const loadBots = () => read<Bot[]>(BOTS_KEY, []);
export const saveBots = (bots: Bot[]) => write(BOTS_KEY, bots);
