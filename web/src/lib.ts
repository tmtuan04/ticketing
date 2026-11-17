import { useEffect, useRef, useState, useSyncExternalStore } from 'react';
import type { SeatView } from './api';

// ---------- Router theo hash: #/events/<id>, #/orders/<id>, ... ----------

function subscribeHash(fn: () => void) {
  window.addEventListener('hashchange', fn);
  return () => window.removeEventListener('hashchange', fn);
}

export function useRoute(): string[] {
  const hash = useSyncExternalStore(subscribeHash, () => window.location.hash);
  return hash.replace(/^#\/?/, '').split('/').filter(Boolean);
}

export const navigate = (path: string) => {
  window.location.hash = path;
};

// ---------- Hook ----------

/** Gọi `fn` mỗi `ms` (bỏ qua nếu lần trước chưa xong). `ms` = null để dừng. */
export function useInterval(fn: () => unknown, ms: number | null) {
  const saved = useRef(fn);
  saved.current = fn;
  useEffect(() => {
    if (ms === null) return;
    let busy = false;
    const id = setInterval(async () => {
      if (busy) return;
      busy = true;
      try {
        await saved.current();
      } finally {
        busy = false;
      }
    }, ms);
    return () => clearInterval(id);
  }, [ms]);
}

/** Thời điểm hiện tại, cập nhật mỗi `ms`, để vẽ đồng hồ đếm ngược. */
export function useNow(ms = 250) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(id);
  }, [ms]);
  return now;
}

// ---------- Định dạng ----------

export function money(amount: number, currency: string) {
  try {
    return new Intl.NumberFormat('vi-VN', { style: 'currency', currency: currency || 'VND' }).format(amount);
  } catch {
    return `${amount.toLocaleString('vi-VN')} ${currency}`;
  }
}

export const seatLabel = (s: Pick<SeatView, 'section' | 'row' | 'number'>) => `${s.section}-${s.row}-${s.number}`;

export function fmtTime(iso: string | number | null | undefined) {
  if (iso === null || iso === undefined) return '—';
  return new Date(iso).toLocaleTimeString('vi-VN', { hour12: false });
}

export function fmtDateTime(iso: string | null | undefined) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('vi-VN', {
    hour: '2-digit',
    minute: '2-digit',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour12: false,
  });
}

export function fmtCountdown(msLeft: number) {
  if (msLeft <= 0) return '00:00';
  const s = Math.ceil(msLeft / 1000);
  return `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
}

export const shortId = (id: string) => id.slice(0, 8);
