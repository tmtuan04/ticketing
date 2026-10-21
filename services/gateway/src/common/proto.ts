import type { Timestamp } from '../inventory/inventory.pb';

/** Timestamp từ proto-loader có seconds dạng string (longs: String). */
export function toIso(ts: Timestamp | null | undefined): string | null {
  if (!ts) return null;
  return new Date(Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6)).toISOString();
}

export function fromDate(d: Date): Timestamp {
  const ms = d.getTime();
  return { seconds: Math.floor(ms / 1000), nanos: (ms % 1000) * 1e6 };
}
