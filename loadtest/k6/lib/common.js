// Phần dùng chung cho các kịch bản k6: đọc file seed, header, helper.
import { SharedArray } from 'k6/data';

export const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';
export const PAY_URL = __ENV.PAY_URL || 'http://localhost:3001';
const SEED_FILE = __ENV.SEED_FILE || '../.data/seed.json';

// SharedArray: callback chạy MỘT lần, dữ liệu dùng chung cho mọi VU (chỉ đọc). Nếu mỗi VU tự
// JSON.parse file seed (~2 MB với 5.000 token) thì 5.000 VU tốn vài GB RAM chỉ để giữ bản sao.
// Tách hai mảng vì mỗi lần truy cập phần tử SharedArray là một lần copy phần tử đó.
export const users = new SharedArray('users', () => JSON.parse(open(SEED_FILE)).users);
const meta = new SharedArray('meta', () => {
  const s = JSON.parse(open(SEED_FILE));
  return [{ eventId: s.eventId, seatIds: s.seatIds }];
})[0];
export const eventId = meta.eventId;
export const seatIds = meta.seatIds;

export function envInt(key, def) {
  const v = __ENV[key];
  if (v === undefined || v === '') return def;
  const n = Number(v);
  if (!Number.isInteger(n) || n < 0) throw new Error(`${key} must be a non-negative integer, got "${v}"`);
  return n;
}

/** User của VU hiện tại: mỗi VU đóng vai đúng một user (VU đánh số từ 1). */
export function currentUser() {
  return users[(__VU - 1) % users.length];
}

export function authHeaders(user, extra = {}) {
  return {
    'content-type': 'application/json',
    authorization: `Bearer ${user.token}`,
    ...extra,
  };
}

/** Chọn ngẫu nhiên n phần tử khác nhau (Fisher–Yates rút gọn, không sửa mảng gốc). */
export function pickDistinct(arr, n) {
  const a = arr.slice();
  for (let i = 0; i < n; i++) {
    const j = i + Math.floor(Math.random() * (a.length - i));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a.slice(0, n);
}

/** Id ngẫu nhiên ngắn cho mỗi lần chạy, ghép vào Idempotency-Key để các lần chạy không đụng nhau. */
export function runId() {
  return `${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`;
}
