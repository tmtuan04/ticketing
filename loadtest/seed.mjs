// Seed cho load test (Phase 3): tạo 1 event đã publish và N user, ký sẵn JWT ra file JSON để k6 đọc.
//
//   node loadtest/seed.mjs            (hoặc `make seed`, cần stack đang chạy)
//
// Vì sao không cho k6 gọi /auth/register + /auth/login: scrypt ăn CPU của gateway và bucket rate limit
// `auth` (theo IP) chặn request, làm nhiễu số đo của luồng giữ ghế, là luồng ta thực sự muốn đo.
// Nên user được ghi thẳng vào auth_db (cùng một hash mật khẩu, tính 1 lần) và JWT được ký bằng chính
// secret của gateway (JwtAuthGuard chỉ kiểm tra chữ ký/iss/aud/exp, không tra DB).
// Event thì đi qua REST admin của gateway như thật, để catalog gọi InitEventInventory sang inventory.
//
// Mỗi lần chạy tạo một event MỚI: các lần chạy độc lập với nhau, verify.mjs chỉ xét event trong file seed.

import { createHmac, randomBytes, scrypt } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import pg from 'pg';

const here = dirname(fileURLToPath(import.meta.url));

const cfg = {
  gatewayUrl: process.env.GATEWAY_URL ?? 'http://localhost:3000',
  authDbUrl: process.env.AUTH_DB_URL ?? 'postgresql://app:app@localhost:5440/auth_db',
  // phải trùng JWT_ACCESS_SECRET của gateway (deploy/docker-compose.yml)
  jwtSecret: process.env.JWT_ACCESS_SECRET ?? 'dev-only-secret-change-me-0123456789abcdef',
  adminEmail: process.env.ADMIN_EMAIL ?? 'admin@ticketing.local',
  adminPassword: process.env.ADMIN_PASSWORD ?? 'admin12345',
  users: int('USERS', 5000),
  // sơ đồ ghế ROWS x SEATS_PER_ROW, mặc định 10 x 10 = 100 ghế
  rows: int('ROWS', 10),
  seatsPerRow: int('SEATS_PER_ROW', 10),
  priceAmount: int('PRICE', 500_000),
  // token phải sống lâu hơn cả lần chạy k6 dài nhất (gateway mặc định chỉ 15m)
  tokenTtlSec: int('TOKEN_TTL_SEC', 6 * 3600),
  out: resolve(here, process.env.SEED_OUT ?? '.data/seed.json'),
};

const USER_DOMAIN = 'loadtest.local';
const USER_PASSWORD = 'loadtest12345';

function int(key, def) {
  const v = process.env[key];
  if (v === undefined || v === '') return def;
  const n = Number(v);
  if (!Number.isInteger(n) || n <= 0) throw new Error(`${key} must be a positive integer, got "${v}"`);
  return n;
}

async function api(method, path, { token, body } = {}) {
  const res = await fetch(cfg.gatewayUrl + path, {
    method,
    headers: {
      'content-type': 'application/json',
      ...(token && { authorization: `Bearer ${token}` }),
    },
    body: body && JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}: ${text}`);
  return text ? JSON.parse(text) : undefined;
}

/** Cùng định dạng với services/gateway/src/auth/password.ts: scrypt$N$r$p$<salt>$<hash>. */
async function hashPassword(password) {
  const N = 2 ** 15, r = 8, p = 1;
  const salt = randomBytes(16);
  const key = await promisify(scrypt)(password, salt, 64, { N, r, p, maxmem: 256 * N * r });
  return `scrypt$${N}$${r}$${p}$${salt.toString('base64')}$${key.toString('base64')}`;
}

const b64url = (v) => Buffer.from(typeof v === 'string' ? v : JSON.stringify(v)).toString('base64url');

/** JWT HS256 giống AuthService.tokenPair của gateway (iss/aud phải khớp JwtAuthGuard). */
function signAccessToken(userId, now) {
  const header = b64url({ alg: 'HS256', typ: 'JWT' });
  const payload = b64url({
    sub: userId,
    role: 'user',
    iat: now,
    exp: now + cfg.tokenTtlSec,
    iss: 'ticketing-gateway',
    aud: 'ticketing',
  });
  const sig = createHmac('sha256', cfg.jwtSecret).update(`${header}.${payload}`).digest('base64url');
  return `${header}.${payload}.${sig}`;
}

async function seedEvent() {
  const { accessToken: admin } = await api('POST', '/auth/login', {
    body: { email: cfg.adminEmail, password: cfg.adminPassword },
  });
  const stamp = new Date().toISOString();
  const venue = await api('POST', '/admin/venues', {
    token: admin,
    body: { name: `Loadtest Arena ${stamp}`, address: 'k6' },
  });
  const event = await api('POST', '/admin/events', {
    token: admin,
    body: {
      name: `Flash sale loadtest ${stamp}`,
      description: 'Sinh bởi loadtest/seed.mjs',
      venueId: venue.id,
      startsAt: new Date(Date.now() + 30 * 86400e3).toISOString(),
      tiers: [{ name: 'Standard', priceAmount: cfg.priceAmount, currency: 'VND' }],
    },
  });
  await api('POST', `/admin/events/${event.id}/seats`, {
    token: admin,
    body: {
      blocks: [{ section: 'A', rowCount: cfg.rows, seatsPerRow: cfg.seatsPerRow, tierId: event.tiers[0].id }],
    },
  });
  await api('POST', `/admin/events/${event.id}/publish`, { token: admin });

  // Đọc lại qua API công khai: vừa lấy seat_id vừa xác nhận inventory đã khởi tạo đủ ghế.
  const { seats } = await api('GET', `/events/${event.id}/seats`);
  const notReady = seats.filter((s) => s.status !== 'available');
  if (seats.length !== cfg.rows * cfg.seatsPerRow || notReady.length) {
    throw new Error(`expected ${cfg.rows * cfg.seatsPerRow} available seats, got ${seats.length} (${notReady.length} not available)`);
  }
  // giữ thứ tự hàng/ghế để kịch bản k6 chọn "ghế hot" (hàng đầu) cho dễ hiểu
  seats.sort((a, b) => a.row.localeCompare(b.row, undefined, { numeric: true }) || a.number - b.number);
  return { eventId: event.id, seatIds: seats.map((s) => s.seatId) };
}

async function seedUsers() {
  const db = new pg.Client({ connectionString: cfg.authDbUrl });
  await db.connect();
  try {
    const passwordHash = await hashPassword(USER_PASSWORD);
    // Một câu INSERT cho cả N user; chạy lại thì user cũ giữ nguyên (ON CONFLICT), id không đổi.
    await db.query(
      `INSERT INTO users (id, email, password_hash, role, created_at, updated_at)
       SELECT gen_random_uuid(), format('lt-%s@${USER_DOMAIN}', lpad(i::text, 6, '0')), $1, 'user', now(), now()
       FROM generate_series(1, $2::int) AS i
       ON CONFLICT (email) DO NOTHING`,
      [passwordHash, cfg.users],
    );
    const { rows } = await db.query(
      `SELECT id, email FROM users WHERE email LIKE 'lt-%@${USER_DOMAIN}' ORDER BY email LIMIT $1`,
      [cfg.users],
    );
    return rows;
  } finally {
    await db.end();
  }
}

async function main() {
  const t0 = Date.now();
  const [event, users] = await Promise.all([seedEvent(), seedUsers()]);
  const now = Math.floor(Date.now() / 1000);
  const seed = {
    createdAt: new Date().toISOString(),
    gatewayUrl: cfg.gatewayUrl,
    eventId: event.eventId,
    seatIds: event.seatIds,
    tokenExpiresAt: new Date((now + cfg.tokenTtlSec) * 1000).toISOString(),
    users: users.map((u) => ({ id: u.id, token: signAccessToken(u.id, now) })),
  };

  // Thử một token trước khi giao cho k6: sai secret/iss/aud thì báo ngay ở đây.
  await api('GET', '/auth/me', { token: seed.users[0].token });

  await mkdir(dirname(cfg.out), { recursive: true });
  await writeFile(cfg.out, JSON.stringify(seed));
  console.log(
    `seed ok in ${Date.now() - t0}ms: event ${seed.eventId}, ${seed.seatIds.length} seats, ` +
      `${seed.users.length} users (password "${USER_PASSWORD}"), token valid until ${seed.tokenExpiresAt}\n-> ${cfg.out}`,
  );
}

main().catch((err) => {
  console.error('seed failed:', err.message);
  process.exit(1);
});
