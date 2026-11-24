// Kiểm tra bất biến sau load test (PLAN 3.3), cho event trong file seed.
//
//   node loadtest/verify.mjs          (hoặc `make verify`)
//
// Khác PLAN: không phải một file verify.sql. Mỗi service có database riêng (inventory_db, order_db,
// payment_db) nên một câu SQL không JOIN chéo được (trừ khi bật dblink/postgres_fdw, tức là sửa
// schema chỉ để kiểm thử). Kiểm tra trong một DB viết bằng SQL; kiểm tra chéo DB thì đọc dữ liệu
// của event về rồi so trong JS (event chỉ có vài trăm ghế/đơn nên rất nhẹ).
//
// Hệ thống nhất quán dần (relay, consumer, reaper, timeout worker chạy nền), nên ngay sau khi k6
// dừng có thể vẫn còn message chưa publish hay đơn chưa xử lý. Script thử lại tới khi mọi bất biến
// PASS hoặc hết VERIFY_TIMEOUT_SEC, rồi in kết quả lần cuối. Thoát với mã 1 nếu còn bất biến FAIL.

import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

const here = dirname(fileURLToPath(import.meta.url));
const DB_BASE = process.env.DB_BASE_URL ?? 'postgresql://app:app@localhost:5440';
const cfg = {
  seedFile: resolve(here, process.env.SEED_OUT ?? '.data/seed.json'),
  timeoutSec: Number(process.env.VERIFY_TIMEOUT_SEC ?? 90),
  // khoảng trễ cho phép của worker nền: reaper chạy mỗi 5s, timeout worker mỗi 2s
  graceSec: Number(process.env.VERIFY_GRACE_SEC ?? 30),
};

async function connect(db) {
  const c = new pg.Client({ connectionString: `${DB_BASE}/${db}` });
  await c.connect();
  return c;
}

/** Đọc mọi thứ cần thiết của một event từ ba DB (một "ảnh chụp", không cần nhất quán tuyệt đối). */
async function snapshot(dbs, eventId, graceSec) {
  const { inv, ord, pay } = dbs;
  const q = async (c, sql, params = []) => (await c.query(sql, params)).rows;

  const seats = await q(inv, `SELECT seat_id, status, hold_id, held_until FROM seat_inventory WHERE event_id = $1`, [eventId]);
  const holds = await q(inv, `SELECT id, user_id, status, seat_ids, expires_at FROM holds WHERE event_id = $1`, [eventId]);
  const orders = await q(ord, `SELECT id, hold_id, seat_ids, status, failure_reason, payment_deadline FROM orders WHERE event_id = $1`, [eventId]);
  const orderIds = orders.map((o) => o.id);
  const intents = orderIds.length
    ? await q(pay, `SELECT id, order_id, status::text AS status FROM payment_intents WHERE order_id = ANY($1::uuid[])`, [orderIds])
    : [];
  const intentIds = intents.map((i) => i.id);

  // Các kiểm tra viết thẳng bằng SQL trong một DB.
  const sql = {
    // Hai hold "còn sống" cùng chứa một ghế = bán vượt. Hold active đã quá hạn nhưng reaper chưa
    // dọn không tính: ghế của nó được phép bị người khác giữ (UPDATE ... OR held_until < now()).
    doubleHeldSeats: await q(
      inv,
      `SELECT s AS seat_id, array_agg(h.id) AS hold_ids
         FROM holds h, unnest(h.seat_ids) AS s
        WHERE h.event_id = $1 AND (h.status = 'confirmed' OR (h.status = 'active' AND h.expires_at > now()))
        GROUP BY s HAVING count(*) > 1`,
      [eventId],
    ),
    staleHeldSeats: await q(
      inv,
      `SELECT seat_id, hold_id, held_until FROM seat_inventory
        WHERE event_id = $1 AND status = 'held' AND held_until < now() - make_interval(secs => $2)`,
      [eventId, graceSec],
    ),
    staleActiveHolds: await q(
      inv,
      `SELECT id, expires_at FROM holds
        WHERE event_id = $1 AND status = 'active' AND expires_at < now() - make_interval(secs => $2)`,
      [eventId, graceSec],
    ),
    doubleSoldInOrders: await q(
      ord,
      `SELECT s AS seat_id, array_agg(o.id) AS order_ids
         FROM orders o, unnest(o.seat_ids) AS s
        WHERE o.event_id = $1 AND o.status = 'confirmed'
        GROUP BY s HAVING count(*) > 1`,
      [eventId],
    ),
    overdueOrders: await q(
      ord,
      `SELECT id, payment_deadline FROM orders
        WHERE event_id = $1 AND status = 'awaiting_payment' AND payment_deadline < now() - make_interval(secs => $2)`,
      [eventId, graceSec],
    ),
    // Outbox xét toàn bộ bảng (không lọc event): message nào còn kẹt cũng là vấn đề.
    orderOutboxPending: await q(ord, `SELECT count(*)::int AS n FROM outbox WHERE published_at IS NULL`),
    paymentOutboxPending: await q(pay, `SELECT count(*)::int AS n FROM outbox WHERE published_at IS NULL`),
    // Webhook cổng giả lập chưa gửi (charge 'timeout' cố ý không bao giờ gửi). Nhánh "trả muộn" của
    // full_flow.js đặt lịch webhook sau deadline: chưa gửi xong thì đơn cancelled + intent
    // requires_payment trông vẫn hợp lệ, nên phải chờ tới khi gửi hết mới chốt kết quả.
    // Chỉ xét charge của event này: charge cũ của stack dev không liên quan.
    mockChargesPending: intentIds.length
      ? await q(
          pay,
          `SELECT count(*)::int AS n FROM mock_charges
            WHERE intent_id = ANY($1::uuid[]) AND delivered_at IS NULL AND outcome <> 'timeout'`,
          [intentIds],
        )
      : [{ n: 0 }],
  };
  return { seats, holds, orders, intents, sql };
}

/** Trả về danh sách bất biến, mỗi cái { name, ok, detail[] }. */
function evaluate(snap, capacity) {
  const { seats, holds, orders, intents, sql } = snap;
  const checks = [];
  const add = (name, bad, fmt = (x) => JSON.stringify(x)) =>
    checks.push({ name, ok: bad.length === 0, count: bad.length, detail: bad.slice(0, 5).map(fmt) });

  const holdById = new Map(holds.map((h) => [h.id, h]));
  const seatById = new Map(seats.map((s) => [s.seat_id, s]));
  const intentByOrder = new Map(intents.map((i) => [i.order_id, i.status]));
  const sold = seats.filter((s) => s.status === 'sold');

  // 1. Không bán vượt
  add(`seat_inventory có đúng ${capacity} ghế của event`, seats.length === capacity ? [] : [`${seats.length} rows`]);
  add('không ghế nào thuộc 2 hold còn hiệu lực', sql.doubleHeldSeats);
  add('không ghế nào nằm trong 2 đơn confirmed', sql.doubleSoldInOrders);
  add(`tổng ghế sold <= capacity (${sold.length}/${capacity})`, sold.length <= capacity ? [] : [sold.length]);

  // 2. Trạng thái ghế khớp với hold đang giữ nó
  add(
    'ghế held/sold khớp hold (held -> active, sold -> confirmed, hold chứa ghế)',
    seats.filter((s) => {
      if (s.status === 'available') return false;
      const h = holdById.get(s.hold_id);
      if (!h || !h.seat_ids.includes(s.seat_id)) return true;
      return s.status === 'sold' ? h.status !== 'confirmed' : h.status !== 'active';
    }),
  );
  add(
    'hold confirmed -> mọi ghế của nó là sold bởi chính hold đó',
    holds
      .filter((h) => h.status === 'confirmed')
      .filter((h) => h.seat_ids.some((id) => seatById.get(id)?.status !== 'sold' || seatById.get(id)?.hold_id !== h.id))
      .map((h) => h.id),
  );

  // 3. Đơn confirmed <-> ghế sold (chéo order_db và inventory_db)
  const confirmed = orders.filter((o) => o.status === 'confirmed');
  const confirmedSeatCount = confirmed.reduce((n, o) => n + o.seat_ids.length, 0);
  add(
    `số ghế của đơn confirmed = số ghế sold (${confirmedSeatCount} vs ${sold.length})`,
    confirmedSeatCount === sold.length ? [] : [{ confirmedSeatCount, sold: sold.length }],
  );
  add(
    'mỗi ghế của đơn confirmed đang sold bởi đúng hold của đơn',
    confirmed.filter((o) => o.seat_ids.some((id) => seatById.get(id)?.status !== 'sold' || seatById.get(id)?.hold_id !== o.hold_id)).map((o) => o.id),
  );

  // 4. Tiền (chéo order_db và payment_db)
  add('mọi đơn đều có payment intent', orders.filter((o) => !intentByOrder.has(o.id)).map((o) => o.id));
  const expectIntent = {
    confirmed: ['succeeded'],
    refunded: ['refunded'],
    cancelled: ['requires_payment', 'failed'],
    awaiting_payment: ['requires_payment'],
  };
  add(
    'trạng thái đơn khớp payment intent (đã trả tiền mà không có ghế -> refunded)',
    orders
      .filter((o) => intentByOrder.has(o.id))
      .filter((o) => !(expectIntent[o.status] ?? []).includes(intentByOrder.get(o.id)))
      .map((o) => ({ order: o.id, order_status: o.status, intent: intentByOrder.get(o.id), reason: o.failure_reason })),
  );

  // 5. Worker nền đã làm xong việc
  add('không còn ghế held quá hạn (reaper)', sql.staleHeldSeats);
  add('không còn hold active quá hạn (reaper)', sql.staleActiveHolds);
  add('không còn đơn awaiting_payment quá payment_deadline (timeout worker)', sql.overdueOrders);
  add(
    'không còn đơn ở trạng thái trung gian (pending/paid/refunding)',
    orders.filter((o) => ['pending', 'paid', 'refunding'].includes(o.status)).map((o) => ({ id: o.id, status: o.status })),
  );
  add('outbox của order đã publish hết', sql.orderOutboxPending[0].n ? [`${sql.orderOutboxPending[0].n} pending`] : []);
  add('outbox của payment đã publish hết', sql.paymentOutboxPending[0].n ? [`${sql.paymentOutboxPending[0].n} pending`] : []);
  add('webhook của cổng giả lập đã gửi hết', sql.mockChargesPending[0].n ? [`${sql.mockChargesPending[0].n} pending`] : []);
  return checks;
}

function countBy(rows, key) {
  const m = {};
  for (const r of rows) m[r[key]] = (m[r[key]] ?? 0) + 1;
  return m;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const seed = JSON.parse(await readFile(cfg.seedFile, 'utf8'));
  const capacity = seed.seatIds.length;
  const dbs = {
    inv: await connect('inventory_db'),
    ord: await connect('order_db'),
    pay: await connect('payment_db'),
  };
  try {
    const deadline = Date.now() + cfg.timeoutSec * 1000;
    let snap, checks, attempt = 0;
    for (;;) {
      attempt++;
      snap = await snapshot(dbs, seed.eventId, cfg.graceSec);
      checks = evaluate(snap, capacity);
      if (checks.every((c) => c.ok) || Date.now() >= deadline) break;
      const failing = checks.filter((c) => !c.ok).map((c) => c.name);
      console.log(`attempt ${attempt}: ${failing.length} invariant(s) not yet satisfied, retrying... (${failing.join('; ')})`);
      await sleep(3000);
    }

    console.log(`\nevent ${seed.eventId} (capacity ${capacity})`);
    console.log('  seats  :', countBy(snap.seats, 'status'));
    console.log('  holds  :', countBy(snap.holds, 'status'));
    console.log('  orders :', countBy(snap.orders, 'status'));
    console.log('  intents:', countBy(snap.intents, 'status'));
    console.log('');
    for (const c of checks) {
      console.log(`${c.ok ? 'PASS' : 'FAIL'}  ${c.name}${c.ok ? '' : ` (${c.count})`}`);
      for (const d of c.detail) console.log(`        ${d}`);
    }
    const failed = checks.filter((c) => !c.ok).length;
    console.log(`\n${failed ? `${failed} invariant(s) FAILED` : `all ${checks.length} invariants PASS`} after ${attempt} attempt(s)`);
    process.exitCode = failed ? 1 : 0;
  } finally {
    await Promise.all(Object.values(dbs).map((c) => c.end()));
  }
}

main().catch((err) => {
  console.error('verify failed:', err.message);
  process.exit(2);
});
