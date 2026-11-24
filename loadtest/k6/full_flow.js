// Kịch bản 2: luồng mua vé trọn vẹn, chạy qua toàn bộ Saga dưới tải.
//
//   make seed && make loadtest script=full_flow && make verify
//   VUS=300 DURATION=1m MIX=70,20,10,0 make loadtest script=full_flow
//
// Mỗi VU là một user, lặp lại "lượt mua" cho tới khi có vé (đơn confirmed) thì dừng:
//   1. xem sơ đồ ghế (GET /events/:id/seats), chọn ngẫu nhiên SEATS_PER_HOLD ghế đang available
//   2. giữ ghế (POST /events/:id/holds); 409 = người khác nhanh tay hơn, nghỉ chút rồi chọn lại
//   3. tạo đơn (POST /orders), đơn ở awaiting_payment
//   4. poll payment intent ở payment service (intent tạo bất đồng bộ: order outbox -> Redpanda -> payment)
//   5. "trả tiền" ở cổng giả lập theo MIX (xem dưới)
//   6. poll GET /orders/:id tới trạng thái cuối, so với kết quả mong đợi của nhánh đã chọn
// Đơn bị hủy thì ghế được nhả và quay lại "chợ", nên cùng một ghế có thể qua tay nhiều user: đúng
// thứ verify.mjs cần kiểm tra (ghế sold <-> đơn confirmed, tiền <-> trạng thái đơn).
//
// MIX = tỉ lệ phần trăm "success,fail,abandon,late" (mặc định 65,20,10,5):
// - success: trả tiền thành công                      -> đơn confirmed
// - fail   : thẻ bị từ chối (webhook charge.failed)   -> đơn cancelled (payment_failed), ghế được nhả
// - abandon: không trả (bỏ ngang)                     -> timeout worker hủy đơn (payment_timeout)
// - late   : trả thành công nhưng webhook về SAU payment_deadline (delayMs của cổng giả lập)
//            -> đơn đã bị hủy, tiền về muộn -> refunding -> refunded (paid_after_cancel)
// Khác PLAN (70/20/10): thêm nhánh `late` vì đó là nhánh duy nhất đi qua hoàn tiền, bất biến "đã
// trả tiền mà không có ghế thì phải refunded" mới có dữ liệu để kiểm. MIX=70,20,10,0 để chạy đúng PLAN.
//
// Nhánh abandon/late phải chờ hết PAYMENT_TIMEOUT nên profile load test hạ PAYMENT_TIMEOUT của
// order xuống 30s (deploy/docker-compose.loadtest.yml). late cần delayMs = thời gian tới deadline
// + LATE_MARGIN_MS <= MOCK_GATEWAY_MAX_DELAY (60s) của payment, tức PAYMENT_TIMEOUT phải < ~55s.
//
// Chỉ số chính: saga_duration (từ lúc tạo đơn tới trạng thái cuối, theo từng nhánh), intent_ready
// (độ trễ outbox -> Redpanda -> payment), latency từng bước (http_req_duration theo name),
// flow_unexpected (bất kỳ response/trạng thái cuối nào sai mong đợi, ngưỡng < 1%).
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { BASE_URL, PAY_URL, authHeaders, currentUser, envInt, eventId, pickDistinct, runId, seatIds, users } from './lib/common.js';

const VUS = envInt('VUS', 500);
const SEATS_PER_HOLD = envInt('SEATS_PER_HOLD', 1);
const POLL_MS = envInt('POLL_MS', 500);
// chờ intent tối đa bao lâu (bình thường vài trăm ms: relay 500ms + Kafka + consumer)
const INTENT_WAIT_MS = envInt('INTENT_WAIT_MS', 30_000);
// sau payment_deadline còn chờ trạng thái cuối bao lâu (timeout worker 2s, webhook muộn, hoàn tiền)
const SETTLE_WAIT_MS = envInt('SETTLE_WAIT_MS', 60_000);
const LATE_MARGIN_MS = envInt('LATE_MARGIN_MS', 5_000);
const MAX_DELAY_MS = envInt('MOCK_GATEWAY_MAX_DELAY_MS', 60_000);

const BRANCHES = ['success', 'fail', 'abandon', 'late'];
const MIX = parseMix(__ENV.MIX || '65,20,10,5');
// Trạng thái cuối hợp lệ ("status/failure_reason") của từng nhánh.
// late còn chấp nhận confirmed: webhook về sau deadline nhưng TRƯỚC khi timeout worker (chu kỳ 2s)
// kịp hủy đơn thì đơn vẫn awaiting_payment, hold còn (đã gia hạn thêm HOLD_BUFFER) nên bán được.
// Đó là hành vi đúng của thiết kế, chỉ hiếm khi xảy ra với LATE_MARGIN_MS 5s.
const EXPECT = {
  success: ['confirmed/'],
  fail: ['cancelled/payment_failed'],
  abandon: ['cancelled/payment_timeout'],
  late: ['refunded/paid_after_cancel', 'confirmed/'],
};
const TERMINAL = new Set(['confirmed', 'cancelled', 'refunded']);

if (VUS > users.length) throw new Error(`VUS=${VUS} > ${users.length} users in seed, chạy lại seed với USERS lớn hơn`);

function parseMix(s) {
  const parts = s.split(',').map(Number);
  if (parts.length !== BRANCHES.length || parts.some((n) => !Number.isFinite(n) || n < 0)) {
    throw new Error(`MIX must be ${BRANCHES.length} non-negative numbers "success,fail,abandon,late", got "${s}"`);
  }
  const total = parts.reduce((a, b) => a + b, 0);
  if (total <= 0) throw new Error('MIX must not be all zero');
  return parts.map((n) => n / total);
}

function pickBranch() {
  let r = Math.random();
  for (let i = 0; i < BRANCHES.length; i++) {
    if (r < MIX[i]) return BRANCHES[i];
    r -= MIX[i];
  }
  return BRANCHES[BRANCHES.length - 1];
}

const holdWon = new Counter('hold_won');
const holdConflict = new Counter('hold_conflict');
const soldOutWaits = new Counter('sold_out_waits');
const ordersCreated = new Counter('orders_created');
const sagaFinal = new Counter('saga_final'); // tag status + branch
const seatsSold = new Counter('seats_sold');
const sagaStuck = new Counter('saga_stuck');
const lateSkipped = new Counter('late_skipped');
const intentReady = new Trend('intent_ready', true);
const sagaDuration = new Trend('saga_duration', true); // tag branch
const flowUnexpected = new Rate('flow_unexpected');

const STEPS = ['GET /events/:id/seats', 'POST /events/:id/holds', 'POST /orders', 'GET /payments/orders/:id', 'POST /mock-gateway/pay/:id', 'GET /orders/:id'];

export const options = {
  scenarios: {
    full_flow: {
      executor: 'constant-vus',
      vus: VUS,
      duration: __ENV.DURATION || '2m',
      // lượt mua đang dở phải chạy xong (abandon/late chờ hết PAYMENT_TIMEOUT), nếu không đơn bị bỏ
      // giữa chừng vẫn đúng (timeout worker dọn) nhưng k6 không kiểm được trạng thái cuối của nó
      gracefulStop: __ENV.GRACEFUL_STOP || '2m',
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    // Chốt chặn phía client: số ghế bán được không vượt số ghế của event. verify.mjs kiểm lại từ DB.
    seats_sold: [`count<=${seatIds.length}`],
    flow_unexpected: ['rate<0.01'],
    saga_stuck: ['count==0'],
    // Không phải ngưỡng thật: khai báo để summary in sub-metric theo bước/nhánh/trạng thái.
    ...Object.fromEntries(STEPS.map((s) => [`http_req_duration{name:${s}}`, ['max>=0']])),
    ...Object.fromEntries(BRANCHES.map((b) => [`saga_duration{branch:${b}}`, ['max>=0']])),
    ...Object.fromEntries(['confirmed', 'cancelled', 'refunded'].map((s) => [`saga_final{status:${s}}`, ['count>=0']])),
    ...Object.fromEntries(['0', '429', '500', '502', '503', '504'].map((s) => [`http_reqs{status:${s}}`, ['count>=0']])),
  },
};

export function setup() {
  const mix = BRANCHES.map((b, i) => `${b} ${Math.round(MIX[i] * 100)}%`).join(', ');
  console.log(`full_flow: ${VUS} VUs, ${seatIds.length} seats, ${SEATS_PER_HOLD} seat(s)/hold, mix ${mix}, event ${eventId}`);
  return { run: runId() };
}

// Biến cấp module là riêng của từng VU.
let hasTicket = false;

/** Ghi nhận một response; trả về true nếu status nằm trong danh sách mong đợi. */
function expectStatus(res, step, ...statuses) {
  const ok = statuses.includes(res.status);
  check(res, { [`${step}: status ${statuses.join('/')}`]: () => ok });
  flowUnexpected.add(!ok);
  if (!ok) console.warn(`${step} -> ${res.status} ${String(res.body).slice(0, 200)}`);
  return ok;
}

const jitter = (minMs, maxMs) => sleep((minMs + Math.random() * (maxMs - minMs)) / 1000);

export default function (data) {
  if (hasTicket) {
    sleep(5);
    return;
  }
  const user = currentUser();
  const key = (step) => `ff-${data.run}-${__VU}-${__ITER}-${step}`;

  // 1. Sơ đồ ghế
  const mapRes = http.get(`${BASE_URL}/events/${eventId}/seats`, { tags: { name: STEPS[0] } });
  if (!expectStatus(mapRes, 'seat map', 200)) return jitter(1000, 2000);
  const available = mapRes.json('seats').filter((s) => s.status === 'available').map((s) => s.seatId);
  if (available.length < SEATS_PER_HOLD) {
    // Hết ghế, nhưng đơn đang chờ thanh toán có thể bị hủy và nhả ghế: thỉnh thoảng xem lại.
    soldOutWaits.add(1);
    return jitter(2000, 4000);
  }

  // 2. Giữ ghế. 409 là kết quả nghiệp vụ bình thường (sơ đồ ghế đã cũ khi request tới nơi).
  const holdRes = http.post(
    `${BASE_URL}/events/${eventId}/holds`,
    JSON.stringify({ seatIds: pickDistinct(available, SEATS_PER_HOLD) }),
    {
      headers: authHeaders(user, { 'idempotency-key': key('hold') }),
      tags: { name: STEPS[1] },
      responseCallback: http.expectedStatuses(201, 409),
    },
  );
  if (!expectStatus(holdRes, 'hold', 201, 409)) return jitter(1000, 2000);
  if (holdRes.status === 409) {
    holdConflict.add(1);
    return jitter(200, 800);
  }
  holdWon.add(1);
  const hold = holdRes.json();

  // 3. Tạo đơn
  const orderRes = http.post(`${BASE_URL}/orders`, JSON.stringify({ holdId: hold.id }), {
    headers: authHeaders(user, { 'idempotency-key': key('order') }),
    tags: { name: STEPS[2] },
  });
  if (!expectStatus(orderRes, 'create order', 201)) {
    // Nhả hold, nếu không user bị kẹt "already has an active hold" tới khi hold hết hạn.
    http.del(`${BASE_URL}/holds/${hold.id}`, null, { headers: authHeaders(user), tags: { name: 'DELETE /holds/:id' } });
    return jitter(1000, 2000);
  }
  ordersCreated.add(1);
  const order = orderRes.json();
  const orderStart = Date.now();

  // 4. Chờ payment intent (404 tới khi payment consume xong payment.requested)
  let intent = null;
  while (Date.now() - orderStart < INTENT_WAIT_MS) {
    const r = http.get(`${PAY_URL}/payments/orders/${order.id}`, {
      tags: { name: STEPS[3] },
      responseCallback: http.expectedStatuses(200, 404),
    });
    if (r.status === 200) {
      intent = r.json();
      break;
    }
    if (r.status !== 404) expectStatus(r, 'get intent', 200, 404);
    sleep(0.2);
  }
  if (!intent) {
    // không có intent thì không trả được; đơn vẫn tự hủy khi quá hạn
    console.warn(`order ${order.id}: no payment intent after ${INTENT_WAIT_MS}ms`);
    sagaStuck.add(1, { stage: 'intent' });
    flowUnexpected.add(true);
    return;
  }
  intentReady.add(Date.now() - orderStart);

  // 5. Trả tiền theo nhánh đã chọn
  let branch = pickBranch();
  if (branch !== 'abandon') {
    let delayMs = 0;
    if (branch === 'late') {
      delayMs = Math.max(0, Date.parse(order.paymentDeadline) - Date.now()) + LATE_MARGIN_MS;
      if (delayMs > MAX_DELAY_MS) {
        // PAYMENT_TIMEOUT quá dài so với giới hạn delay của cổng giả lập: chuyển thành abandon
        lateSkipped.add(1);
        branch = 'abandon';
      }
    }
    if (branch !== 'abandon') {
      const payRes = http.post(
        `${PAY_URL}/mock-gateway/pay/${intent.id}`,
        JSON.stringify({ outcome: branch === 'fail' ? 'fail' : 'success', delayMs }),
        {
          headers: { 'content-type': 'application/json', 'idempotency-key': key('pay') },
          tags: { name: STEPS[4], branch },
          responseType: 'none',
        },
      );
      if (!expectStatus(payRes, 'pay', 202)) return;
    }
  }

  // 6. Poll đơn tới trạng thái cuối. Hạn chờ tính theo payment_deadline vì abandon/late phải chờ hết nó.
  const giveUpAt = Math.max(Date.parse(order.paymentDeadline), Date.now()) + SETTLE_WAIT_MS;
  let final = null;
  while (Date.now() < giveUpAt) {
    sleep(POLL_MS / 1000);
    const r = http.get(`${BASE_URL}/orders/${order.id}`, { headers: authHeaders(user), tags: { name: STEPS[5] } });
    if (r.status !== 200) {
      expectStatus(r, 'get order', 200);
      continue;
    }
    const o = r.json();
    // late: cancelled chưa phải điểm cuối, webhook về muộn sẽ đưa đơn sang refunding -> refunded
    if (TERMINAL.has(o.status) && !(branch === 'late' && o.status === 'cancelled')) {
      final = o;
      break;
    }
  }
  if (!final) {
    console.warn(`order ${order.id} (${branch}): not settled ${SETTLE_WAIT_MS}ms after deadline`);
    sagaStuck.add(1, { stage: 'settle', branch });
    flowUnexpected.add(true);
    return;
  }
  sagaDuration.add(Date.now() - orderStart, { branch });
  sagaFinal.add(1, { status: final.status, branch });

  const got = `${final.status}/${final.failureReason ?? ''}`;
  const ok = EXPECT[branch].includes(got);
  check(final, { [`saga ${branch} -> ${EXPECT[branch].join(' | ')}`]: () => ok });
  flowUnexpected.add(!ok);
  if (!ok) console.warn(`order ${order.id} (${branch}): expected ${EXPECT[branch].join(' | ')}, got ${got}`);

  if (final.status === 'confirmed') {
    hasTicket = true;
    seatsSold.add(final.seatIds.length);
  } else {
    jitter(200, 1000); // chưa có vé: nghỉ chút rồi mua lại
  }
}
