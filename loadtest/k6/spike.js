// Kịch bản 3: tải tăng vọt (spike) với rate limit THẬT: 0 -> PEAK user trong 10s, giữ đỉnh, rút về 0,
// rồi kiểm tra hệ thống hồi phục.
//
//   RATE_LIMIT_HOLD=10 make loadtest-up        (gateway với rate limit thật, thay cho mức nới của profile)
//   make seed && make loadtest script=spike && make verify
//   make loadtest-up                            (trả lại rate limit nới cho contention/full_flow)
//
// Mỗi VU là một user nôn nóng bấm "giữ ghế" liên tục không nghỉ, như lúc mở bán ai cũng F5. Kiểm tra:
// 1. Rate limiter chặn đúng: mỗi user chỉ lọt tối đa RATE_LIMIT_HOLD request mỗi cửa sổ, phần còn lại
//    nhận 429 (rẻ: chỉ một lệnh Redis, không chạm inventory). Ngưỡng `rate_limit_violations`.
// 2. Không sập: ngoài 201/409/429 không có gì khác (5xx, lỗi kết nối), ngưỡng `spike_unexpected`.
// 3. Hồi phục: sau spike, một nhóm user KHÁC (chưa bị chặn) gọi đều đều; mọi request phải thành công
//    với latency bình thường (scenario `recovery`).
// 4. Không bán vượt: tổng ghế giữ được <= số ghế, verify.mjs kiểm lại từ DB.
//
// Chỉ đánh vào route theo user (POST /events/:id/holds). Route công khai (sơ đồ ghế) giới hạn theo
// IP, mà mọi VU của k6 đi ra từ một IP: chúng sẽ chung một bộ đếm và bị 429 gần hết, đo vậy vô nghĩa.
// Gateway lấy IP từ hop nginx thêm vào (TRUST_PROXY=1) nên k6 không giả X-Forwarded-For được, đúng
// như mong muốn về bảo mật.
//
// Bộ đếm rate limit nằm ở Redis theo user id, mà seed giữ nguyên id user giữa các lần chạy: hai lần
// spike cách nhau dưới RATE_LIMIT_WINDOW thì user đã bị chặn sẵn từ đầu (vẫn đúng, chỉ là ít 201/409).
import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import { BASE_URL, authHeaders, envInt, eventId, runId, seatIds, users } from './lib/common.js';

const PEAK = envInt('PEAK', 4000);
const RAMP = __ENV.RAMP || '10s';
const HOLD_PEAK = __ENV.HOLD_PEAK || '30s';
// phải khớp cấu hình gateway (RATE_LIMIT_HOLD, RATE_LIMIT_WINDOW)
const LIMIT = envInt('RATE_LIMIT_HOLD', 10);
const WINDOW_MS = envInt('RATE_LIMIT_WINDOW_SEC', 60) * 1000;
const RECOVERY_RATE = envInt('RECOVERY_RATE', 20);
const RECOVERY_FOR = __ENV.RECOVERY_FOR || '20s';

// Chia user: [0, PEAK) cho spike, [PEAK, cuối - 1) cho recovery, user cuối để thăm dò rate limit.
const PROBE_USER = users.length - 1;
const RECOVERY_USERS = users.length - 1 - PEAK;
if (RECOVERY_USERS < 100) {
  throw new Error(`cần ít nhất PEAK + 101 user trong seed (PEAK=${PEAK}, seed có ${users.length}), giảm PEAK hoặc seed thêm USERS`);
}

// Thời điểm recovery bắt đầu: sau khi spike rút hết (ramp lên + đỉnh + ramp xuống + gracefulRampDown).
const toSec = (d) => {
  const m = /^(\d+)(ms|s|m)$/.exec(d);
  if (!m) throw new Error(`duration "${d}" phải có dạng 500ms, 10s hoặc 2m`);
  return Number(m[1]) / { ms: 1000, s: 1, m: 1 / 60 }[m[2]];
};
const spikeSec = 2 * toSec(RAMP) + toSec(HOLD_PEAK) + 5;

const accepted = new Counter('hold_accepted'); // 201 + 409: lọt qua rate limiter
const rateLimited = new Rate('rate_limited'); // tỉ lệ 429 trong các request của spike
const seatsWon = new Counter('seats_won');
const violations = new Counter('rate_limit_violations');
const spikeUnexpected = new Rate('spike_unexpected');

export const options = {
  scenarios: {
    spike: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: RAMP, target: PEAK },
        { duration: HOLD_PEAK, target: PEAK },
        { duration: RAMP, target: 0 },
      ],
      gracefulRampDown: '5s',
      exec: 'spike',
    },
    recovery: {
      executor: 'constant-arrival-rate',
      startTime: `${spikeSec}s`,
      rate: RECOVERY_RATE,
      timeUnit: '1s',
      duration: RECOVERY_FOR,
      preAllocatedVUs: Math.max(10, RECOVERY_RATE),
      exec: 'recovery',
    },
  },
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    // Không sập: mọi response của spike là 201/409/429.
    spike_unexpected: ['rate<0.01'],
    // Rate limiter đúng: không user nào lọt quá giới hạn (xem spike()).
    rate_limit_violations: ['count==0'],
    // Rate limiter có làm việc: user spam liên tục thì phần lớn request phải là 429.
    rate_limited: ['rate>0.5'],
    seats_won: [`count<=${seatIds.length}`],
    // Hồi phục: sau spike, user mới gọi bình thường thì không lỗi và nhanh.
    'http_req_failed{scenario:recovery}': ['rate==0'],
    'http_req_duration{scenario:recovery}': ['p(95)<500'],
    // Không phải ngưỡng thật: khai báo để summary in latency theo loại response và theo pha.
    'http_req_duration{scenario:spike,status:201}': ['max>=0'],
    'http_req_duration{scenario:spike,status:409}': ['max>=0'],
    'http_req_duration{scenario:spike,status:429}': ['max>=0'],
    ...Object.fromEntries(['0', '500', '502', '503', '504'].map((s) => [`http_reqs{status:${s}}`, ['count>=0']])),
  },
};

// spike: 429 là kết quả đúng (bị chặn); recovery: 429 là lỗi (user chưa gửi gì), tính vào http_req_failed
const SPIKE_OK = http.expectedStatuses(201, 409, 429);
const RECOVERY_OK = http.expectedStatuses(201, 409);

function holdRequest(user, key, responseCallback) {
  return http.post(
    `${BASE_URL}/events/${eventId}/holds`,
    JSON.stringify({ seatIds: [seatIds[Math.floor(Math.random() * seatIds.length)]] }),
    {
      headers: authHeaders(user, { 'idempotency-key': key }),
      tags: { name: 'POST /events/:id/holds' },
      responseCallback,
    },
  );
}

/**
 * Thăm dò trước khi chạy: gateway có đang bật rate limit thật không. Gửi body sai (seatIds rỗng):
 * guard rate limit chạy TRƯỚC ValidationPipe nên request vẫn được đếm, nhận 400 mà không giữ ghế nào.
 * Không thấy 429 sau LIMIT + 1 request => gateway đang chạy rate limit nới của profile load test.
 */
export function setup() {
  const user = users[PROBE_USER];
  const statuses = [];
  for (let i = 0; i <= LIMIT; i++) {
    const r = http.post(`${BASE_URL}/events/${eventId}/holds`, JSON.stringify({ seatIds: [] }), {
      headers: authHeaders(user, { 'idempotency-key': `probe-${i}` }),
      tags: { name: 'probe' },
      responseCallback: http.expectedStatuses(400, 429),
    });
    statuses.push(r.status);
    if (r.status === 429) break;
  }
  if (!statuses.includes(429)) {
    exec.test.abort(
      `gateway không trả 429 sau ${LIMIT + 1} request (${statuses.join(',')}): rate limit đang nới. ` +
        `Chạy lại stack: RATE_LIMIT_HOLD=${LIMIT} make loadtest-up`,
    );
  }
  console.log(
    `spike: 0 -> ${PEAK} VUs in ${RAMP}, hold ${HOLD_PEAK}, limit ${LIMIT}/${WINDOW_MS / 1000}s per user; ` +
      `recovery ${RECOVERY_RATE} req/s for ${RECOVERY_FOR} from ${spikeSec}s; probe ${statuses.join(',')}`,
  );
  return { run: runId() };
}

// Biến cấp module là riêng của từng VU (mỗi VU của spike là đúng một user).
let firstAcceptedAt = 0;
let acceptedCount = 0;
let violated = false;

export function spike(data) {
  const res = holdRequest(users[__VU - 1], `sp-${data.run}-${__VU}-${__ITER}`, SPIKE_OK);
  const ok = [201, 409, 429].includes(res.status);
  check(res, { 'spike: 201/409/429': () => ok });
  spikeUnexpected.add(!ok);
  rateLimited.add(res.status === 429);
  if (res.status === 201) seatsWon.add(1);
  if (res.status !== 201 && res.status !== 409) return;

  // Lọt qua rate limiter. Fixed window: trong khoảng thời gian dài `span`, một user chạm tối đa
  // floor(span / window) + 2 cửa sổ (cửa sổ đầu có thể đã chạy dở), mỗi cửa sổ tối đa LIMIT request.
  accepted.add(1);
  const now = Date.now();
  if (!firstAcceptedAt) firstAcceptedAt = now;
  acceptedCount++;
  const bound = LIMIT * (Math.floor((now - firstAcceptedAt) / WINDOW_MS) + 2);
  if (acceptedCount > bound && !violated) {
    violated = true;
    violations.add(1);
    console.warn(`VU ${__VU}: ${acceptedCount} requests passed the rate limiter, bound ${bound}`);
  }
}

export function recovery(data) {
  // mỗi lượt một user khác nhau trong nhóm recovery: không ai chạm giới hạn
  const i = PEAK + (exec.scenario.iterationInTest % RECOVERY_USERS);
  const res = holdRequest(users[i], `rc-${data.run}-${exec.scenario.iterationInTest}`, RECOVERY_OK);
  check(res, { 'recovery: 201/409': (r) => r.status === 201 || r.status === 409 });
  if (res.status === 201) seatsWon.add(1);
}
