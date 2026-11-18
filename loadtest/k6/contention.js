// Kịch bản 1: nhiều user cùng lúc tranh một nhóm ghế hot (flash sale mở bán).
//
//   make seed && make loadtest script=contention
//   VUS=2000 HOT_SEATS=20 DURATION=20s make loadtest script=contention
//
// Mỗi VU là một user (token ký sẵn trong file seed), bấm "giữ ghế" liên tục trên các ghế hot, không
// nghỉ (THINK_MS=0) cho tới khi giữ được thì dừng, giống người thật đã có vé thì không bấm nữa.
// Toàn bộ VU được khởi tạo trước rồi mới bắt đầu nên tải lên đỉnh ngay lập tức.
//
// Hai chế độ:
// - Mặc định (closed model, `constant-vus`): 5.000 VU không nghỉ. Đây là stress test: tải vượt năng
//   lực hệ thống nên latency = số VU / thông lượng (định luật Little), p99 phản ánh hàng đợi lúc
//   quá tải chứ không phải độ trễ thật. Dùng để chứng minh tính đúng đắn và đo thông lượng tối đa.
// - RATE=n (open model, `constant-arrival-rate`): bắn đúng n request/s bất kể server nhanh hay chậm,
//   mỗi request là một user lần lượt trong 5.000 user. Dùng để đo p95/p99 ở mức tải bền vững.
//     RATE=1500 make loadtest script=contention
//
// 409 ở đây là KẾT QUẢ ĐÚNG (người thua), không phải lỗi. Thứ cần đo: RPS, p95/p99, tỉ lệ 409, và
// tổng số ghế giữ được không bao giờ vượt số ghế hot (threshold `seats_won`). verify.mjs kiểm tra
// lại điều đó từ phía DB sau khi chạy.
import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import { BASE_URL, authHeaders, currentUser, envInt, eventId, pickDistinct, runId, seatIds, users } from './lib/common.js';

const VUS = envInt('VUS', 5000);
const RATE = envInt('RATE', 0);
const HOT_SEATS = Math.min(envInt('HOT_SEATS', seatIds.length), seatIds.length);
const SEATS_PER_HOLD = envInt('SEATS_PER_HOLD', 1);
const THINK_MS = envInt('THINK_MS', 0);
const hotSeats = seatIds.slice(0, HOT_SEATS);

if (VUS > users.length) throw new Error(`VUS=${VUS} > ${users.length} users in seed, chạy lại seed với USERS lớn hơn`);
if (SEATS_PER_HOLD < 1 || SEATS_PER_HOLD > HOT_SEATS) throw new Error('SEATS_PER_HOLD must be in [1, HOT_SEATS]');

const holdWon = new Counter('hold_won');
const seatsWon = new Counter('seats_won');
const holdConflict = new Counter('hold_conflict');
const holdUnexpected = new Rate('hold_unexpected');

export const options = {
  scenarios: {
    contention: RATE
      ? {
          executor: 'constant-arrival-rate',
          rate: RATE,
          timeUnit: '1s',
          duration: __ENV.DURATION || '30s',
          // đủ VU để giữ đúng nhịp kể cả khi latency tăng; thiếu VU thì k6 báo dropped_iterations
          preAllocatedVUs: Math.min(VUS, RATE * 2),
          maxVUs: VUS,
          gracefulStop: '10s',
        }
      : {
          executor: 'constant-vus',
          vus: VUS,
          duration: __ENV.DURATION || '30s',
          gracefulStop: '10s',
        },
  },
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    // Chốt chặn phía client: tổng số ghế giữ được không vượt số ghế đem bán.
    seats_won: [`count<=${HOT_SEATS}`],
    // Mọi response phải là 201 (thắng) hoặc 409 (thua); 429/5xx/timeout đều tính là bất thường.
    hold_unexpected: ['rate<0.01'],
    // Không phải ngưỡng thật: khai báo threshold cho sub-metric để summary in ra số request theo
    // từng loại lỗi (status 0 = lỗi mạng/timeout phía k6).
    ...Object.fromEntries(['0', '429', '500', '502', '503', '504'].map((s) => [`http_reqs{status:${s}}`, ['count>=0']])),
  },
};

export function setup() {
  const mode = RATE ? `open model ${RATE} req/s (<= ${VUS} VUs)` : `closed model ${VUS} VUs`;
  console.log(`contention: ${mode}, ${HOT_SEATS} hot seats, ${SEATS_PER_HOLD} seat(s)/hold, event ${eventId}`);
  return { run: runId() };
}

// Biến cấp module là riêng của từng VU.
let won = false;

export default function (data) {
  // Open model: mỗi lượt là user kế tiếp, không bỏ lượt (bỏ lượt sẽ làm RPS thật thấp hơn RATE).
  // User đã giữ được ghế mà tới lượt lại thì nhận 409 "already has an active hold", vẫn là 409 đúng.
  if (!RATE && won) {
    sleep(1);
    return;
  }
  const user = RATE ? users[exec.scenario.iterationInTest % users.length] : currentUser();
  const res = http.post(
    `${BASE_URL}/events/${eventId}/holds`,
    JSON.stringify({ seatIds: pickDistinct(hotSeats, SEATS_PER_HOLD) }),
    {
      headers: authHeaders(user, { 'idempotency-key': `ct-${data.run}-${__VU}-${__ITER}` }),
      tags: { name: 'POST /events/:id/holds' },
      // 409 là kết quả nghiệp vụ bình thường, không tính vào http_req_failed
      responseCallback: http.expectedStatuses(201, 409),
    },
  );

  const ok = check(res, { 'status is 201 or 409': (r) => r.status === 201 || r.status === 409 });
  holdUnexpected.add(!ok);
  if (res.status === 201) {
    won = true;
    holdWon.add(1);
    seatsWon.add(SEATS_PER_HOLD);
  } else if (res.status === 409) {
    holdConflict.add(1);
  }
  if (THINK_MS) sleep(THINK_MS / 1000);
}
