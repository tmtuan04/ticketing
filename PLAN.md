# Plan: Hệ thống bán vé Flash Sale (NestJS + Go), Phase 1–3

## Context
Mục tiêu là một project microservice thiên về backend để đưa vào CV, giải quyết các bài toán thật: nhiều người tranh cùng một ghế, giữ chỗ có TTL, thanh toán lỗi giữa chừng (Saga/Outbox) và chứng minh **không bán vượt** bằng load test.
Thư mục `nestjs_golang/` đang trống. Máy đã có Node 22 và Docker 27, **chưa cài Go**.

Các lựa chọn đã chốt: **Redpanda** (tương thích Kafka API), **thư mục đơn giản + Makefile + buf**, **Prisma (Nest) + pgx/sqlc (Go)**.

Phạm vi: Phase 1 (core giữ ghế), Phase 2 (Order Saga + Payment + Outbox), Phase 3 (k6 + kiểm tra bất biến). Chưa làm Observability, Waiting Room, Notification.

---

## Kiến trúc & cấu trúc repo

```
nestjs_golang/
├── proto/                     # nguồn gRPC dùng chung
│   ├── buf.yaml, buf.gen.yaml
│   └── ticketing/{catalog,inventory,order}/v1/*.proto
├── gen/go/                    # code Go sinh từ proto (module riêng)
├── gen/ts/                    # code TS sinh bằng ts-proto (outputServices=grpc-js, nestJs=true)
├── services/
│   ├── gateway/    (NestJS)   REST công khai, Auth/User, JWT, rate limit, gọi gRPC xuống dưới
│   ├── catalog/    (NestJS)   Event, sơ đồ ghế, giá vé, gRPC server + REST admin qua gateway
│   ├── payment/    (NestJS)   Phase 2: cổng thanh toán giả lập, webhook, idempotency
│   ├── inventory/  (Go)       Giữ/nhả/xác nhận ghế: Redis Lua + Postgres làm nguồn dữ liệu gốc
│   └── order/      (Go)       Phase 2: tạo đơn, điều phối Saga, outbox relay
├── deploy/
│   ├── docker-compose.yml     postgres, redis, redpanda, redpanda-console, các service
│   └── postgres/init.sql      tạo DB riêng: auth_db, catalog_db, inventory_db, order_db, payment_db
├── loadtest/k6/               Phase 3
├── go.work                    gom gen/go, services/inventory, services/order
└── Makefile                   proto, sqlc, migrate, up/down, test, loadtest
```

**Giao tiếp:**
- Client → Gateway: REST/JSON.
- Gateway → Catalog/Inventory/Order: **gRPC**, propagate `user_id` và `x-request-id` qua metadata.
- Phase 2: Order ↔ Payment chạy **bất đồng bộ qua Redpanda**. Order → Inventory vẫn là gRPC đồng bộ, do orchestrator gọi.

**Thư viện:**
- Go: `grpc-go`, `pgx/v5`, `sqlc`, `go-redis/v9`, `franz-go`, `golang-migrate`, `log/slog`, `testcontainers-go`.
- Nest: `@nestjs/microservices` (gRPC transport), `@grpc/grpc-js`, `ts-proto`, `prisma`, `@nestjs/jwt` + passport, `@nestjs/throttler` (Redis storage), `kafkajs`, `class-validator`.

---

## Phase 0: Chuẩn bị (≈0.5 ngày)
1. Cài Go 1.23+, `buf`, `sqlc`, `protoc-gen-go`, `protoc-gen-go-grpc`, `golang-migrate`.
2. Tạo khung repo, `go.work`, `Makefile`, `.editorconfig`, `.gitignore`, rồi `git init`.
3. `docker-compose.yml` cho hạ tầng: Postgres 16 (1 instance, 5 database), Redis 7, Redpanda + Console.
4. `buf.gen.yaml` sinh code cho Go và TS. Target `make proto`.

## Phase 1: Core giữ ghế (≈1.5–2 tuần)

### 1.1 Gateway (NestJS, `auth_db`)
- Module `auth`: register, login, refresh token (rotation, lưu hash refresh token), RBAC với role `user` | `admin`.
- `JwtAuthGuard` toàn cục, decorator `@Roles('admin')`.
- `@nestjs/throttler` dùng Redis storage: rate limit theo user và IP, endpoint giữ ghế có giới hạn chặt hơn.
- Các gRPC client tới catalog và inventory. Exception filter map gRPC status sang HTTP (ví dụ `ALREADY_EXISTS`/`FAILED_PRECONDITION` → 409).

### 1.2 Catalog (NestJS, `catalog_db`)
- Bảng: `events`, `venues`, `seats` (section/row/number), `ticket_tiers` (giá).
- REST admin (qua gateway): tạo event, sinh sơ đồ ghế, đặt giá, **publish** event.
- gRPC: `GetEvent`, `ListSeats`.
- Khi publish, gọi `inventory.InitEventInventory(event_id, seat_ids[])` (idempotent). Phase 2 có thể chuyển sang outbox event.

### 1.3 Inventory (Go, `inventory_db` + Redis), trọng tâm của project
**Nguồn dữ liệu gốc là Postgres:**
```sql
seat_inventory(event_id, seat_id, status  -- available|held|sold
               , hold_id, held_until, version, PRIMARY KEY(event_id, seat_id))
holds(id, event_id, user_id, seat_ids[], status, expires_at, idempotency_key UNIQUE)
```
**Luồng `HoldSeats(user, event, seat_ids, idem_key)`:**
1. **Redis Lua** (atomic, đường nhanh): kiểm tra mọi key `seat:{event}:{seat}` đều trống. Nếu trống thì `SET ... PX ttl` cho tất cả, ngược lại trả về danh sách ghế bị chiếm. Phần lớn request tranh chấp bị loại ở đây và không chạm DB.
2. **Postgres** (chốt chặn cuối):
   `UPDATE seat_inventory SET status='held', hold_id=$1, held_until=$2 WHERE event_id=$3 AND seat_id=ANY($4) AND (status='available' OR (status='held' AND held_until<now()))`.
   Nếu `rowsAffected != len(seat_ids)` thì rollback và nhả các key Redis vừa set.
3. Insert vào `holds`. Trùng `idempotency_key` thì trả lại hold cũ.

**Các RPC khác:**
- `ReleaseHold(hold_id)`.
- `ConfirmHold(hold_id)`: `held → sold` với điều kiện `hold_id` khớp và `held_until > now()`.
- `GetHold`.

**Reaper goroutine:** mỗi vài giây quét `held_until < now()` bằng `FOR UPDATE SKIP LOCKED`, trả ghế về `available`. Redis key tự hết hạn nhờ PX.

**Giới hạn:** tối đa N ghế mỗi hold, mỗi user chỉ có 1 hold active cho mỗi event.

**Cấu trúc code:** `cmd/server`, `internal/{grpc,service,repo(sqlc),redisx(lua),reaper}`, `migrations/`, `queries/`.

### 1.4 Kiểm thử Phase 1
- Integration test Go với testcontainers (Postgres + Redis): **1000 goroutine tranh 10 ghế**, assert mỗi ghế có ≤ 1 holder và DB khớp Redis.
- Test TTL: hold xong chờ hết hạn, reaper trả ghế, user khác giữ được.
- e2e cho Nest bằng supertest: register, login, admin tạo và publish event, user giữ ghế.

**Hoàn thành Phase 1 khi:** `make up` chạy đủ stack, và giữ, nhả, hết hạn ghế qua REST hoạt động đúng.

---

## Phase 2: Order Saga + Payment + Outbox (≈2 tuần)

### 2.1 Order (Go, `order_db`): Saga orchestration
```sql
orders(id, user_id, event_id, hold_id UNIQUE, amount, status, payment_deadline, version)
order_saga_log(order_id, step, status, detail, created_at)   -- audit, phục vụ debug
idempotency_keys(key, user_id, request_hash, response, created_at)
outbox(id, aggregate_id, topic, key, payload jsonb, created_at, published_at)
inbox(message_id PK, processed_at)                           -- consumer idempotency
```
**Máy trạng thái:**
`PENDING → AWAITING_PAYMENT → PAID → CONFIRMED`; nhánh lỗi: `→ CANCELLED`, hoặc `PAID → REFUNDING → REFUNDED`.

| Bước | Hành động | Khi lỗi / bù trừ |
|---|---|---|
| 1 | `CreateOrder(hold_id, Idempotency-Key)`: verify hold qua gRPC, insert order + outbox `payment.requested` **trong cùng 1 transaction** | Hold không hợp lệ thì trả 409 |
| 2 | Consume `payment.succeeded` → `inventory.ConfirmHold` → `CONFIRMED`, outbox `order.confirmed` | Confirm thất bại (hold đã hết hạn) → outbox `payment.refund_requested` → `REFUNDED` |
| 3 | Consume `payment.failed` | `inventory.ReleaseHold` → `CANCELLED` |
| 4 | Timeout worker: quá `payment_deadline` | `ReleaseHold` + `CANCELLED`. Nếu payment thành công đến muộn thì xử lý như bước 2 (refund) |

**Ràng buộc:** TTL của hold phải **lớn hơn** payment deadline cộng một khoảng đệm. Khi tạo order sẽ gọi gia hạn hold, thêm RPC `ExtendHold` vào inventory.

### 2.2 Transactional Outbox + consumer (dùng chung pattern cho Go và Nest)
- **Relay:** poll `SELECT ... WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED`, publish lên Redpanda với key = `order_id` để giữ thứ tự theo đơn, sau đó set `published_at`. Đảm bảo at-least-once.
- **Consumer:** trong cùng transaction nghiệp vụ, insert `inbox(message_id)`; nếu trùng thì bỏ qua (exactly-once về mặt hiệu ứng).
- **Retry:** exponential backoff tối đa N lần, sau đó đẩy sang `<topic>.dlq` kèm header lỗi.
- **Topics:** `payment.commands` (requested / refund_requested), `payment.events` (succeeded / failed / refunded), `order.events`.

### 2.3 Payment (NestJS, `payment_db`)
- Consume `payment.requested`: tạo `payment_intent`, idempotent theo `order_id` (UNIQUE).
- **Cổng thanh toán giả lập:** `POST /mock-gateway/pay/:intentId { outcome: success|fail|timeout, delayMs }`. Sau đó cổng gọi **webhook** `POST /payments/webhook` có chữ ký **HMAC**.
- Webhook: verify chữ ký, dedup theo `provider_event_id`, cập nhật intent, ghi outbox `payment.succeeded|failed`.
- Endpoint REST do client gọi phải có header `Idempotency-Key`.
- Relay outbox chạy bằng `@nestjs/schedule` interval, publish qua kafkajs.

### 2.4 Kiểm thử Phase 2
- Happy path: hold → order → pay success → `CONFIRMED`, ghế ở trạng thái `sold`.
- Pay fail → `CANCELLED`, ghế trả về `available`.
- Payment thành công **sau** khi hold hết hạn → refund.
- Gửi trùng webhook hoặc message → chỉ xử lý 1 lần.
- **Chaos:** `docker stop payment` giữa luồng rồi start lại → Saga tiếp tục chạy (outbox chưa publish vẫn còn trong DB).
- Poison message → message nằm trong DLQ, xem được trên Redpanda Console.

---

## Phase 3: Load test + chứng minh bất biến (≈1 tuần)

### 3.1 Chuẩn bị
- Script seed: 1 event với **100 ghế**, **5.000 user**, sinh sẵn JWT ra file JSON để k6 đọc, tránh login làm nhiễu số đo.
- Profile `loadtest` trong compose: nới rate limit, giới hạn CPU/RAM cho từng container để kết quả tái lập được.

### 3.2 Kịch bản k6 (`loadtest/k6/`)
1. `contention.js`: 5.000 VU cùng lúc giữ **cùng một tập ghế hot** (ramp tức thì). Đo RPS, p95/p99, tỉ lệ 409.
2. `full_flow.js`: hold → order → mock pay (ngẫu nhiên 70% success, 20% fail, 10% không trả) → poll trạng thái order.
3. `spike.js`: 0 → peak trong 10s, kiểm tra rate limit và việc hệ thống không sập.

### 3.3 Kiểm tra bất biến (`loadtest/verify.sql` + script)
- Mỗi `(event_id, seat_id)` có tối đa 1 vé sold, và tổng số sold ≤ capacity.
- Số order `CONFIRMED` = số ghế `sold`.
- Mọi order có payment `succeeded` nhưng không có ghế đều phải ở trạng thái `REFUNDED`.
- Không còn ghế `held` quá hạn sau khi reaper chạy.
- Không còn outbox chưa publish sau khi hệ thống ổn định.

### 3.4 Kết quả
`docs/loadtest.md` ghi cấu hình máy, các số đo, ảnh chụp k6 summary, và câu chốt kiểu: *"5.000 user tranh 100 ghế: X RPS, p99 Y ms, 0 vé bán vượt"*.
Viết thêm `README.md` có sơ đồ kiến trúc và sequence diagram của Saga.

---

## Verification tổng
- `make up && make migrate && make seed` → toàn bộ stack healthy.
- `make test`: unit + integration bằng testcontainers cho Go, jest e2e cho Nest.
- Bộ Postman collection (có Postman CLI) chạy luồng end-to-end: `postman collection run`.
- `make loadtest && make verify` → mọi bất biến trong 3.3 đều PASS.

## Thứ tự thực hiện (milestone)
1. M0: repo, compose, proto/buf.
2. M1: Gateway auth.
3. M2: Catalog.
4. M3: Inventory + test tranh chấp.
5. M4: Order + outbox relay.
6. M5: Payment + webhook.
7. M6: Saga nhánh lỗi, DLQ, chaos.
8. M7: k6 + verify + docs.
