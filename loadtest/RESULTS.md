# Kết quả load test (Phase 3 / M7)

> **5.000 user tranh 100 ghế: 2.614 RPS, trung vị 240ms, 0 vé bán vượt.** Luồng mua trọn vẹn 500 user qua đủ 4 nhánh Saga: 0 lỗi, bán hết 100/100 ghế. Spike 0 → 4.000 user trong 10s với rate limit thật: 0 lỗi 5xx, hồi phục ngay với p95 7,8ms. Sau mọi lần chạy, cả 17 bất biến đều PASS.

File này ghi chi tiết số đo. README chỉ có bảng tóm tắt. Phần "khác plan" và lý do từng quyết định nằm ở mục **Trạng thái M7** của [PLAN.md](../PLAN.md).

## Môi trường đo

| | |
|---|---|
| Máy | Windows 11, Docker Desktop 27.4 (VM WSL2 12 vCPU / 8 GB) |
| Stack | `make loadtest-up` = `deploy/docker-compose.yml` + `deploy/docker-compose.loadtest.yml` |
| Gateway | 3 replica sau nginx (`gateway-lb`, `deploy/loadtest/nginx.conf`) |
| Bộ sinh tải | `grafana/k6:0.54.0` chạy trong network của compose (không qua port host), giới hạn riêng 2 CPU / 3 GB |
| Dữ liệu | `make seed`: mỗi lần một event mới **100 ghế**, **5.000 user** có JWT ký sẵn (không login trong lúc đo) |
| Postgres | giữ nguyên `fsync`/`synchronous_commit` như production |

Giới hạn tài nguyên từng container (cố định để kết quả tái lập được):

| Container | CPU | RAM |
|---|---|---|
| postgres | 2 | 1 GB |
| redis | 1 | 256 MB |
| redpanda | 1 | 1,5 GB |
| inventory (Go) | 2 | 256 MB |
| order (Go) | 1 | 256 MB |
| catalog (Nest) | 1 | 384 MB |
| payment (Nest) | 1 | 384 MB |
| gateway (Nest) × 3 | 1,5 mỗi replica | 512 MB mỗi replica |
| gateway-lb (nginx) | 1 | 256 MB |

Khác stack dev: rate limit nới lên 1.000.000 (trừ `spike`), `LOG_LEVEL=warn` và `PAYMENT_TIMEOUT=30s` cho order (để nhánh bỏ ngang/trả muộn của `full_flow` xong trong một lần chạy).

## Bất biến (`loadtest/verify.mjs`)

Ba DB tách riêng nên không có một file `verify.sql`. Kiểm tra trong từng DB viết bằng SQL, còn kiểm tra chéo DB thì so trong JS. Script thử lại tới khi ổn định (`VERIFY_TIMEOUT_SEC`, mặc định 90s) và thoát mã 1 nếu còn bất biến FAIL. Đã kiểm chứng script bắt được lỗi: sửa tay một ghế thành `sold` thì có 2 bất biến FAIL.

| # | Bất biến | Nhóm |
|---|---|---|
| 1 | `seat_inventory` có đúng số ghế của event | dữ liệu |
| 2 | không ghế nào thuộc 2 hold còn hiệu lực | không bán vượt |
| 3 | không ghế nào nằm trong 2 đơn `confirmed` | không bán vượt |
| 4 | tổng ghế `sold` ≤ sức chứa | không bán vượt |
| 5 | ghế `held` thuộc hold `active`, ghế `sold` thuộc hold `confirmed`, và hold chứa ghế đó | ghế ↔ hold |
| 6 | hold `confirmed` → mọi ghế của nó là `sold` bởi chính hold đó | ghế ↔ hold |
| 7 | số ghế của các đơn `confirmed` = số ghế `sold` | ghế ↔ đơn (chéo DB) |
| 8 | mỗi ghế của đơn `confirmed` đang `sold` bởi đúng hold của đơn | ghế ↔ đơn (chéo DB) |
| 9 | mọi đơn đều có payment intent | đơn ↔ tiền (chéo DB) |
| 10 | trạng thái đơn khớp intent (đã trả tiền mà không có ghế → `refunded`) | đơn ↔ tiền (chéo DB) |
| 11 | không còn ghế `held` quá hạn | reaper |
| 12 | không còn hold `active` quá hạn | reaper |
| 13 | không còn đơn `awaiting_payment` quá `payment_deadline` | timeout worker |
| 14 | không còn đơn ở trạng thái trung gian (`pending`/`paid`/`refunding`) | Saga |
| 15 | outbox của order đã publish hết | outbox |
| 16 | outbox của payment đã publish hết | outbox |
| 17 | webhook của cổng giả lập đã gửi hết | webhook |

## 1. `contention.js`: tranh một nhóm ghế hot

Mỗi VU là một user bấm giữ ghế liên tục, không nghỉ, cho tới khi giữ được. Mọi VU được khởi tạo trước nên tải lên đỉnh ngay lập tức. Mã 409 ở đây là kết quả đúng (người thua), không phải lỗi.

**Closed model** (mặc định): 5.000 VU trong 30s, 100 ghế hot.

| Chỉ số | Giá trị |
|---|---|
| Thông lượng | **2.614 RPS** |
| Mã trả về | chỉ có 201 và 409, **0 lỗi** (không 5xx, không lỗi kết nối) |
| Ghế giữ được | **100/100**, mỗi ghế đúng một chủ |
| Latency | trung vị 240ms, p90 395ms, p99 ~30s |
| verify | 17/17 PASS |

Ở mức này hệ thống quá tải khoảng 2 lần. Theo định luật Little, latency ≈ số VU / thông lượng, nên p99 chỉ đo độ dài hàng đợi. Log của nginx cho thấy request phải chờ trong gateway trước khi tới handler (Node phục vụ không công bằng khi quá tải). Chế độ này dùng để chứng minh tính đúng đắn và đo thông lượng tối đa, không dùng để đo latency.

**Open model** (`RATE=n`): bắn đúng n request/s, đo latency ở mức tải bền vững.

| Tải | Trung vị | p95 | p99 |
|---|---|---|---|
| 1.000 req/s | | 305ms | 1,49s |
| 2.000 req/s | 20ms | 260ms | 3,47s |

**Nút thắt tìm được:** khi chạy 1 replica gateway, một tiến trình Node bão hòa ở ~960 RPS (4,6% request bị 504/timeout), trong khi inventory mới dùng ~20% CPU. Vì vậy profile load test chạy 3 replica sau nginx. Khi đặt gateway sau LB còn phải sửa hai điểm (Phase 4 sau ingress cũng cần):

- `keepAliveTimeout` 65s, dài hơn keepalive 4s của nginx. Nếu không, Node đóng kết nối trước nginx và nginx nhận reset → 502.
- Listen backlog 4096 thay cho 511. Với 5.000 VU, hàng đợi accept bị tràn (ListenOverflows lên hàng chục nghìn), kernel bật SYN cookie rồi reset kết nối.

## 2. `full_flow.js`: luồng mua trọn vẹn qua Saga

Mỗi VU xem sơ đồ ghế → giữ ghế còn trống → tạo đơn → poll payment intent → trả tiền theo `MIX` → poll đơn tới trạng thái cuối rồi so với kết quả mong đợi của nhánh. Có vé thì dừng. Đơn bị hủy thì mua lại, vì ghế được nhả quay lại "chợ".

`MIX` mặc định 65/20/10/5:

| Nhánh | Hành vi | Kết quả mong đợi |
|---|---|---|
| success | trả tiền thành công | `confirmed` |
| fail | thẻ bị từ chối | `cancelled` (`payment_failed`), nhả ghế |
| abandon | không trả | `cancelled` (`payment_timeout`) do timeout worker |
| late | trả thành công nhưng webhook về sau deadline | `refunded` (`paid_after_cancel`) |

### Trước khi sửa: catalog là nút thắt

500 VU, cache lạnh. Sơ đồ ghế và `CreateOrder` (order gọi `GetEvent` + `ListSeats` để tính giá) đều đi qua catalog. Một tiến trình Node 1 CPU tốn ~5ms CPU cho mỗi RPC (Prisma + mã hóa của proto-loader), nên bão hòa ở ~100 lượt sơ đồ ghế mỗi giây.

| Chỉ số | Giá trị |
|---|---|
| Lỗi | **8.364 lỗi 504** (deadline 3s): 48% request sơ đồ ghế, 95% `POST /orders` |
| Bán được | **59/100 ghế** trong 2 phút |
| Sơ đồ ghế | trung vị 2,99s |
| verify | vẫn 17/17 PASS: quá tải chỉ làm mất doanh thu, không bán vượt hay mất tiền |

**Cách sửa:** cache dữ liệu catalog của event đã publish ở gateway (`PublishedCatalogCache`) và order (`priceCache`). Dữ liệu này không đổi sau publish nên không cần TTL hay invalidate. Khi cache còn trống, các request trùng được gộp lại (Promise dùng chung / `singleflight`), nên đợt mở bán chỉ gọi catalog một lần. Trạng thái ghế vẫn hỏi inventory ở mỗi request.

### Sau khi sửa

500 VU trên stack vừa khởi động (cache lạnh), chạy ~2 phút.

| Chỉ số | Giá trị |
|---|---|
| Request | 19.327, **0 lỗi** (không 429/5xx/lỗi kết nối), `flow_unexpected` 0% |
| Đơn tạo ra | 148, gồm 100 `confirmed`, 39 `cancelled` (fail + abandon), 9 `refunded` (late) |
| Bán được | **100/100 ghế**, `saga_stuck` = 0 |
| verify | 17/17 PASS |

Latency từng bước:

| Bước | Trung vị | p95 | p99 |
|---|---|---|---|
| `GET /events/:id/seats` | 4,5ms | 24ms | 2,09s |
| `POST /events/:id/holds` | 103ms | 2,38s | 2,64s |
| `POST /orders` | 605ms | 1,20s | 2,31s |
| `POST /mock-gateway/pay/:id` | 107ms | 495ms | 531ms |
| `GET /orders/:id` | 5,8ms | 33ms | 101ms |
| `GET /payments/orders/:id` | 130ms | 1,62s | 1,97s |

Thời gian Saga (từ lúc tạo đơn tới trạng thái cuối):

| Nhánh | Trung vị | p95 |
|---|---|---|
| success | 4,9s | 6,1s |
| fail | 5,0s | 6,3s |
| abandon | 31,2s (≈ `PAYMENT_TIMEOUT` 30s + timeout worker) | 32,3s |
| late | 36,2s (deadline + webhook trả muộn + hoàn tiền) | 36,4s |

Intent sẵn sàng sau khi tạo đơn (order outbox → Redpanda → payment): trung vị 2,1s, p95 4,1s. Ở 50 VU con số này chỉ ~1s, saga success/fail trung vị ~2,2s.

Đuôi p99 ~2s của sơ đồ ghế, giữ ghế và tạo đơn chỉ rơi vào đợt mở bán đầu tiên, lúc toàn bộ VU vào cùng lúc khi cache còn lạnh.

## 3. `spike.js`: tải tăng vọt với rate limit thật

Chạy với `RATE_LIMIT_HOLD=10 make loadtest-up` (10 lần giữ ghế/phút/user). Tải tăng từ 0 lên 4.000 user trong 10s, giữ đỉnh 30s rồi rút về 0. Mỗi VU bấm giữ ghế liên tục, không nghỉ. Sau đó là pha `recovery`: 20 req/s từ một nhóm user chưa bị chặn trong 20s. `setup()` gửi LIMIT+1 request để thăm dò. Nếu không thấy 429, script dừng và báo cách chạy lại.

| Chỉ số | Giá trị |
|---|---|
| Request | 96.409 (~96.000 trong 50s spike), **0 lỗi** ngoài 201/409/429 |
| Bị chặn 429 | 71.309 (**74%**) |
| Vi phạm rate limit | **0** (kiểm phía client theo từng VU, theo cửa sổ cố định) |
| Ghế giữ được | đúng **100/100** |
| Recovery | 401/401 thành công, trung vị 4,9ms, **p95 7,8ms**, p99 10,4ms |
| Container restart | 0 |
| verify | 17/17 PASS |

Latency theo mã trả về trong pha spike:

| Mã | Trung vị | p95 | p99 |
|---|---|---|---|
| 201 | 121ms | 256ms | 474ms |
| 409 | 357ms | 31,8s | 42,0s |
| 429 | 208ms | 459ms | 8,0s |

Đuôi latency lúc đỉnh vẫn dài vì 4.000 VU không nghỉ (closed model) vượt năng lực của 3 replica gateway. Mã 429 cũng không rẻ như mong đợi: gateway vẫn verify JWT và gọi Redis trước khi chặn. Muốn cắt tải rẻ hơn thì phải chặn sớm ở LB/ingress (ví dụ `limit_req` của nginx), việc này để cho Phase 4.

Chỉ đánh vào route giới hạn theo user (giữ ghế). Route công khai giới hạn theo IP, mà mọi VU của k6 đều ra từ cùng một IP. Gateway lấy IP từ hop nginx (`TRUST_PROXY=1`) nên client không giả được `X-Forwarded-For`, và đó là hành vi đúng về bảo mật.

## Chạy lại

```bash
make loadtest-up
make seed && make loadtest script=contention && make verify
RATE=2000 make loadtest script=contention          # open model (seed lại trước nếu muốn event mới)
make seed && make loadtest script=full_flow && make verify
RATE_LIMIT_HOLD=10 make loadtest-up
make seed && make loadtest script=spike && make verify
make loadtest-down && make up                      # quay về stack dev
```

Tham số của từng kịch bản (`VUS`, `RATE`, `DURATION`, `HOT_SEATS`, `MIX`, `PEAK`, ...) được giải thích ở đầu mỗi file trong `loadtest/k6/`. k6 ghi summary ra `loadtest/results/<script>.json`. File này bị gitignore và bị ghi đè sau mỗi lần chạy, nên các con số ở đây được chép lại từ những lần chạy nêu trên.
