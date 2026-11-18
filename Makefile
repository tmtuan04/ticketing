COMPOSE := docker compose -f deploy/docker-compose.yml
# Stack load test = stack dev + override (nới rate limit, giới hạn CPU/RAM, container k6)
LOADTEST_COMPOSE := $(COMPOSE) -f deploy/docker-compose.loadtest.yml

INVENTORY_DB_URL ?= postgres://app:app@localhost:5440/inventory_db?sslmode=disable
INVENTORY_MIGRATIONS := services/inventory/migrations
GATEWAY_DB_URL ?= postgresql://app:app@localhost:5440/auth_db
CATALOG_DB_URL ?= postgresql://app:app@localhost:5440/catalog_db
PAYMENT_DB_URL ?= postgresql://app:app@localhost:5440/payment_db
ORDER_DB_URL ?= postgres://app:app@localhost:5440/order_db?sslmode=disable
ORDER_MIGRATIONS := services/order/migrations

.PHONY: up down reset logs ps proto proto-lint sqlc migrate migrate-inventory migrate-inventory-down migrate-inventory-new migrate-gateway migrate-catalog migrate-payment migrate-order migrate-order-down migrate-order-new test test-short test-inventory test-order test-gateway test-catalog test-payment e2e web web-build loadtest-up loadtest-down seed loadtest verify

up:
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

proto-lint:
	cd proto && buf lint

proto: proto-lint
	cd proto && buf generate
	cd gen/go && go mod tidy

# Chạy migration của mọi service (compose cũng tự chạy khi `make up`)
migrate: migrate-inventory migrate-order migrate-gateway migrate-catalog migrate-payment

migrate-inventory:
	migrate -path $(INVENTORY_MIGRATIONS) -database "$(INVENTORY_DB_URL)" up

# Lùi 1 migration
migrate-inventory-down:
	migrate -path $(INVENTORY_MIGRATIONS) -database "$(INVENTORY_DB_URL)" down 1

# Tạo file migration mới: make migrate-inventory-new name=add_xxx
migrate-inventory-new:
	migrate create -ext sql -dir $(INVENTORY_MIGRATIONS) -seq $(name)

migrate-order:
	migrate -path $(ORDER_MIGRATIONS) -database "$(ORDER_DB_URL)" up

migrate-order-down:
	migrate -path $(ORDER_MIGRATIONS) -database "$(ORDER_DB_URL)" down 1

migrate-order-new:
	migrate create -ext sql -dir $(ORDER_MIGRATIONS) -seq $(name)

migrate-gateway: export DATABASE_URL = $(GATEWAY_DB_URL)
migrate-gateway:
	cd services/gateway && npx prisma migrate deploy

migrate-catalog: export DATABASE_URL = $(CATALOG_DB_URL)
migrate-catalog:
	cd services/catalog && npx prisma migrate deploy

migrate-payment: export DATABASE_URL = $(PAYMENT_DB_URL)
migrate-payment:
	cd services/payment && npx prisma migrate deploy

# sqlc chạy qua Docker (mỗi service Go có sqlc.yaml riêng). Trong Git Bash cần MSYS_NO_PATHCONV=1.
sqlc:
	docker run --rm -v "$(CURDIR)/services/inventory:/src" -w /src sqlc/sqlc:1.27.0 generate
	docker run --rm -v "$(CURDIR)/services/order:/src" -w /src sqlc/sqlc:1.27.0 generate

# Integration/e2e test dùng testcontainers (cần Docker đang chạy). Service Nest cần Node 24 (.nvmrc).
test: test-inventory test-order test-gateway test-catalog test-payment

test-inventory:
	cd services/inventory && go test -race ./...

test-order:
	cd services/order && go test -race ./...

test-gateway:
	cd services/gateway && npx prisma generate && npm test && npm run test:e2e

test-catalog:
	cd services/catalog && npx prisma generate && npm test && npm run test:e2e

test-payment:
	cd services/payment && npx prisma generate && npm test && npm run test:e2e

# Chỉ unit test, bỏ qua test cần container
test-short:
	cd services/inventory && go test -short ./...
	cd services/order && go test -short ./...
	cd services/gateway && npm test
	cd services/catalog && npm test
	cd services/payment && npm test

# Luồng end-to-end (Phase 1 + tạo đơn + thanh toán + Saga) qua gateway trên stack đang chạy (`make up`). Folder
# Expiry và Payment timeout chỉ chạy khi stack dựng với TTL ngắn, vd `HOLD_TTL=15s PAYMENT_TIMEOUT=20s make up`.
e2e:
	postman collection run postman/ticketing-phase1.postman_collection.json

# Giao diện demo (web/, React + Vite). Dev server ở http://localhost:5173, proxy /api -> :3000, /pay -> :3001
# nên cần stack đang chạy (`make up`). `make up` cũng dựng sẵn bản build tĩnh ở http://localhost:4000.
web:
	cd web && npm install && npm run dev

web-build:
	cd web && npm install && npm run build

# ---- Phase 3: load test (loadtest/) ----
# Quy trình: `make loadtest-up` (một lần) -> `make seed` -> `make loadtest script=contention` -> `make verify`.
# Mỗi lần seed tạo một event mới 100 ghế + 5.000 user có JWT ký sẵn; nên seed lại trước mỗi lần chạy k6.
# k6 chạy trong container (không cần cài), kết quả JSON ghi vào loadtest/results/.
# Gateway chạy GATEWAY_REPLICAS (mặc định 3) replica sau nginx (gateway-lb giữ cổng 3000 ở host).
loadtest-up:
	$(LOADTEST_COMPOSE) up -d --build --wait --remove-orphans

# Quay về stack dev: `make loadtest-down && make up` (giữ nguyên volume, khác `make reset`)
loadtest-down:
	$(LOADTEST_COMPOSE) down --remove-orphans

seed:
	node loadtest/seed.mjs

# Tham số k6 truyền qua biến môi trường, vd: VUS=2000 HOT_SEATS=20 make loadtest script=contention
# hoặc open model (đo latency ở tải bền vững): RATE=1500 make loadtest script=contention
# Luồng Saga trọn vẹn: VUS=300 MIX=70,20,10,0 make loadtest script=full_flow (xem đầu file full_flow.js)
# Spike cần rate limit THẬT: `RATE_LIMIT_HOLD=10 make loadtest-up` rồi `make loadtest script=spike`
# (script tự thăm dò và dừng nếu gateway còn rate limit nới); xong thì `make loadtest-up` để nới lại.
script ?= contention
loadtest:
	MSYS_NO_PATHCONV=1 $(LOADTEST_COMPOSE) run --rm \
		-e VUS -e RATE -e DURATION -e HOT_SEATS -e SEATS_PER_HOLD -e THINK_MS \
		-e MIX -e POLL_MS -e INTENT_WAIT_MS -e SETTLE_WAIT_MS -e LATE_MARGIN_MS -e GRACEFUL_STOP \
		-e PEAK -e RAMP -e HOLD_PEAK -e RATE_LIMIT_HOLD -e RATE_LIMIT_WINDOW_SEC -e RECOVERY_RATE -e RECOVERY_FOR \
		k6 run --summary-export /loadtest/results/$(script).json /loadtest/k6/$(script).js

verify:
	node loadtest/verify.mjs
