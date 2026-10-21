COMPOSE := docker compose -f deploy/docker-compose.yml

INVENTORY_DB_URL ?= postgres://app:app@localhost:5440/inventory_db?sslmode=disable
INVENTORY_MIGRATIONS := services/inventory/migrations
GATEWAY_DB_URL ?= postgresql://app:app@localhost:5440/auth_db
CATALOG_DB_URL ?= postgresql://app:app@localhost:5440/catalog_db

.PHONY: up down reset logs ps proto proto-lint sqlc migrate migrate-inventory migrate-inventory-down migrate-inventory-new migrate-gateway migrate-catalog test test-short test-inventory test-gateway test-catalog

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
migrate: migrate-inventory migrate-gateway migrate-catalog

migrate-inventory:
	migrate -path $(INVENTORY_MIGRATIONS) -database "$(INVENTORY_DB_URL)" up

# Lùi 1 migration
migrate-inventory-down:
	migrate -path $(INVENTORY_MIGRATIONS) -database "$(INVENTORY_DB_URL)" down 1

# Tạo file migration mới: make migrate-inventory-new name=add_xxx
migrate-inventory-new:
	migrate create -ext sql -dir $(INVENTORY_MIGRATIONS) -seq $(name)

migrate-gateway: export DATABASE_URL = $(GATEWAY_DB_URL)
migrate-gateway:
	cd services/gateway && npx prisma migrate deploy

migrate-catalog: export DATABASE_URL = $(CATALOG_DB_URL)
migrate-catalog:
	cd services/catalog && npx prisma migrate deploy

# sqlc chạy qua Docker; dùng cho Phase 1 (mỗi service Go có sqlc.yaml riêng)
sqlc:
	docker run --rm -v "$(CURDIR)/services/inventory:/src" -w /src sqlc/sqlc:1.27.0 generate

# Integration/e2e test dùng testcontainers (cần Docker đang chạy). Service Nest cần Node 24 (.nvmrc).
test: test-inventory test-gateway test-catalog

test-inventory:
	cd services/inventory && go test -race ./...

test-gateway:
	cd services/gateway && npx prisma generate && npm test && npm run test:e2e

test-catalog:
	cd services/catalog && npx prisma generate && npm test && npm run test:e2e

# Chỉ unit test, bỏ qua test cần container
test-short:
	cd services/inventory && go test -short ./...
	cd services/gateway && npm test
	cd services/catalog && npm test
