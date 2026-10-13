COMPOSE := docker compose -f deploy/docker-compose.yml

INVENTORY_DB_URL ?= postgres://app:app@localhost:5440/inventory_db?sslmode=disable
INVENTORY_MIGRATIONS := services/inventory/migrations

.PHONY: up down reset logs ps proto proto-lint sqlc migrate migrate-inventory migrate-inventory-down migrate-inventory-new

up:
	$(COMPOSE) up -d --wait

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

# Chạy migration của mọi service (Phase 1: chỉ inventory)
migrate: migrate-inventory

migrate-inventory:
	migrate -path $(INVENTORY_MIGRATIONS) -database "$(INVENTORY_DB_URL)" up

# Lùi 1 migration
migrate-inventory-down:
	migrate -path $(INVENTORY_MIGRATIONS) -database "$(INVENTORY_DB_URL)" down 1

# Tạo file migration mới: make migrate-inventory-new name=add_xxx
migrate-inventory-new:
	migrate create -ext sql -dir $(INVENTORY_MIGRATIONS) -seq $(name)

# sqlc chạy qua Docker; dùng cho Phase 1 (mỗi service Go có sqlc.yaml riêng)
sqlc:
	docker run --rm -v "$(CURDIR)/services/inventory:/src" -w /src sqlc/sqlc:1.27.0 generate
