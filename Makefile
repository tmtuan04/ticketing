COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: up down reset logs ps proto proto-lint sqlc

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

# sqlc chạy qua Docker; dùng cho Phase 1 (mỗi service Go có sqlc.yaml riêng)
sqlc:
	docker run --rm -v "$(CURDIR)/services/inventory:/src" -w /src sqlc/sqlc:1.27.0 generate
