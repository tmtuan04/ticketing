# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Flash-sale ticketing system: NestJS + Go microservices talking over gRPC and Kafka (Redpanda). Core problems: many users racing for the same seat (no overselling), seat holds with TTL, and payments failing mid-flow (Saga + Transactional Outbox/Inbox). `PLAN.md` is the source of truth for scope, milestones and per-milestone "Trạng thái" notes recording where the implementation deliberately differs from the plan — read the relevant section before changing a service. Phases 0–2 (M0–M6) are done; Phase 3 (k6 load test + `verify.sql` invariants, `loadtest/k6/`) and Phase 4 (Kubernetes) are not yet implemented.

Code comments, docs, Makefile comments and PLAN.md are written in Vietnamese; follow that convention for comments. `docs/` is gitignored (personal study notes) — don't put committed deliverables there. `/main.go` at the root is an ignored scratch file, not part of the project.

## Commands

Everything is driven from the root `Makefile` (Git Bash on Windows). Nest services need Node 24 (`.nvmrc`); Nest CLI 12 does not run on Node 22.

- `make up` / `make down` / `make reset` (drops volumes) / `make logs` — full stack via `deploy/docker-compose.yml`. Compose runs all migrations and Redpanda topic creation itself.
- Fast-expiry testing: `HOLD_TTL=15s PAYMENT_TIMEOUT=20s make up`.
- `make proto` — `buf lint` + `buf generate` into `gen/go` and `gen/ts` (uses local plugins: `protoc-gen-go`, `protoc-gen-go-grpc`, root `node_modules` ts-proto). Never hand-edit `gen/`.
- `make sqlc` — regenerates `internal/repo` for inventory and order via Docker (`sqlc/sqlc:1.27.0`); in Git Bash may need `MSYS_NO_PATHCONV=1`. Never hand-edit `internal/repo/*.sql.go`; edit `queries/*.sql` / `migrations/` instead.
- Migrations: Go services use golang-migrate (`make migrate-order-new name=xxx`, `make migrate-inventory-new name=xxx`); Nest services use Prisma (`prisma/migrations`, `npx prisma migrate dev` inside the service). Host Postgres is on port **5440**.
- `make test` — all tests (Go integration + Nest unit + e2e). Requires Docker running (testcontainers).
- `make test-short` — unit tests only, no containers.
- `make e2e` — Postman CLI collection (`postman/ticketing-phase1.postman_collection.json`) against a running stack via the gateway. Expiry/payment-timeout folders only pass with the short-TTL `make up` above.

Single tests:
- Go: `cd services/order && go test -race -run TestName ./internal/service/` (add `-short` to skip container-backed tests, which check `testing.Short()`).
- Nest unit: `cd services/gateway && npm test -- src/auth/password.spec.ts` (or `-t "name"`). Run `npx prisma generate` first on a fresh checkout.
- Nest e2e: `cd services/gateway && npm run test:e2e -- test/orders.e2e-spec.ts`. Jest is invoked with `--experimental-vm-modules` from the root `node_modules` (npm workspaces).

## Architecture

```
client ─REST─> gateway (Nest :3000) ─gRPC─> catalog (Nest :50052) ─gRPC─> inventory (Go :50051)
                                    ─gRPC─> inventory
                                    ─gRPC─> order (Go :50053) ─gRPC─> inventory, catalog
order <──Redpanda (payment.commands / payment.events / order.events + *.dlq)──> payment (Nest :3001, REST called directly, not via gateway)
```

- One Postgres instance, one DB per service (`auth_db`, `catalog_db`, `inventory_db`, `order_db`, `payment_db`, created by `deploy/postgres/init.sql`). Go uses pgx/v5 + sqlc; Nest uses Prisma 7 with `@prisma/adapter-pg`.
- **Proto/codegen wiring:** `proto/` is shared. Go imports `github.com/tmtuan04/ticketing/gen/go/...` via `go.work` (`gen/go`, `services/inventory`, `services/order`). Nest services re-export ts-proto types through per-service `*.pb.ts` shims (e.g. `services/gateway/src/catalog/catalog.pb.ts` → `gen/ts/...`), but load `.proto` files at runtime with `@grpc/proto-loader` (`PROTO_DIR` + `protoPath`). Because of this, Nest `tsconfig.json` has `rootDir: ../..` and build output is `dist/services/<svc>/src/main.js`; Dockerfiles build with the repo root as context. ts-proto uses `useDate=false`: Timestamps are `{seconds, nanos}` (convert via helpers like `toIso`/`fromDate`).
- **Gateway:** custom JWT guard (no passport), refresh-token rotation with family revocation, scrypt passwords, RBAC (`@Roles('admin')`), Redis-backed throttler with per-route buckets (auth/hold/order) that fails open. Maps gRPC status → HTTP (`common/grpc-errors.ts`). Propagates `user_id` and `x-request-id` as gRPC metadata. Holds and orders require an `Idempotency-Key` header.
- **Inventory (the anti-oversell core):** Redis Lua scripts (`internal/redisx/*.lua`) are a fast atomic filter; Postgres conditional `UPDATE ... WHERE status='available' OR expired-held` is the final authority (rows affected must equal seat count, else rollback + release Redis keys). A reaper goroutine frees expired holds with `FOR UPDATE SKIP LOCKED`. `ExtendHold` only extends, capped by `MAX_HOLD_EXTENSION`.
- **Order (Saga orchestrator):** `CreateOrder` = GetHold → price from catalog → ExtendHold(`PAYMENT_TIMEOUT` + `HOLD_BUFFER`) → one transaction writing `orders` + `order_saga_log` + `outbox`. Idempotency is `UNIQUE(user_id, idempotency_key)` with order_id = UUIDv5(user, key). `internal/outbox` relays outbox rows to Redpanda (key = order_id). `internal/consumer` consumes `payment.events` (franz-go): per message one transaction doing inbox dedupe → `SELECT order FOR UPDATE` → inventory Confirm/ReleaseHold while holding the lock → state change + outbox. `internal/expiry` cancels orders past `payment_deadline`. State machine: `awaiting_payment → confirmed | cancelled | refunding → refunded` (no persisted `paid` state).
- **Payment:** kafkajs consumer of `payment.commands` (inbox + `payment_intents` UNIQUE `order_id`), mock gateway `POST /mock-gateway/pay/:intentId` that calls back `POST /payments/webhook` with HMAC-SHA256 signature over the raw body, outbox relay and webhook worker built on the custom `PollLoop` (not `@nestjs/schedule`). Must register the snappy codec (`kafkajs-snappy`) because franz-go compresses with snappy.
- **Retry/DLQ convention (same in Go and Nest):** contract-violating message → `<topic>.dlq` immediately; transient errors (Postgres/inventory down) → retry forever with backoff, never DLQ; other errors → `CONSUMER_MAX_ATTEMPTS` (5) then DLQ with `dlq_*` headers.
- All background workers (reaper, relays, expiry, webhook worker) use `FOR UPDATE SKIP LOCKED` so they are safe to run on multiple replicas; preserve this when adding workers.
- Go services share a layout (`cmd/server`, `internal/{config,grpcserver,service,repo}`, `migrations/`, `queries/`); images are distroless, so compose healthchecks call the binary's `healthcheck` subcommand.

## Testing notes

- Go integration tests use testcontainers (Postgres, Redis, Redpanda) — e.g. 1000 goroutines racing for 10 seats, saga branches, consumer/expiry races.
- Nest e2e tests start containers in `test/global-setup.ts`, run `prisma migrate deploy`, and stand up fake gRPC servers in-process (`test/helpers.ts`); they run `--runInBand`.
- When adding API flows, also extend the Postman collection (folders are numbered by milestone).
