-- CreateSchema
CREATE SCHEMA IF NOT EXISTS "public";

-- CreateEnum
CREATE TYPE "payment_status" AS ENUM ('requires_payment', 'succeeded', 'failed');

-- CreateEnum
CREATE TYPE "charge_outcome" AS ENUM ('success', 'fail', 'timeout');

-- CreateTable
CREATE TABLE "payment_intents" (
    "id" UUID NOT NULL,
    "order_id" UUID NOT NULL,
    "user_id" UUID NOT NULL,
    "amount" BIGINT NOT NULL,
    "currency" CHAR(3) NOT NULL,
    "status" "payment_status" NOT NULL DEFAULT 'requires_payment',
    "payment_deadline" TIMESTAMPTZ NOT NULL,
    "failure_reason" TEXT NOT NULL DEFAULT '',
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ NOT NULL,

    CONSTRAINT "payment_intents_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "webhook_events" (
    "provider_event_id" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "received_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "webhook_events_pkey" PRIMARY KEY ("provider_event_id")
);

-- CreateTable
CREATE TABLE "outbox" (
    "id" BIGSERIAL NOT NULL,
    "message_id" UUID NOT NULL,
    "aggregate_id" UUID NOT NULL,
    "topic" TEXT NOT NULL,
    "key" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "published_at" TIMESTAMPTZ,

    CONSTRAINT "outbox_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "inbox" (
    "message_id" UUID NOT NULL,
    "topic" TEXT NOT NULL,
    "processed_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "inbox_pkey" PRIMARY KEY ("message_id")
);

-- CreateTable
CREATE TABLE "mock_charges" (
    "id" UUID NOT NULL,
    "intent_id" UUID NOT NULL,
    "idempotency_key" TEXT NOT NULL,
    "outcome" "charge_outcome" NOT NULL,
    "amount" BIGINT NOT NULL,
    "currency" CHAR(3) NOT NULL,
    "deliver_at" TIMESTAMPTZ NOT NULL,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "delivered_at" TIMESTAMPTZ,
    "last_error" TEXT NOT NULL DEFAULT '',
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "mock_charges_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "payment_intents_order_id_key" ON "payment_intents"("order_id");

-- CreateIndex
CREATE UNIQUE INDEX "outbox_message_id_key" ON "outbox"("message_id");

-- CreateIndex
CREATE UNIQUE INDEX "mock_charges_intent_id_key" ON "mock_charges"("intent_id");

-- AddForeignKey
ALTER TABLE "mock_charges" ADD CONSTRAINT "mock_charges_intent_id_fkey" FOREIGN KEY ("intent_id") REFERENCES "payment_intents"("id") ON DELETE RESTRICT ON UPDATE CASCADE;


-- Viết tay (schema.prisma không khai báo được partial index): relay outbox và worker gửi webhook
-- của cổng giả lập chỉ quét các dòng còn việc phải làm, giống outbox_unpublished_idx của order.
CREATE INDEX "outbox_unpublished_idx" ON "outbox"("id") WHERE "published_at" IS NULL;
CREATE INDEX "mock_charges_pending_delivery_idx" ON "mock_charges"("deliver_at")
    WHERE "delivered_at" IS NULL AND "outcome" <> 'timeout';
