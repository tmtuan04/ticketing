-- M6: hoàn tiền khi order gửi payment.refund_requested.
-- AlterEnum
ALTER TYPE "payment_status" ADD VALUE 'refunded';

-- AlterTable
ALTER TABLE "payment_intents" ADD COLUMN "refund_reason" TEXT NOT NULL DEFAULT '',
ADD COLUMN "refunded_at" TIMESTAMPTZ;
