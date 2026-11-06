-- name: CreateOrder :one
-- id do service sinh (UUID v5 từ user_id + idempotency_key). Có thể vi phạm unique: trùng key
-- (request song song cùng key) hoặc hold đã có đơn; service bắt lỗi 23505 rồi phân loại.
INSERT INTO orders (id, user_id, event_id, hold_id, seat_ids, amount, currency, status,
                    payment_deadline, idempotency_key)
VALUES (@id, @user_id, @event_id, @hold_id, @seat_ids, @amount, @currency, @status,
        @payment_deadline, @idempotency_key)
RETURNING *;

-- name: GetOrder :one
SELECT * FROM orders
WHERE id = $1;

-- name: GetOrderByIdempotencyKey :one
-- Đơn đã tạo bằng key này (client retry). Dùng unique index orders_user_idempotency_uidx.
SELECT * FROM orders
WHERE user_id = @user_id
  AND idempotency_key = @idempotency_key;

-- name: GetOrderByHold :one
SELECT * FROM orders
WHERE hold_id = $1;

-- name: InsertSagaLog :exec
INSERT INTO order_saga_log (order_id, step, status, detail)
VALUES (@order_id, @step, @status, @detail);

-- name: LockOrder :one
-- Saga: khóa đơn trong suốt transaction xử lý một message (kể cả lúc gọi inventory). Consumer và
-- timeout worker không bao giờ cùng đổi một đơn: worker dùng SKIP LOCKED nên bỏ qua đơn đang bị khóa.
SELECT * FROM orders
WHERE id = $1
FOR UPDATE;

-- name: LockDueOrders :many
-- Timeout worker: khóa một lô đơn chờ thanh toán đã quá hạn (dùng orders_payment_deadline_idx).
-- SKIP LOCKED: nhiều replica chia nhau, và đơn consumer đang xử lý thì để consumer quyết định.
SELECT * FROM orders
WHERE status = 'awaiting_payment'
  AND payment_deadline < now()
ORDER BY payment_deadline
LIMIT @batch_size
FOR UPDATE SKIP LOCKED;

-- name: UpdateOrderStatus :one
-- Chỉ gọi khi đang giữ khóa dòng (LockOrder/LockDueOrders); service kiểm tra trạng thái nguồn trước.
UPDATE orders
SET status         = @status,
    failure_reason = @failure_reason,
    version        = version + 1,
    updated_at     = now()
WHERE id = @id
RETURNING *;
