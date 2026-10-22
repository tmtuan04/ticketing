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
