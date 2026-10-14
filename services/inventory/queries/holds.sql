-- name: CreateHold :one
INSERT INTO holds (id, event_id, user_id, seat_ids, idempotency_key, expires_at)
VALUES (@id, @event_id, @user_id, @seat_ids, @idempotency_key, @expires_at)
RETURNING *;

-- name: GetHold :one
SELECT * FROM holds
WHERE id = $1;

-- name: GetHoldByIdempotencyKey :one
SELECT * FROM holds
WHERE user_id = @user_id
  AND idempotency_key = @idempotency_key;

-- name: GetActiveHoldForUpdate :one
-- Hold active của user trong event (tối đa 1, do unique index đảm bảo). Nếu đã quá hạn mà
-- reaper chưa quét thì service expire nó trước khi tạo hold mới.
SELECT * FROM holds
WHERE event_id = @event_id
  AND user_id = @user_id
  AND status = 'active'
FOR UPDATE;

-- name: MarkHoldReleased :one
-- Các câu MarkHold* chỉ chuyển trạng thái khi hợp lệ; không có dòng trả về (pgx.ErrNoRows)
-- thì service đọc lại hold để biết lý do. Trong transaction luôn khóa hold trước, ghế sau.
UPDATE holds
SET status = 'released', updated_at = now()
WHERE id = @id
  AND user_id = @user_id
  AND status = 'active'
RETURNING *;

-- name: MarkHoldConfirmed :one
UPDATE holds
SET status = 'confirmed', updated_at = now()
WHERE id = @id
  AND status = 'active'
  AND expires_at > now()
RETURNING *;

-- name: MarkHoldExpired :one
UPDATE holds
SET status = 'expired', updated_at = now()
WHERE id = @id
  AND status = 'active'
  AND expires_at < now()
RETURNING *;

-- name: ExpireDueHolds :many
-- Reaper: expire một lô hold quá hạn. SKIP LOCKED để nhiều instance chạy song song không
-- giẫm chân nhau và không phải chờ transaction Confirm/Release đang giữ khóa hold.
UPDATE holds h
SET status = 'expired', updated_at = now()
FROM (
    SELECT id
    FROM holds
    WHERE status = 'active'
      AND expires_at < now()
    ORDER BY expires_at
    LIMIT @batch_size
    FOR UPDATE SKIP LOCKED
) due
WHERE h.id = due.id
RETURNING h.id, h.event_id, h.seat_ids;
