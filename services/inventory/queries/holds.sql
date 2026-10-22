-- name: CreateHold :one
-- Tạo hold mới. id do service sinh sẵn (UUID v5 từ user_id + idempotency_key).
-- Có thể vi phạm unique index (trùng idempotency key, hoặc user đã có hold active trong event);
-- service bắt lỗi 23505 này trong isConflict.
INSERT INTO holds (id, event_id, user_id, seat_ids, idempotency_key, expires_at)
VALUES (@id, @event_id, @user_id, @seat_ids, @idempotency_key, @expires_at)
RETURNING *;

-- name: GetHold :one
-- Đọc hold theo id. Không khóa dòng (chỉ đọc).
SELECT * FROM holds
WHERE id = $1;

-- name: GetHoldByIdempotencyKey :one
-- Tìm hold đã tạo bằng idempotency key này (để trả lại khi client retry).
-- Dùng unique index holds_user_idempotency_uidx.
SELECT * FROM holds
WHERE user_id = @user_id
  AND idempotency_key = @idempotency_key;

-- name: GetActiveHoldForUpdate :one
-- Hold active của user trong event (tối đa 1, do unique index đảm bảo). Nếu đã quá hạn mà
-- reaper chưa quét thì service expire nó trước khi tạo hold mới.
-- FOR UPDATE khóa dòng này tới hết transaction, để không transaction khác sửa nó cùng lúc.
SELECT * FROM holds
WHERE event_id = @event_id
  AND user_id = @user_id
  AND status = 'active'
FOR UPDATE;

-- name: MarkHoldReleased :one
-- Các câu MarkHold* chỉ chuyển trạng thái khi hợp lệ; không có dòng trả về (pgx.ErrNoRows)
-- thì service đọc lại hold để biết lý do. Trong transaction luôn khóa hold trước, ghế sau.
-- Điều kiện user_id = @user_id đảm bảo chỉ chủ hold mới nhả được.
UPDATE holds
SET status = 'released', updated_at = now()
WHERE id = @id
  AND user_id = @user_id
  AND status = 'active'
RETURNING *;

-- name: MarkHoldConfirmed :one
-- Chỉ confirm hold còn active VÀ chưa hết hạn (so với giờ của DB, không phụ thuộc giờ server app).
UPDATE holds
SET status = 'confirmed', updated_at = now()
WHERE id = @id
  AND status = 'active'
  AND expires_at > now()
RETURNING *;

-- name: ExtendHold :one
-- Kéo dài hạn của hold còn active và chưa hết hạn (Order gọi khi tạo đơn).
-- GREATEST: chỉ kéo dài, không rút ngắn; gọi lại với hạn nhỏ hơn thì giữ nguyên hạn cũ.
-- Không có dòng trả về thì service đọc lại hold để biết lý do (giống MarkHold*).
UPDATE holds
SET expires_at = GREATEST(expires_at, @expires_at::timestamptz),
    updated_at = now()
WHERE id = @id
  AND user_id = @user_id
  AND status = 'active'
  AND expires_at > now()
RETURNING *;

-- name: MarkHoldExpired :one
-- Chỉ expire hold active đã quá hạn. Còn hạn thì không có dòng nào -> service báo
-- ErrActiveHoldExists.
UPDATE holds
SET status = 'expired', updated_at = now()
WHERE id = @id
  AND status = 'active'
  AND expires_at < now()
RETURNING *;

-- name: ExpireDueHolds :many
-- Reaper: expire một lô hold quá hạn. SKIP LOCKED để nhiều instance chạy song song không
-- giẫm chân nhau và không phải chờ transaction Confirm/Release đang giữ khóa hold.
-- Subquery "due" chọn tối đa @batch_size hold quá hạn (cũ nhất trước) và khóa chúng;
-- dòng nào đang bị transaction khác khóa thì bỏ qua (SKIP LOCKED) thay vì đứng chờ.
-- UPDATE ... FROM nối bảng holds với "due" để chỉ cập nhật đúng các dòng đã chọn.
-- Dùng index một phần holds_expires_at_idx (chỉ chứa hold active) nên quét rất nhanh.
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
