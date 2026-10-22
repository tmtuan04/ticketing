-- name: InitSeats :execrows
-- Idempotent: ghế đã có thì bỏ qua. Trả về số ghế mới thêm.
-- unnest(mảng) biến mảng seat_ids thành nhiều dòng -> insert nhiều ghế trong 1 câu lệnh.
-- @x::uuid là ép kiểu để sqlc biết chính xác kiểu tham số Go cần sinh.
INSERT INTO seat_inventory (event_id, seat_id)
SELECT @event_id::uuid, unnest(@seat_ids::uuid[])
ON CONFLICT (event_id, seat_id) DO NOTHING;

-- name: CountSeatsByEvent :one
-- Tổng số ghế của event.
SELECT count(*) FROM seat_inventory
WHERE event_id = $1;

-- name: HoldSeats :many
-- Chốt chặn cuối sau Redis: chỉ lấy ghế còn trống hoặc đang giữ nhưng đã quá hạn.
-- Trả về các ghế lấy được; nếu thiếu so với yêu cầu thì service rollback cả transaction.
-- Quy ước khóa: mọi câu cập nhật nhiều ghế đều khóa theo thứ tự (event_id, seat_id) trong CTE
-- trước khi UPDATE, để các transaction tranh cùng tập ghế không deadlock lẫn nhau.
-- CTE (WITH target AS ...) là truy vấn con đặt tên: chọn và khóa (FOR UPDATE) các ghế hợp lệ,
-- sau đó UPDATE ... FROM target chỉ cập nhật đúng những ghế đó.
-- Khi hai transaction cùng tranh một ghế, transaction sau phải chờ khóa; khi được chạy tiếp,
-- Postgres kiểm tra lại điều kiện WHERE trên dữ liệu mới nhất, thấy ghế đã 'held' nên bỏ qua.
-- version tăng mỗi lần đổi trạng thái (tiện debug / optimistic locking sau này).
WITH target AS (
    SELECT event_id, seat_id
    FROM seat_inventory
    WHERE event_id = @event_id::uuid
      AND seat_id = ANY (@seat_ids::uuid[])
      AND (status = 'available' OR (status = 'held' AND held_until < now()))
    ORDER BY seat_id
    FOR UPDATE
)
UPDATE seat_inventory s
SET status     = 'held',
    hold_id    = @hold_id::uuid,
    held_until = @held_until::timestamptz,
    version    = s.version + 1,
    updated_at = now()
FROM target t
WHERE s.event_id = t.event_id
  AND s.seat_id = t.seat_id
RETURNING s.seat_id;

-- name: SellSeatsByHold :execrows
-- held -> sold. Service so số dòng với số ghế của hold; thiếu nghĩa là đã có ghế bị lấy mất.
-- Giữ nguyên hold_id trên ghế đã bán để biết ghế thuộc đơn nào (CHECK constraint bắt buộc).
WITH target AS (
    SELECT event_id, seat_id
    FROM seat_inventory
    WHERE hold_id = @hold_id::uuid
      AND status = 'held'
      AND held_until > now()
    ORDER BY event_id, seat_id
    FOR UPDATE
)
UPDATE seat_inventory s
SET status     = 'sold',
    held_until = NULL,
    version    = s.version + 1,
    updated_at = now()
FROM target t
WHERE s.event_id = t.event_id
  AND s.seat_id = t.seat_id;

-- name: ReleaseSeatsByHolds :execrows
-- Trả ghế về available. Chỉ đụng ghế vẫn còn thuộc các hold này: ghế quá hạn đã bị
-- hold khác lấy thì hold_id đã đổi nên không bị nhả nhầm.
-- Nhận mảng hold_ids để reaper nhả ghế của cả lô hold trong một câu lệnh.
WITH target AS (
    SELECT event_id, seat_id
    FROM seat_inventory
    WHERE hold_id = ANY (@hold_ids::uuid[])
      AND status = 'held'
    ORDER BY event_id, seat_id
    FOR UPDATE
)
UPDATE seat_inventory s
SET status     = 'available',
    hold_id    = NULL,
    held_until = NULL,
    version    = s.version + 1,
    updated_at = now()
FROM target t
WHERE s.event_id = t.event_id
  AND s.seat_id = t.seat_id;

-- name: ListSeatsByEvent :many
-- Trạng thái hiệu lực: ghế held đã quá hạn nhưng reaper chưa quét vẫn coi là còn trống.
-- ::text ép kết quả CASE về text để sqlc sinh field Status kiểu string.
SELECT seat_id,
       (CASE WHEN status = 'held' AND held_until < now() THEN 'available' ELSE status END)::text AS status
FROM seat_inventory
WHERE event_id = $1
ORDER BY seat_id;
