-- name: InitSeats :execrows
-- Idempotent: ghế đã có thì bỏ qua. Trả về số ghế mới thêm.
INSERT INTO seat_inventory (event_id, seat_id)
SELECT @event_id::uuid, unnest(@seat_ids::uuid[])
ON CONFLICT (event_id, seat_id) DO NOTHING;

-- name: CountSeatsByEvent :one
SELECT count(*) FROM seat_inventory
WHERE event_id = $1;

-- name: HoldSeats :many
-- Chốt chặn cuối sau Redis: chỉ lấy ghế còn trống hoặc đang giữ nhưng đã quá hạn.
-- Trả về các ghế lấy được; nếu thiếu so với yêu cầu thì service rollback cả transaction.
-- Quy ước khóa: mọi câu cập nhật nhiều ghế đều khóa theo thứ tự (event_id, seat_id) trong CTE
-- trước khi UPDATE, để các transaction tranh cùng tập ghế không deadlock lẫn nhau.
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
SELECT seat_id,
       (CASE WHEN status = 'held' AND held_until < now() THEN 'available' ELSE status END)::text AS status
FROM seat_inventory
WHERE event_id = $1
ORDER BY seat_id;
