-- Trạng thái từng ghế của từng event. Đây là nguồn dữ liệu gốc; Redis chỉ là lớp lọc nhanh.
CREATE TABLE seat_inventory (
    event_id   uuid        NOT NULL,
    seat_id    uuid        NOT NULL,
    status     text        NOT NULL DEFAULT 'available',
    hold_id    uuid,
    held_until timestamptz,
    version    bigint      NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, seat_id),
    CONSTRAINT seat_inventory_status_chk CHECK (status IN ('available', 'held', 'sold')),
    -- Bất biến: ghế trống thì không thuộc hold nào; ghế đang giữ phải có hold và hạn giữ;
    -- ghế đã bán phải biết thuộc hold nào.
    CONSTRAINT seat_inventory_hold_chk CHECK (
        (status = 'available' AND hold_id IS NULL AND held_until IS NULL)
        OR (status = 'held' AND hold_id IS NOT NULL AND held_until IS NOT NULL)
        OR (status = 'sold' AND hold_id IS NOT NULL)
    )
);

-- Reaper quét ghế held quá hạn.
CREATE INDEX seat_inventory_held_until_idx ON seat_inventory (held_until) WHERE status = 'held';
-- Release/Confirm tìm ghế theo hold.
CREATE INDEX seat_inventory_hold_id_idx ON seat_inventory (hold_id) WHERE hold_id IS NOT NULL;

-- Mỗi lần giữ chỗ của một user. id do ứng dụng sinh (cần có trước khi set key Redis).
CREATE TABLE holds (
    id              uuid        PRIMARY KEY,
    event_id        uuid        NOT NULL,
    user_id         uuid        NOT NULL,
    seat_ids        uuid[]      NOT NULL,
    status          text        NOT NULL DEFAULT 'active',
    idempotency_key text        NOT NULL,
    expires_at      timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT holds_status_chk CHECK (status IN ('active', 'released', 'confirmed', 'expired')),
    CONSTRAINT holds_seat_ids_chk CHECK (cardinality(seat_ids) > 0)
);

-- Idempotency theo phạm vi user: cùng user gửi lại cùng key thì nhận lại hold cũ.
CREATE UNIQUE INDEX holds_user_idempotency_uidx ON holds (user_id, idempotency_key);
-- Mỗi user chỉ có tối đa 1 hold active cho mỗi event, do DB đảm bảo.
CREATE UNIQUE INDEX holds_one_active_per_user_event_uidx ON holds (event_id, user_id) WHERE status = 'active';
-- Reaper quét hold active quá hạn.
CREATE INDEX holds_expires_at_idx ON holds (expires_at) WHERE status = 'active';
