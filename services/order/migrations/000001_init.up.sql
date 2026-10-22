-- Migration đầu tiên: schema cho order service (Saga orchestrator của Phase 2).
-- Chạy bằng golang-migrate (make migrate-order); sqlc cũng đọc file này để sinh code Go.

-- Đơn hàng. Một hold chỉ có một đơn; trạng thái đi theo máy trạng thái của Saga:
--   pending -> awaiting_payment -> paid -> confirmed
--   awaiting_payment -> cancelled             (thanh toán lỗi / quá payment_deadline)
--   paid -> refunding -> refunded             (đã trả tiền nhưng không xác nhận được ghế)
CREATE TABLE orders (
    id               uuid        PRIMARY KEY, -- UUID v5 từ (user_id, idempotency_key), xem service
    user_id          uuid        NOT NULL,
    event_id         uuid        NOT NULL,
    hold_id          uuid        NOT NULL,
    seat_ids         uuid[]      NOT NULL,
    amount           bigint      NOT NULL, -- đơn vị nhỏ nhất (VND: đồng), chốt lúc tạo đơn
    currency         text        NOT NULL,
    status           text        NOT NULL,
    payment_deadline timestamptz NOT NULL,
    failure_reason   text        NOT NULL DEFAULT '', -- lý do cancelled/refunded
    idempotency_key  text        NOT NULL,
    version          bigint      NOT NULL DEFAULT 0, -- tăng mỗi lần đổi trạng thái
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT orders_status_chk CHECK (status IN (
        'pending', 'awaiting_payment', 'paid', 'confirmed', 'cancelled', 'refunding', 'refunded'
    )),
    CONSTRAINT orders_amount_chk CHECK (amount >= 0),
    CONSTRAINT orders_seat_ids_chk CHECK (cardinality(seat_ids) > 0)
);

-- Idempotency theo phạm vi user (giống holds của inventory): gửi lại cùng key nhận lại đơn cũ.
-- Thay cho bảng idempotency_keys riêng trong PLAN: CreateOrder chỉ tạo đúng một dòng orders, nên
-- chính dòng đó là "response đã lưu".
CREATE UNIQUE INDEX orders_user_idempotency_uidx ON orders (user_id, idempotency_key);
-- Mỗi hold tối đa một đơn: chặn mua hai lần cùng một lượt giữ ghế.
CREATE UNIQUE INDEX orders_hold_uidx ON orders (hold_id);
-- Timeout worker (M6) quét đơn chờ thanh toán đã quá hạn.
CREATE INDEX orders_payment_deadline_idx ON orders (payment_deadline) WHERE status = 'awaiting_payment';

-- Nhật ký từng bước của Saga, chỉ ghi thêm (append-only). Phục vụ debug và audit:
-- "đơn này đã đi qua những bước nào, lúc nào, vì sao".
CREATE TABLE order_saga_log (
    id         bigserial   PRIMARY KEY,
    order_id   uuid        NOT NULL REFERENCES orders (id),
    step       text        NOT NULL, -- vd order_created, payment_succeeded, hold_confirmed
    status     text        NOT NULL, -- trạng thái đơn SAU bước này
    detail     jsonb       NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX order_saga_log_order_idx ON order_saga_log (order_id, id);

-- Transactional Outbox: message cần publish lên Redpanda được ghi CÙNG transaction với thay đổi
-- nghiệp vụ, relay (M4.4) đọc ra và publish sau. Không bao giờ có "đơn đã tạo mà message bị mất".
CREATE TABLE outbox (
    id           bigserial   PRIMARY KEY, -- thứ tự publish
    message_id   uuid        NOT NULL UNIQUE, -- consumer dùng để chống xử lý trùng (bảng inbox)
    aggregate_id uuid        NOT NULL, -- order_id
    topic        text        NOT NULL, -- vd payment.commands
    key          text        NOT NULL, -- partition key = order_id, giữ thứ tự message của một đơn
    type         text        NOT NULL, -- vd payment.requested (một topic chứa nhiều loại message)
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz -- NULL = chưa publish
);
-- Relay chỉ quét message chưa publish.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Inbox: message_id đã xử lý (M5). Consumer insert trong cùng transaction nghiệp vụ; trùng khóa
-- chính nghĩa là message đã xử lý rồi -> bỏ qua. At-least-once + inbox = hiệu ứng exactly-once.
CREATE TABLE inbox (
    message_id   uuid        PRIMARY KEY,
    topic        text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);
