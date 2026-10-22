-- name: InsertOutbox :exec
-- Luôn gọi bên trong transaction nghiệp vụ (cùng tx với thay đổi của orders).
INSERT INTO outbox (message_id, aggregate_id, topic, key, type, payload)
VALUES (@message_id, @aggregate_id, @topic, @key, @type, @payload);

-- name: LockUnpublished :many
-- Relay: khóa một lô message chưa publish, cũ nhất trước. SKIP LOCKED: nhiều relay (nhiều
-- replica order) chạy song song thì mỗi relay lấy một lô khác nhau, không publish trùng và không
-- đứng chờ nhau. Khóa giữ tới khi transaction của relay COMMIT (sau khi đã publish xong).
SELECT id, message_id, topic, key, type, payload, created_at
FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT @batch_size
FOR UPDATE SKIP LOCKED;

-- name: MarkPublished :exec
UPDATE outbox
SET published_at = now()
WHERE id = ANY (@ids::bigint[]);
