-- name: InsertOutbox :exec
-- Luôn gọi bên trong transaction nghiệp vụ (cùng tx với thay đổi của orders).
INSERT INTO outbox (message_id, aggregate_id, topic, key, type, payload)
VALUES (@message_id, @aggregate_id, @topic, @key, @type, @payload);
