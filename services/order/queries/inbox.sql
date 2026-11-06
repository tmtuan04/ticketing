-- name: InsertInbox :execrows
-- Gọi trong CÙNG transaction với thay đổi nghiệp vụ. 0 dòng = message_id đã xử lý (relay publish
-- lại, hoặc consumer chết trước khi commit offset) -> bỏ qua.
INSERT INTO inbox (message_id, topic)
VALUES (@message_id, @topic)
ON CONFLICT DO NOTHING;
