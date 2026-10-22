-- Hoàn tác 000001_init.up.sql (make migrate-inventory-down). Xóa bảng kéo theo xóa luôn index.
-- CẢNH BÁO: mất toàn bộ dữ liệu ghế và hold.
DROP TABLE IF EXISTS holds;
DROP TABLE IF EXISTS seat_inventory;
