-- Giữ tất cả ghế hoặc không giữ ghế nào.
-- KEYS: key của từng ghế. ARGV[1]: hold_id, ARGV[2]: TTL (ms).
-- Trả về vị trí (1-based) các ghế đang thuộc hold khác; mảng rỗng nghĩa là đã giữ đủ.
--
-- Vì sao dùng Lua: Redis chạy cả script như MỘT lệnh nguyên tử (atomic), không client nào
-- chen vào giữa. Nếu làm bằng nhiều lệnh GET/SET rời rạc từ Go, hai request có thể cùng thấy
-- ghế trống rồi cùng SET -> cả hai tưởng mình giữ được.
--
-- File này được nhúng vào binary bằng //go:embed trong redisx.go.
-- Ghi chú Lua: `local` = biến cục bộ; mảng (table) đánh chỉ số từ 1; `#t` = độ dài; `~=` là
-- "khác"; ipairs duyệt mảng theo thứ tự.

-- Bước 1: chỉ đọc. Tìm các ghế đang có chủ KHÁC hold này.
-- (Key trùng hold_id của chính mình = request retry cùng idempotency key -> không tính là bị chiếm.)
local taken = {}
for i, key in ipairs(KEYS) do
  local owner = redis.call('GET', key) -- key không tồn tại thì GET trả về false
  if owner and owner ~= ARGV[1] then
    taken[#taken + 1] = i
  end
end
-- Có ít nhất một ghế bị chiếm -> không ghi gì cả ("all or nothing").
if #taken > 0 then
  return taken
end
-- Bước 2: mọi ghế đều trống (hoặc đã là của mình) -> set tất cả, kèm TTL tính bằng mili giây.
-- Hết TTL mà không confirm thì key tự biến mất, ghế tự trống lại ở tầng Redis.
for _, key in ipairs(KEYS) do
  redis.call('SET', key, ARGV[1], 'PX', ARGV[2])
end
return taken -- mảng rỗng = thành công
