-- Chỉ xóa key còn thuộc hold này, không đụng key mà hold khác đã set sau khi key cũ hết hạn.
-- KEYS: key của từng ghế. ARGV[1]: hold_id. Trả về số key đã xóa.
--
-- Tình huống cần cẩn thận: hold A hết hạn -> key tự mất -> hold B giữ lại ghế đó (key = B).
-- Nếu sau đó A gọi DEL mù quáng thì sẽ xóa nhầm key của B. Vì vậy phải GET kiểm tra chủ
-- rồi mới DEL; làm trong Lua để GET + DEL là nguyên tử (không ai chen vào giữa).
local n = 0
for _, key in ipairs(KEYS) do
  if redis.call('GET', key) == ARGV[1] then
    redis.call('DEL', key)
    n = n + 1
  end
end
return n
