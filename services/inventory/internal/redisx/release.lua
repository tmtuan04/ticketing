-- Chỉ xóa key còn thuộc hold này, không đụng key mà hold khác đã set sau khi key cũ hết hạn.
-- KEYS: key của từng ghế. ARGV[1]: hold_id. Trả về số key đã xóa.
local n = 0
for _, key in ipairs(KEYS) do
  if redis.call('GET', key) == ARGV[1] then
    redis.call('DEL', key)
    n = n + 1
  end
end
return n
