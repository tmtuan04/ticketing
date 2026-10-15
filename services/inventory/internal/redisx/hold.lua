-- Giữ tất cả ghế hoặc không giữ ghế nào.
-- KEYS: key của từng ghế. ARGV[1]: hold_id, ARGV[2]: TTL (ms).
-- Trả về vị trí (1-based) các ghế đang thuộc hold khác; mảng rỗng nghĩa là đã giữ đủ.
local taken = {}
for i, key in ipairs(KEYS) do
  local owner = redis.call('GET', key)
  if owner and owner ~= ARGV[1] then
    taken[#taken + 1] = i
  end
end
if #taken > 0 then
  return taken
end
for _, key in ipairs(KEYS) do
  redis.call('SET', key, ARGV[1], 'PX', ARGV[2])
end
return taken
