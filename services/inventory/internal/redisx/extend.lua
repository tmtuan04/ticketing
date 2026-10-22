-- Gia hạn key của các ghế còn thuộc hold này tới một thời điểm tuyệt đối.
-- KEYS: key của từng ghế. ARGV[1]: hold_id, ARGV[2]: hạn mới (unix ms). Trả về số key đã gia hạn.
--
-- PEXPIREAT (thời điểm tuyệt đối) thay vì PEXPIRE (khoảng thời gian) để key hết hạn đúng bằng
-- expires_at trong Postgres, không lệch theo độ trễ mạng.
--
-- Key đã mất (Redis restart, hold vừa bị nhả) thì KHÔNG tạo lại: nếu hold vừa được nhả ngay sau
-- khi Postgres gia hạn, tạo lại key sẽ chặn nhầm ghế đang trống tới hết hạn mới. Thiếu key chỉ làm
-- request tranh ghế đi tiếp xuống Postgres, nơi vẫn chặn đúng.
local n = 0
for _, key in ipairs(KEYS) do
  if redis.call('GET', key) == ARGV[1] then
    redis.call('PEXPIREAT', key, ARGV[2])
    n = n + 1
  end
end
return n
