import { Logger } from '@nestjs/common';
import type { ThrottlerStorage } from '@nestjs/throttler';
import type { Redis } from 'ioredis';

// ThrottlerStorageRecord không được export ở entry của @nestjs/throttler
type ThrottlerStorageRecord = Awaited<ReturnType<ThrottlerStorage['increment']>>;

// Fixed window + block, atomic trong một lần gọi Redis để nhiều instance gateway dùng chung bộ đếm.
// KEYS[1] = bộ đếm, KEYS[2] = cờ block; ARGV = ttl ms, limit, blockDuration ms.
// Trả về {totalHits, pttl bộ đếm, isBlocked, pttl block}.
const INCREMENT_LUA = `
local blocked = redis.call('PTTL', KEYS[2])
if blocked > 0 then
  local hits = tonumber(redis.call('GET', KEYS[1]) or '0')
  return {hits, redis.call('PTTL', KEYS[1]), 1, blocked}
end
local hits = redis.call('INCR', KEYS[1])
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
  ttl = tonumber(ARGV[1])
end
if hits > tonumber(ARGV[2]) then
  redis.call('SET', KEYS[2], '1', 'PX', ARGV[3])
  return {hits, ttl, 1, tonumber(ARGV[3])}
end
return {hits, ttl, 0, 0}
`;

export class RedisThrottlerStorage implements ThrottlerStorage {
  private readonly log = new Logger(RedisThrottlerStorage.name);

  constructor(private readonly redis: Redis) {}

  async increment(
    key: string,
    ttl: number,
    limit: number,
    blockDuration: number,
    throttlerName: string,
  ): Promise<ThrottlerStorageRecord> {
    // hash tag {..} giữ hai key trên cùng slot nếu sau này dùng Redis Cluster
    const base = `throttle:{${throttlerName}:${key}}`;
    try {
      const [hits, pttl, blocked, blockPttl] = (await this.redis.eval(
        INCREMENT_LUA,
        2,
        `${base}:hits`,
        `${base}:block`,
        ttl,
        limit,
        blockDuration,
      )) as [number, number, number, number];
      return {
        totalHits: hits,
        timeToExpire: Math.ceil(Math.max(pttl, 0) / 1000),
        isBlocked: blocked === 1,
        timeToBlockExpire: Math.ceil(blockPttl / 1000),
      };
    } catch (err) {
      // Fail-open: rate limit là lớp bảo vệ, không phải điều kiện đúng đắn
      // (chống bán vượt nằm ở inventory). Redis sập thì cho request đi qua.
      this.log.warn(`rate limit skipped, redis error: ${(err as Error).message}`);
      return { totalHits: 0, timeToExpire: Math.ceil(ttl / 1000), isBlocked: false, timeToBlockExpire: 0 };
    }
  }
}
