import { Global, Inject, Logger, Module, OnApplicationShutdown } from '@nestjs/common';
import { Redis } from 'ioredis';
import { APP_CONFIG, type AppConfig } from '../config';

export const REDIS = Symbol('REDIS');

@Global()
@Module({
  providers: [
    {
      provide: REDIS,
      inject: [APP_CONFIG],
      useFactory: async (config: AppConfig) => {
        const log = new Logger('Redis');
        // Không xếp hàng lệnh khi mất kết nối: rate limit fail-open thay vì treo request.
        const redis = new Redis(config.redisUrl, {
          lazyConnect: true,
          enableOfflineQueue: false,
          maxRetriesPerRequest: 1,
        });
        redis.on('error', (err: Error) => log.warn(`redis error: ${err.message}`));
        try {
          await redis.connect();
        } catch (err) {
          // Redis chỉ phục vụ rate limit: thiếu Redis thì gateway vẫn chạy, ioredis tự kết nối lại.
          log.warn(`redis unavailable at startup, rate limiting disabled until it reconnects: ${err}`);
        }
        return redis;
      },
    },
  ],
  exports: [REDIS],
})
export class RedisModule implements OnApplicationShutdown {
  constructor(@Inject(REDIS) private readonly redis: Redis) {}

  async onApplicationShutdown() {
    await this.redis.quit().catch(() => this.redis.disconnect());
  }
}
