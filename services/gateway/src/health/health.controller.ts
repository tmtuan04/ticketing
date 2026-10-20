import { Controller, Get, Inject, ServiceUnavailableException } from '@nestjs/common';
import { SkipThrottle } from '@nestjs/throttler';
import type { Redis } from 'ioredis';
import { Public } from '../common/decorators';
import { PrismaService } from '../prisma/prisma.service';
import { REDIS } from '../throttle/redis.module';

@Controller('healthz')
export class HealthController {
  constructor(
    private readonly prisma: PrismaService,
    @Inject(REDIS) private readonly redis: Redis,
  ) {}

  /** Chỉ Postgres là bắt buộc; Redis mất thì gateway vẫn phục vụ (rate limit fail-open). */
  @Public()
  @SkipThrottle()
  @Get()
  async check() {
    try {
      await this.prisma.$queryRaw`SELECT 1`;
    } catch {
      throw new ServiceUnavailableException({ status: 'error', postgres: 'down' });
    }
    return { status: 'ok', postgres: 'up', redis: this.redis.status === 'ready' ? 'up' : 'down' };
  }
}
