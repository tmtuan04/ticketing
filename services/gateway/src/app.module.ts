import { Module, ValidationPipe, type DynamicModule } from '@nestjs/common';
import { APP_FILTER, APP_GUARD, APP_PIPE } from '@nestjs/core';
import { ThrottlerModule } from '@nestjs/throttler';
import type { Redis } from 'ioredis';
import { AuthModule } from './auth/auth.module';
import { JwtAuthGuard, RolesGuard } from './auth/guards';
import { AppExceptionFilter } from './common/grpc-errors';
import { APP_CONFIG, AppConfigModule, type AppConfig } from './config';
import { HealthController } from './health/health.controller';
import { InventoryModule } from './inventory/inventory.module';
import { PrismaModule } from './prisma/prisma.service';
import { AppThrottlerGuard } from './throttle/app-throttler.guard';
import { REDIS, RedisModule } from './throttle/redis.module';
import { RedisThrottlerStorage } from './throttle/redis-throttler.storage';

@Module({})
export class AppModule {
  static forRoot(config: AppConfig): DynamicModule {
    return {
      module: AppModule,
      imports: [
        AppConfigModule.forRoot(config),
        PrismaModule,
        RedisModule,
        ThrottlerModule.forRootAsync({
          inject: [APP_CONFIG, REDIS],
          useFactory: (cfg: AppConfig, redis: Redis) => ({
            throttlers: [{ name: 'default', ttl: cfg.rateLimit.windowMs, limit: cfg.rateLimit.default }],
            storage: new RedisThrottlerStorage(redis),
          }),
        }),
        AuthModule,
        InventoryModule,
      ],
      controllers: [HealthController],
      providers: [
        // Thứ tự quan trọng: xác thực trước để rate limit đếm theo user thay vì IP.
        { provide: APP_GUARD, useClass: JwtAuthGuard },
        { provide: APP_GUARD, useClass: RolesGuard },
        { provide: APP_GUARD, useClass: AppThrottlerGuard },
        {
          provide: APP_PIPE,
          useValue: new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }),
        },
        { provide: APP_FILTER, useClass: AppExceptionFilter },
      ],
    };
  }
}
