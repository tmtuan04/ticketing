import { Inject, Injectable } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import {
  InjectThrottlerOptions,
  InjectThrottlerStorage,
  ThrottlerGuard,
  type ThrottlerModuleOptions,
  type ThrottlerRequest,
  type ThrottlerStorage,
} from '@nestjs/throttler';
import { RATE_LIMIT_BUCKET } from '../common/decorators';
import type { AppRequest } from '../common/request';
import { APP_CONFIG, type AppConfig, type RateLimitBucket } from '../config';

/**
 * Đếm theo user khi đã đăng nhập, theo IP khi chưa. Chạy sau JwtAuthGuard để req.user đã có.
 * Route gắn @RateLimit(bucket) dùng giới hạn của bucket đó thay cho giới hạn mặc định.
 */
@Injectable()
export class AppThrottlerGuard extends ThrottlerGuard {
  constructor(
    @InjectThrottlerOptions() options: ThrottlerModuleOptions,
    @InjectThrottlerStorage() storage: ThrottlerStorage,
    reflector: Reflector,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {
    super(options, storage, reflector);
  }

  protected async getTracker(req: Record<string, any>): Promise<string> {
    const user = (req as AppRequest).user;
    return user ? `user:${user.id}` : `ip:${await super.getTracker(req)}`;
  }

  protected async handleRequest(props: ThrottlerRequest): Promise<boolean> {
    const bucket = this.reflector.getAllAndOverride<RateLimitBucket | undefined>(RATE_LIMIT_BUCKET, [
      props.context.getHandler(),
      props.context.getClass(),
    ]);
    if (bucket) props = { ...props, limit: this.config.rateLimit[bucket] };
    return super.handleRequest(props);
  }
}
