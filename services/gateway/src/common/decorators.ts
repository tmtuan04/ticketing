import {
  BadRequestException,
  createParamDecorator,
  SetMetadata,
  type ExecutionContext,
} from '@nestjs/common';
import { Metadata } from '@grpc/grpc-js';
import type { RateLimitBucket } from '../config';
import type { AppRequest, Role } from './request';

export const IS_PUBLIC = 'isPublic';
/** Bỏ qua JwtAuthGuard. */
export const Public = () => SetMetadata(IS_PUBLIC, true);

export const ROLES = 'roles';
export const Roles = (...roles: Role[]) => SetMetadata(ROLES, roles);

export const RATE_LIMIT_BUCKET = 'rateLimitBucket';
/** Dùng giới hạn riêng (RATE_LIMIT_AUTH, RATE_LIMIT_HOLD...) thay cho giới hạn mặc định. */
export const RateLimit = (bucket: RateLimitBucket) => SetMetadata(RATE_LIMIT_BUCKET, bucket);

/** User đã xác thực; chỉ dùng trên route không @Public. */
export const CurrentUser = createParamDecorator((_: unknown, ctx: ExecutionContext) => {
  return ctx.switchToHttp().getRequest<AppRequest>().user!;
});

/** Metadata gRPC truyền xuống service phía sau: x-request-id và user-id. */
export const GrpcMeta = createParamDecorator((_: unknown, ctx: ExecutionContext) => {
  const req = ctx.switchToHttp().getRequest<AppRequest>();
  const md = new Metadata();
  md.set('x-request-id', req.id);
  if (req.user) md.set('user-id', req.user.id);
  return md;
});

const MAX_IDEMPOTENCY_KEY_LEN = 200;

/** Header Idempotency-Key, bắt buộc. */
export const IdempotencyKey = createParamDecorator((_: unknown, ctx: ExecutionContext) => {
  const key = ctx.switchToHttp().getRequest<AppRequest>().header('idempotency-key')?.trim();
  if (!key) throw new BadRequestException('Idempotency-Key header is required');
  if (key.length > MAX_IDEMPOTENCY_KEY_LEN) {
    throw new BadRequestException(`Idempotency-Key must be at most ${MAX_IDEMPOTENCY_KEY_LEN} characters`);
  }
  return key;
});
