import { BadRequestException, createParamDecorator, type ExecutionContext } from '@nestjs/common';
import type { Request } from 'express';

const MAX_IDEMPOTENCY_KEY_LEN = 200;

/** Header Idempotency-Key, bắt buộc (giống gateway). */
export const IdempotencyKey = createParamDecorator((_: unknown, ctx: ExecutionContext) => {
  const key = ctx.switchToHttp().getRequest<Request>().header('idempotency-key')?.trim();
  if (!key) throw new BadRequestException('Idempotency-Key header is required');
  if (key.length > MAX_IDEMPOTENCY_KEY_LEN) {
    throw new BadRequestException(`Idempotency-Key must be at most ${MAX_IDEMPOTENCY_KEY_LEN} characters`);
  }
  return key;
});
