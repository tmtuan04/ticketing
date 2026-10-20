import { Metadata, status, type ServiceError } from '@grpc/grpc-js';
import { GatewayTimeoutException } from '@nestjs/common';
import { NEVER, of } from 'rxjs';
import { grpcToHttp, isGrpcError, unary } from './grpc-errors';

function grpcError(code: status, details = 'boom'): ServiceError {
  return Object.assign(new Error(details), { code, details, metadata: new Metadata() });
}

describe('grpcToHttp', () => {
  it.each([
    [status.INVALID_ARGUMENT, 400],
    [status.NOT_FOUND, 404],
    [status.ALREADY_EXISTS, 409],
    [status.FAILED_PRECONDITION, 409],
    [status.PERMISSION_DENIED, 403],
    [status.UNAUTHENTICATED, 401],
    [status.RESOURCE_EXHAUSTED, 429],
    [status.DEADLINE_EXCEEDED, 504],
    [status.UNAVAILABLE, 503],
    [status.INTERNAL, 500],
    [status.UNKNOWN, 500],
  ])('gRPC %s => HTTP %d', (code, http) => {
    expect(grpcToHttp(grpcError(code)).getStatus()).toBe(http);
  });

  it('passes business details through for 4xx', () => {
    expect(grpcToHttp(grpcError(status.NOT_FOUND, 'hold not found')).message).toBe('hold not found');
  });

  it('hides upstream details for 5xx', () => {
    expect(grpcToHttp(grpcError(status.INTERNAL, 'pq: connection refused')).message).not.toContain('pq');
  });
});

describe('isGrpcError', () => {
  it('distinguishes grpc errors from ordinary errors', () => {
    expect(isGrpcError(grpcError(status.NOT_FOUND))).toBe(true);
    expect(isGrpcError(new Error('x'))).toBe(false);
    expect(isGrpcError({ code: 5 })).toBe(false);
    expect(isGrpcError(null)).toBe(false);
  });
});

describe('unary', () => {
  it('resolves with the first value', async () => {
    await expect(unary(of(42), 100)).resolves.toBe(42);
  });

  it('fails with 504 when the call outlives the timeout', async () => {
    await expect(unary(NEVER, 20)).rejects.toBeInstanceOf(GatewayTimeoutException);
  });
});
