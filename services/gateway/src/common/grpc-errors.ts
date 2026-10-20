import {
  ArgumentsHost,
  BadRequestException,
  Catch,
  ConflictException,
  ForbiddenException,
  GatewayTimeoutException,
  HttpException,
  HttpStatus,
  InternalServerErrorException,
  Logger,
  NotFoundException,
  ServiceUnavailableException,
  UnauthorizedException,
} from '@nestjs/common';
import { BaseExceptionFilter } from '@nestjs/core';
import { status, type ServiceError } from '@grpc/grpc-js';
import { firstValueFrom, throwError, timeout, type Observable } from 'rxjs';

/** Lỗi gRPC client của Nest trả về chính là ServiceError của grpc-js. */
export function isGrpcError(err: unknown): err is ServiceError {
  return (
    typeof err === 'object' &&
    err !== null &&
    typeof (err as ServiceError).code === 'number' &&
    'details' in err &&
    'metadata' in err
  );
}

/** Map gRPC status sang HTTP. Lỗi 5xx ẩn chi tiết từ service phía sau. */
export function grpcToHttp(err: ServiceError): HttpException {
  const details = err.details || undefined;
  switch (err.code) {
    case status.INVALID_ARGUMENT:
    case status.OUT_OF_RANGE:
      return new BadRequestException(details);
    case status.NOT_FOUND:
      return new NotFoundException(details);
    case status.ALREADY_EXISTS:
    case status.FAILED_PRECONDITION:
    case status.ABORTED:
      return new ConflictException(details);
    case status.PERMISSION_DENIED:
      return new ForbiddenException(details);
    case status.UNAUTHENTICATED:
      return new UnauthorizedException(details);
    case status.RESOURCE_EXHAUSTED:
      return new HttpException(details ?? 'too many requests', HttpStatus.TOO_MANY_REQUESTS);
    case status.DEADLINE_EXCEEDED:
      return new GatewayTimeoutException('upstream timed out');
    case status.UNAVAILABLE:
    case status.CANCELLED:
      return new ServiceUnavailableException('upstream unavailable');
    default:
      return new InternalServerErrorException();
  }
}

/**
 * Gọi unary RPC với timeout. Hết hạn thì hủy subscription (Nest cancel call gRPC) và trả 504.
 */
export function unary<T>(call: Observable<T>, timeoutMs: number): Promise<T> {
  return firstValueFrom(
    call.pipe(
      timeout({
        first: timeoutMs,
        with: () => throwError(() => new GatewayTimeoutException('upstream timed out')),
      }),
    ),
  );
}

@Catch()
export class AppExceptionFilter extends BaseExceptionFilter {
  private readonly log = new Logger('Exceptions');

  catch(exception: unknown, host: ArgumentsHost) {
    if (isGrpcError(exception)) {
      const http = grpcToHttp(exception);
      if (http.getStatus() >= 500) {
        this.log.error(`grpc ${status[exception.code]}: ${exception.details}`);
      }
      return super.catch(http, host);
    }
    return super.catch(exception, host);
  }
}
