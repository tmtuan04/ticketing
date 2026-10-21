import {
  Catch,
  Injectable,
  Logger,
  type ArgumentsHost,
  type CallHandler,
  type ExecutionContext,
  type NestInterceptor,
  type RpcExceptionFilter,
} from '@nestjs/common';
import { Metadata, status, type ServiceError } from '@grpc/grpc-js';
import { RpcException } from '@nestjs/microservices';
import { firstValueFrom, tap, throwError, timeout, type Observable } from 'rxjs';

// Lỗi nghiệp vụ trả về dạng gRPC status; gateway map sang HTTP (INVALID_ARGUMENT => 400...).
const rpcError = (code: status) => (message: string) => new RpcException({ code, message });

export const invalidArg = rpcError(status.INVALID_ARGUMENT);
export const notFound = rpcError(status.NOT_FOUND);
export const alreadyExists = rpcError(status.ALREADY_EXISTS);
export const failedPrecondition = rpcError(status.FAILED_PRECONDITION);

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

/** Gọi unary RPC với timeout. Hết hạn thì hủy subscription (Nest cancel call gRPC). */
export function unary<T>(call: Observable<T>, timeoutMs: number): Promise<T> {
  return firstValueFrom(
    call.pipe(
      timeout({
        first: timeoutMs,
        with: () =>
          throwError(() => new RpcException({ code: status.DEADLINE_EXCEEDED, message: 'upstream timed out' })),
      }),
    ),
  );
}

/** Chuyển tiếp x-request-id sang service phía sau. */
export function forwardMetadata(incoming: Metadata | undefined): Metadata {
  const md = new Metadata();
  const id = incoming?.get('x-request-id')[0];
  if (id) md.set('x-request-id', String(id));
  return md;
}

/** Lỗi từ service phía sau chỉ giữ code khi đó là lỗi hạ tầng; còn lại là bug phía catalog. */
const PASS_THROUGH = new Set([status.UNAVAILABLE, status.DEADLINE_EXCEEDED, status.RESOURCE_EXHAUSTED]);

interface RpcErrorBody {
  code: status;
  message: string;
}

function toRpcError(err: unknown): RpcErrorBody | undefined {
  if (err instanceof RpcException) {
    const e = err.getError();
    if (typeof e === 'object' && e !== null && 'code' in e) return e as RpcErrorBody;
    return undefined;
  }
  if (isGrpcError(err) && PASS_THROUGH.has(err.code)) {
    return { code: err.code, message: 'upstream unavailable' };
  }
  if (isPrismaError(err, 'P2002')) return { code: status.ALREADY_EXISTS, message: 'already exists' };
  return undefined;
}

export function isPrismaError(err: unknown, code: string): boolean {
  return typeof err === 'object' && err !== null && (err as { code?: unknown }).code === code && 'clientVersion' in err;
}

/** Mọi lỗi không phải lỗi nghiệp vụ thành INTERNAL, không lộ chi tiết DB ra ngoài. */
@Catch()
export class AppRpcExceptionFilter implements RpcExceptionFilter {
  private readonly log = new Logger('Exceptions');

  catch(exception: unknown, _host: ArgumentsHost): Observable<never> {
    const known = toRpcError(exception);
    if (known) return throwError(() => known);
    this.log.error(exception instanceof Error ? (exception.stack ?? exception.message) : String(exception));
    return throwError(() => ({ code: status.INTERNAL, message: 'internal error' }));
  }
}

/** Log mỗi RPC kèm x-request-id do gateway truyền qua metadata (giống interceptor của inventory). */
@Injectable()
export class RpcLoggingInterceptor implements NestInterceptor {
  private readonly log = new Logger('RPC');

  intercept(ctx: ExecutionContext, next: CallHandler): Observable<unknown> {
    if (ctx.getType() !== 'rpc') return next.handle();
    const md = ctx.switchToRpc().getContext<Metadata>();
    const requestId = md?.get('x-request-id')[0];
    const method = `${ctx.getClass().name}.${ctx.getHandler().name}`;
    const start = process.hrtime.bigint();
    const done = (code: status) => {
      const entry = {
        msg: 'rpc',
        method,
        code: status[code],
        duration_ms: Number(process.hrtime.bigint() - start) / 1e6,
        request_id: requestId === undefined ? undefined : String(requestId),
      };
      if (code === status.INTERNAL || code === status.UNKNOWN) this.log.error(entry);
      else this.log.log(entry);
    };
    return next.handle().pipe(
      tap({
        complete: () => done(status.OK),
        error: (err: unknown) => done(toRpcError(err)?.code ?? status.INTERNAL),
      }),
    );
  }
}
