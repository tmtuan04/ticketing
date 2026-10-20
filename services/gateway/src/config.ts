import { Global, LogLevel, Module, type DynamicModule } from '@nestjs/common';
import { resolve } from 'node:path';

// Cấu hình đọc từ biến môi trường, giá trị mặc định dùng cho chạy local với `make up`.
// Lỗi được gom lại và báo một lần lúc khởi động (giống config.go của inventory).

export type RateLimitBucket = 'auth' | 'hold';

export interface AppConfig {
  port: number;
  databaseUrl: string;
  redisUrl: string;
  inventoryGrpcUrl: string;
  grpcTimeoutMs: number;
  protoDir: string;
  /** giá trị cho express `trust proxy`: false, số hop, hoặc danh sách subnet */
  trustProxy: boolean | number | string;
  jwt: {
    accessSecret: string;
    accessTtlSec: number;
    refreshTtlSec: number;
  };
  rateLimit: {
    windowMs: number;
    /** áp cho mọi route, theo user (đã đăng nhập) hoặc IP */
    default: number;
  } & Record<RateLimitBucket, number>;
  /** tạo sẵn tài khoản admin lúc khởi động nếu được cấu hình */
  admin?: { email: string; password: string };
  logLevels: LogLevel[];
}

export const APP_CONFIG = Symbol('APP_CONFIG');

const LOG_LEVELS: Record<string, LogLevel[]> = {
  debug: ['fatal', 'error', 'warn', 'log', 'debug'],
  info: ['fatal', 'error', 'warn', 'log'],
  warn: ['fatal', 'error', 'warn'],
  error: ['fatal', 'error'],
};

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const errs: string[] = [];
  const str = (key: string, def?: string): string => {
    const v = env[key];
    if (v !== undefined && v !== '') return v;
    if (def === undefined) errs.push(`${key}: required`);
    return def ?? '';
  };
  const int = (key: string, def: number): number => {
    const v = env[key];
    if (v === undefined || v === '') return def;
    const n = Number(v);
    if (!Number.isInteger(n) || n <= 0) {
      errs.push(`${key}: must be a positive integer, got "${v}"`);
      return def;
    }
    return n;
  };
  const dur = (key: string, def: string): number => {
    const v = str(key, def);
    const ms = parseDuration(v);
    if (ms === undefined) {
      errs.push(`${key}: must be a duration like 30s, 15m, 7d, got "${v}"`);
      return parseDuration(def)!;
    }
    return ms;
  };

  const accessSecret = str('JWT_ACCESS_SECRET');
  if (accessSecret && accessSecret.length < 32) {
    errs.push('JWT_ACCESS_SECRET: must be at least 32 characters');
  }
  const logLevel = str('LOG_LEVEL', 'info');
  if (!LOG_LEVELS[logLevel]) errs.push(`LOG_LEVEL: must be one of ${Object.keys(LOG_LEVELS)}`);

  const adminEmail = env.ADMIN_EMAIL;
  const adminPassword = env.ADMIN_PASSWORD;
  if (!!adminEmail !== !!adminPassword) {
    errs.push('ADMIN_EMAIL and ADMIN_PASSWORD must be set together');
  }

  const cfg: AppConfig = {
    port: int('PORT', 3000),
    databaseUrl: str('DATABASE_URL', 'postgresql://app:app@localhost:5440/auth_db'),
    redisUrl: str('REDIS_URL', 'redis://localhost:6379'),
    inventoryGrpcUrl: str('INVENTORY_GRPC_URL', 'localhost:50051'),
    grpcTimeoutMs: dur('GRPC_TIMEOUT', '3s'),
    protoDir: resolve(str('PROTO_DIR', '../../proto')),
    trustProxy: parseTrustProxy(str('TRUST_PROXY', 'false')),
    jwt: {
      accessSecret,
      accessTtlSec: dur('JWT_ACCESS_TTL', '15m') / 1000,
      refreshTtlSec: dur('REFRESH_TOKEN_TTL', '7d') / 1000,
    },
    rateLimit: {
      windowMs: dur('RATE_LIMIT_WINDOW', '60s'),
      default: int('RATE_LIMIT_DEFAULT', 120),
      auth: int('RATE_LIMIT_AUTH', 10),
      hold: int('RATE_LIMIT_HOLD', 10),
    },
    admin: adminEmail && adminPassword ? { email: adminEmail, password: adminPassword } : undefined,
    logLevels: LOG_LEVELS[logLevel] ?? LOG_LEVELS.info,
  };
  if (errs.length) throw new Error(`invalid config:\n  ${errs.join('\n  ')}`);
  return cfg;
}

const UNIT_MS: Record<string, number> = { ms: 1, s: 1e3, m: 60e3, h: 3600e3, d: 86400e3 };

export function parseDuration(v: string): number | undefined {
  const m = /^(\d+)(ms|s|m|h|d)$/.exec(v.trim());
  if (!m) return undefined;
  const ms = Number(m[1]) * UNIT_MS[m[2]];
  return ms > 0 ? ms : undefined;
}

function parseTrustProxy(v: string): boolean | number | string {
  if (v === 'true') return true;
  if (v === 'false') return false;
  return /^\d+$/.test(v) ? Number(v) : v;
}

@Global()
@Module({})
export class AppConfigModule {
  static forRoot(config: AppConfig): DynamicModule {
    return {
      module: AppConfigModule,
      providers: [{ provide: APP_CONFIG, useValue: config }],
      exports: [APP_CONFIG],
    };
  }
}
