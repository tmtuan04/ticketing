import { Global, LogLevel, Module, type DynamicModule } from '@nestjs/common';
import { resolve } from 'node:path';

// Cấu hình đọc từ biến môi trường, giá trị mặc định dùng cho chạy local với `make up`.
// Lỗi được gom lại và báo một lần lúc khởi động (giống gateway).

export interface AppConfig {
  grpcPort: number;
  databaseUrl: string;
  inventoryGrpcUrl: string;
  grpcTimeoutMs: number;
  protoDir: string;
  /** chặn sơ đồ ghế quá lớn: danh sách seat_ids gửi sang inventory phải vừa 1 message gRPC (4MB) */
  maxSeatsPerEvent: number;
  logLevels: LogLevel[];
}

export const APP_CONFIG = Symbol('APP_CONFIG');

const LOG_LEVELS: Record<string, LogLevel[]> = {
  debug: ['fatal', 'error', 'warn', 'log', 'debug'],
  info: ['fatal', 'error', 'warn', 'log'],
  warn: ['fatal', 'error', 'warn'],
  error: ['fatal', 'error'],
};

// 50k uuid (~37 byte mỗi cái) ≈ 1.9MB, còn dư so với giới hạn 4MB của gRPC
const MAX_SEATS_HARD_LIMIT = 50_000;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const errs: string[] = [];
  const str = (key: string, def: string): string => {
    const v = env[key];
    return v !== undefined && v !== '' ? v : def;
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
      errs.push(`${key}: must be a duration like 500ms, 3s, got "${v}"`);
      return parseDuration(def)!;
    }
    return ms;
  };

  const logLevel = str('LOG_LEVEL', 'info');
  if (!LOG_LEVELS[logLevel]) errs.push(`LOG_LEVEL: must be one of ${Object.keys(LOG_LEVELS)}`);

  const maxSeatsPerEvent = int('MAX_SEATS_PER_EVENT', 20_000);
  if (maxSeatsPerEvent > MAX_SEATS_HARD_LIMIT) {
    errs.push(`MAX_SEATS_PER_EVENT: must be at most ${MAX_SEATS_HARD_LIMIT}`);
  }

  const cfg: AppConfig = {
    grpcPort: int('GRPC_PORT', 50052),
    databaseUrl: str('DATABASE_URL', 'postgresql://app:app@localhost:5440/catalog_db'),
    inventoryGrpcUrl: str('INVENTORY_GRPC_URL', 'localhost:50051'),
    grpcTimeoutMs: dur('GRPC_TIMEOUT', '5s'),
    protoDir: resolve(str('PROTO_DIR', '../../proto')),
    maxSeatsPerEvent,
    logLevels: LOG_LEVELS[logLevel] ?? LOG_LEVELS.info,
  };
  if (errs.length) throw new Error(`invalid config:\n  ${errs.join('\n  ')}`);
  return cfg;
}

const UNIT_MS: Record<string, number> = { ms: 1, s: 1e3, m: 60e3, h: 3600e3 };

export function parseDuration(v: string): number | undefined {
  const m = /^(\d+)(ms|s|m|h)$/.exec(v.trim());
  if (!m) return undefined;
  const ms = Number(m[1]) * UNIT_MS[m[2]];
  return ms > 0 ? ms : undefined;
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
