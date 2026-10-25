import { Global, LogLevel, Module, type DynamicModule } from '@nestjs/common';

// Cấu hình đọc từ biến môi trường, giá trị mặc định dùng cho chạy local với `make up`.
// Lỗi được gom lại và báo một lần lúc khởi động (giống gateway).

export interface AppConfig {
  port: number;
  databaseUrl: string;
  kafka: {
    brokers: string[];
    clientId: string;
    /** consumer group đọc payment.commands; nhiều replica cùng group chia nhau partition */
    groupId: string;
  };
  outbox: {
    intervalMs: number;
    batchSize: number;
  };
  webhook: {
    /** HMAC-SHA256 dùng chung giữa cổng giả lập (ký) và webhook (xác minh) */
    secret: string;
    /** địa chỉ cổng giả lập gọi webhook, mặc định là chính service này */
    url: string;
    /** lệch thời gian tối đa của header chữ ký, chặn replay webhook cũ */
    toleranceSec: number;
  };
  mockGateway: {
    pollIntervalMs: number;
    /** delayMs lớn nhất client được chọn khi giả lập thanh toán */
    maxDelayMs: number;
    deliveryTimeoutMs: number;
  };
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
      errs.push(`${key}: must be a duration like 500ms, 30s, 5m, got "${v}"`);
      return parseDuration(def)!;
    }
    return ms;
  };

  const webhookSecret = str('WEBHOOK_SECRET');
  if (webhookSecret && webhookSecret.length < 32) {
    errs.push('WEBHOOK_SECRET: must be at least 32 characters');
  }
  const logLevel = str('LOG_LEVEL', 'info');
  if (!LOG_LEVELS[logLevel]) errs.push(`LOG_LEVEL: must be one of ${Object.keys(LOG_LEVELS)}`);

  const port = int('PORT', 3001);
  const cfg: AppConfig = {
    port,
    databaseUrl: str('DATABASE_URL', 'postgresql://app:app@localhost:5440/payment_db'),
    kafka: {
      brokers: str('KAFKA_BROKERS', 'localhost:19092')
        .split(',')
        .map((b) => b.trim())
        .filter(Boolean),
      clientId: str('KAFKA_CLIENT_ID', 'payment'),
      groupId: str('KAFKA_GROUP_ID', 'payment'),
    },
    outbox: {
      intervalMs: dur('OUTBOX_INTERVAL', '500ms'),
      batchSize: int('OUTBOX_BATCH_SIZE', 100),
    },
    webhook: {
      secret: webhookSecret,
      url: str('WEBHOOK_URL', `http://127.0.0.1:${port}/payments/webhook`),
      toleranceSec: dur('WEBHOOK_TOLERANCE', '5m') / 1000,
    },
    mockGateway: {
      pollIntervalMs: dur('MOCK_GATEWAY_POLL_INTERVAL', '200ms'),
      maxDelayMs: dur('MOCK_GATEWAY_MAX_DELAY', '60s'),
      deliveryTimeoutMs: dur('MOCK_GATEWAY_DELIVERY_TIMEOUT', '5s'),
    },
    logLevels: LOG_LEVELS[logLevel] ?? LOG_LEVELS.info,
  };
  if (!cfg.kafka.brokers.length) errs.push('KAFKA_BROKERS: must list at least one broker');
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
