import type { NestExpressApplication } from '@nestjs/platform-express';
import { Test } from '@nestjs/testing';
import { CompressionTypes, Kafka, logLevel, Partitioners, type Consumer, type Producer } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';
import { AppModule } from '../src/app.module';
import { configureApp } from '../src/common/http';
import { loadConfig } from '../src/config';
import { header } from '../src/kafka/kafka.module';

export const WEBHOOK_SECRET = 'e2e-webhook-secret-0123456789abcdef';

/** Port trống để biết trước WEBHOOK_URL (cổng giả lập gọi ngược lại chính app). */
async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer().listen(0, '127.0.0.1', () => {
      const { port } = srv.address() as { port: number };
      srv.close(() => resolve(port));
    });
    srv.on('error', reject);
  });
}

/**
 * App đầy đủ như production (consumer, relay, worker webhook đều chạy), listen trên port thật để
 * cổng giả lập gọi được webhook. Mỗi app một consumer group riêng. `env` ghi đè cấu hình.
 */
export async function createApp(env: Record<string, string> = {}) {
  const port = await freePort();
  const config = loadConfig({
    PORT: String(port),
    DATABASE_URL: process.env.E2E_DATABASE_URL,
    KAFKA_BROKERS: process.env.E2E_KAFKA_BROKERS,
    KAFKA_GROUP_ID: `payment-e2e-${randomUUID()}`,
    WEBHOOK_SECRET,
    OUTBOX_INTERVAL: '100ms',
    MOCK_GATEWAY_POLL_INTERVAL: '50ms',
    LOG_LEVEL: 'error',
    ...env,
  });
  const moduleRef = await Test.createTestingModule({ imports: [AppModule.forRoot(config)] }).compile();
  const app = moduleRef.createNestApplication<NestExpressApplication>({
    rawBody: true,
    logger: config.logLevels,
  });
  configureApp(app);
  await app.listen(port, '127.0.0.1');
  return app;
}

const kafka = () =>
  new Kafka({ brokers: [process.env.E2E_KAFKA_BROKERS!], logLevel: logLevel.NOTHING });

/**
 * Đóng vai order service: gửi lệnh lên payment.commands với header như relay của Go, nén snappy
 * như franz-go mặc định (codec snappy được đăng ký khi import kafka.module).
 */
export async function commandProducer() {
  const producer: Producer = kafka().producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();
  return {
    send: (msg: { messageId?: string; type?: string; key: string; value: string }) =>
      producer.send({
        topic: 'payment.commands',
        compression: CompressionTypes.Snappy,
        messages: [
          {
            key: msg.key,
            value: msg.value,
            headers: {
              ...(msg.messageId !== undefined && { message_id: msg.messageId }),
              ...(msg.type !== undefined && { type: msg.type }),
            },
          },
        ],
      }),
    stop: () => producer.disconnect(),
  };
}

export interface ReceivedEvent {
  key: string;
  messageId: string;
  type: string;
  payload: any;
}

/** Đọc payment.events từ đầu topic và gom message lại để test kiểm tra. */
export async function eventCollector() {
  const consumer: Consumer = kafka().consumer({ groupId: `collector-${randomUUID()}` });
  const received: ReceivedEvent[] = [];
  await consumer.connect();
  await consumer.subscribe({ topic: 'payment.events', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      received.push({
        key: message.key!.toString(),
        messageId: header(message.headers, 'message_id')!,
        type: header(message.headers, 'type')!,
        payload: JSON.parse(message.value!.toString()),
      });
    },
  });
  return { received, stop: () => consumer.disconnect() };
}

/** Poll tới khi `fn` trả giá trị truthy (hoặc hết giờ thì ném lỗi kèm giá trị cuối). */
export async function waitFor<T>(fn: () => Promise<T> | T, timeoutMs = 20_000, everyMs = 100): Promise<NonNullable<T>> {
  const deadline = Date.now() + timeoutMs;
  let last: T | undefined;
  while (Date.now() < deadline) {
    last = await fn();
    if (last) return last as NonNullable<T>;
    await sleep(everyMs);
  }
  throw new Error(`waitFor timed out, last value: ${JSON.stringify(last)}`);
}

export function paymentRequested(overrides: Record<string, unknown> = {}) {
  return {
    order_id: randomUUID(),
    user_id: randomUUID(),
    amount: 2_000_000,
    currency: 'VND',
    payment_deadline: new Date(Date.now() + 10 * 60e3).toISOString(),
    ...overrides,
  };
}
