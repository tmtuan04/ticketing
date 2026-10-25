import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import type { Consumer, EachMessagePayload, Kafka } from 'kafkajs';
import { setTimeout as sleep } from 'node:timers/promises';
import { APP_CONFIG, type AppConfig } from '../config';
import { HEADER_MESSAGE_ID, HEADER_TYPE, header, KAFKA } from '../kafka/kafka.module';
import { InvalidMessageError, TOPIC_PAYMENT_COMMANDS, UUID_RE } from './events';
import { PaymentsService } from './payments.service';

const MIN_RESTART_BACKOFF_MS = 500;
const MAX_RESTART_BACKOFF_MS = 10_000;

/**
 * Đọc payment.commands bằng consumer group: nhiều replica cùng group chia nhau partition (topic có
 * 3 partition). Offset chỉ được commit sau khi eachMessage xong, nên process chết giữa chừng thì
 * message được giao lại; inbox trong PaymentsService biến việc giao lại thành vô hại.
 */
@Injectable()
export class PaymentCommandsConsumer implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly log = new Logger(PaymentCommandsConsumer.name);
  private readonly consumer: Consumer;
  private stopping = false;
  /** Tăng dần qua mỗi lần start/crash liên tiếp, về mức nhỏ nhất khi xử lý xong một message. */
  private restartBackoffMs = MIN_RESTART_BACKOFF_MS;

  constructor(
    private readonly payments: PaymentsService,
    @Inject(KAFKA) kafka: Kafka,
    @Inject(APP_CONFIG) config: AppConfig,
  ) {
    this.consumer = kafka.consumer({ groupId: config.kafka.groupId });
    // kafkajs tự khởi động lại consumer với lỗi tạm thời (restart=true). Lỗi còn lại thì consumer
    // dừng hẳn, nên tự kết nối lại để service không âm thầm ngừng nhận lệnh.
    this.consumer.on(this.consumer.events.CRASH, ({ payload }) => {
      this.log.error({ msg: 'consumer crashed', err: String(payload.error), restart: payload.restart });
      if (!payload.restart && !this.stopping) void this.backoff().then(() => this.start());
    });
  }

  onApplicationBootstrap() {
    // Không await: Redpanda chưa sẵn sàng thì start() tự thử lại, HTTP vẫn phục vụ bình thường.
    void this.start();
  }

  async onApplicationShutdown() {
    this.stopping = true;
    // disconnect() rời group ngay để replica khác nhận partition, không phải chờ session timeout.
    await this.consumer.disconnect();
  }

  private async start() {
    while (!this.stopping) {
      try {
        await this.consumer.connect();
        // fromBeginning chỉ áp dụng khi group chưa có offset: lệnh gửi trước lần chạy đầu không bị bỏ sót.
        await this.consumer.subscribe({ topic: TOPIC_PAYMENT_COMMANDS, fromBeginning: true });
        await this.consumer.run({
          partitionsConsumedConcurrently: 3,
          eachMessage: (p) => this.handle(p),
        });
        this.log.log({ msg: 'consuming', topic: TOPIC_PAYMENT_COMMANDS });
        return;
      } catch (err) {
        if (this.stopping) return;
        this.log.warn({ msg: 'consumer start failed, retrying', err: String(err), retry_in_ms: this.restartBackoffMs });
        await this.consumer.disconnect().catch(() => {});
        await this.backoff();
      }
    }
  }

  private async backoff() {
    await sleep(this.restartBackoffMs);
    this.restartBackoffMs = Math.min(this.restartBackoffMs * 2, MAX_RESTART_BACKOFF_MS);
  }

  private async handle({ topic, partition, message }: EachMessagePayload) {
    const where = { topic, partition, offset: message.offset };
    const messageId = header(message.headers, HEADER_MESSAGE_ID);
    if (!messageId || !UUID_RE.test(messageId)) {
      this.log.error({ msg: 'skipping message without valid message_id', ...where });
      return;
    }
    const type = header(message.headers, HEADER_TYPE);
    try {
      const result = await this.payments.handleCommand({ messageId, topic, type, value: message.value });
      this.restartBackoffMs = MIN_RESTART_BACKOFF_MS;
      this.log.debug({ msg: 'command handled', result, type, message_id: messageId, ...where });
    } catch (err) {
      if (err instanceof InvalidMessageError) {
        // Poison message: bỏ qua để không chặn cả partition. M6 sẽ đẩy sang payment.commands.dlq.
        this.log.error({ msg: 'skipping invalid message', err: err.message, message_id: messageId, ...where });
        return;
      }
      throw err; // lỗi tạm thời (DB...): kafkajs retry, không commit offset
    }
  }
}
