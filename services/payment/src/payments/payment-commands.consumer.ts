import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import type { Consumer, EachMessagePayload, Kafka, Producer } from 'kafkajs';
import { setTimeout as sleep } from 'node:timers/promises';
import { isTransient } from '../common/transient';
import { APP_CONFIG, type AppConfig } from '../config';
import { HEADER_MESSAGE_ID, HEADER_TYPE, header, KAFKA } from '../kafka/kafka.module';
import { DLQ_SUFFIX, InvalidMessageError, TOPIC_PAYMENT_COMMANDS, UUID_RE } from './events';
import { PaymentsService } from './payments.service';

const MIN_RESTART_BACKOFF_MS = 500;
const MAX_RESTART_BACKOFF_MS = 10_000;
/** Backoff tối đa giữa các lần thử một message. Phải nhỏ hơn session timeout (30s) vì giữa hai lần thử mới gửi heartbeat. */
const MAX_RETRY_BACKOFF_MS = 10_000;

// Header thêm vào message khi đẩy sang DLQ, giống consumer bên order (Go).
export const HEADER_DLQ_ERROR = 'dlq_error';
export const HEADER_DLQ_TOPIC = 'dlq_source_topic';
export const HEADER_DLQ_PARTITION = 'dlq_source_partition';
export const HEADER_DLQ_OFFSET = 'dlq_source_offset';
export const HEADER_DLQ_ATTEMPTS = 'dlq_attempts';

/**
 * Đọc payment.commands bằng consumer group: nhiều replica cùng group chia nhau partition (topic có
 * 3 partition). Offset chỉ được commit sau khi eachMessage xong, nên process chết giữa chừng thì
 * message được giao lại; inbox trong PaymentsService biến việc giao lại thành vô hại.
 *
 * Lỗi khi xử lý (cùng quy tắc với order):
 * - InvalidMessageError (sai contract, thiếu message_id): đẩy ngay sang payment.commands.dlq.
 * - lỗi tạm thời (Postgres sập...): retry mãi với backoff, chặn partition đó.
 * - lỗi khác: retry tối đa `maxAttempts` lần rồi đẩy DLQ, để không chặn partition mãi mãi.
 */
@Injectable()
export class PaymentCommandsConsumer implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly log = new Logger(PaymentCommandsConsumer.name);
  private readonly consumer: Consumer;
  private readonly dlq: Producer;
  private dlqConnected = false;
  private stopping = false;
  /** Hủy các lần chờ retry khi tắt, để disconnect() không phải đợi hết backoff. */
  private readonly abort = new AbortController();
  /** Tăng dần qua mỗi lần start/crash liên tiếp, về mức nhỏ nhất khi xử lý xong một message. */
  private restartBackoffMs = MIN_RESTART_BACKOFF_MS;

  constructor(
    private readonly payments: PaymentsService,
    @Inject(KAFKA) kafka: Kafka,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {
    this.consumer = kafka.consumer({ groupId: config.kafka.groupId });
    this.dlq = kafka.producer();
    this.dlq.on(this.dlq.events.DISCONNECT, () => (this.dlqConnected = false));
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
    this.abort.abort();
    // disconnect() rời group ngay để replica khác nhận partition, không phải chờ session timeout.
    await this.consumer.disconnect();
    await this.dlq.disconnect();
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

  /** Xử lý một message tới khi xong (thành công, bỏ qua, hoặc đã nằm trong DLQ). Chỉ ném lỗi khi đang tắt. */
  private async handle(p: EachMessagePayload) {
    const { topic, partition, message } = p;
    const where = { topic, partition, offset: message.offset };
    const messageId = header(message.headers, HEADER_MESSAGE_ID);
    if (!messageId || !UUID_RE.test(messageId)) {
      return this.deadLetter(p, new InvalidMessageError('missing or invalid message_id header'), 1);
    }
    const type = header(message.headers, HEADER_TYPE);
    let waitMs = this.config.kafka.retryBackoffMs;
    for (let attempt = 1; ; attempt++) {
      try {
        const result = await this.payments.handleCommand({ messageId, topic, type, value: message.value });
        this.restartBackoffMs = MIN_RESTART_BACKOFF_MS;
        this.log.debug({ msg: 'command handled', result, type, message_id: messageId, ...where });
        return;
      } catch (err) {
        if (this.stopping) throw err; // không commit offset, lần sau được giao lại
        if (err instanceof InvalidMessageError) return this.deadLetter(p, err, attempt);
        const transient = isTransient(err);
        if (!transient && attempt >= this.config.kafka.maxAttempts) return this.deadLetter(p, err, attempt);
        this.log.warn({
          msg: transient ? 'transient error, retrying' : 'handle failed, retrying',
          err: String(err),
          attempt,
          retry_in_ms: waitMs,
          message_id: messageId,
          ...where,
        });
      }
      await this.pause(p, waitMs);
      waitMs = Math.min(waitMs * 2, MAX_RETRY_BACKOFF_MS);
    }
  }

  /**
   * Chép message sang "<topic>.dlq" kèm header lỗi, rồi coi như đã xử lý (offset được commit).
   * Gửi lỗi thì thử lại tới khi được: message chỉ rời partition khi đã nằm an toàn trong DLQ.
   */
  private async deadLetter(p: EachMessagePayload, cause: unknown, attempts: number) {
    const { topic, partition, message } = p;
    const err = cause instanceof Error ? cause.message : String(cause);
    this.log.error({ msg: 'moving message to DLQ', err, attempts, topic, partition, offset: message.offset });
    const headers = {
      ...message.headers,
      [HEADER_DLQ_ERROR]: err,
      [HEADER_DLQ_TOPIC]: topic,
      [HEADER_DLQ_PARTITION]: String(partition),
      [HEADER_DLQ_OFFSET]: message.offset,
      [HEADER_DLQ_ATTEMPTS]: String(attempts),
    };
    let waitMs = this.config.kafka.retryBackoffMs;
    for (;;) {
      try {
        if (!this.dlqConnected) {
          await this.dlq.connect();
          this.dlqConnected = true;
        }
        await this.dlq.send({
          topic: topic + DLQ_SUFFIX,
          acks: -1,
          messages: [{ key: message.key, value: message.value, headers }],
        });
        return;
      } catch (e) {
        if (this.stopping) throw e;
        this.log.warn({ msg: 'publish to DLQ failed, retrying', err: String(e), retry_in_ms: waitMs });
      }
      await this.pause(p, waitMs);
      waitMs = Math.min(waitMs * 2, MAX_RETRY_BACKOFF_MS);
    }
  }

  /** Chờ giữa hai lần thử rồi gửi heartbeat để group không coi consumer là đã chết. */
  private async pause(p: EachMessagePayload, ms: number) {
    await sleep(ms, undefined, { signal: this.abort.signal });
    await p.heartbeat();
  }
}
