import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import { Partitioners, type Kafka, type Producer, type TopicMessages } from 'kafkajs';
import { PollLoop } from '../common/poll-loop';
import { APP_CONFIG, type AppConfig } from '../config';
import { HEADER_MESSAGE_ID, HEADER_TYPE, KAFKA } from '../kafka/kafka.module';
import { PrismaService } from '../prisma/prisma.service';

interface OutboxRow {
  id: bigint;
  message_id: string;
  topic: string;
  key: string;
  type: string;
  payload: string;
}

/**
 * Nửa sau của Transactional Outbox, cùng thuật toán với relay của order (Go, internal/outbox):
 * mỗi lô một transaction `SELECT ... FOR UPDATE SKIP LOCKED` -> publish chờ ack -> `published_at`.
 * Publish lỗi thì ROLLBACK, lô được thử lại (AT-LEAST-ONCE); consumer chống trùng bằng message_id.
 * Nhiều replica chạy song song an toàn nhờ SKIP LOCKED.
 */
@Injectable()
export class OutboxRelay implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly log = new Logger(OutboxRelay.name);
  private readonly producer: Producer;
  private readonly loop: PollLoop;
  private connected = false;

  constructor(
    private readonly prisma: PrismaService,
    @Inject(KAFKA) kafka: Kafka,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {
    this.producer = kafka.producer({
      // acks=all + số thứ tự: broker bỏ bản trùng do chính producer retry (như franz-go mặc định).
      // kafkajs chỉ đảm bảo thứ tự khi idempotent nếu tối đa 1 request đang bay.
      idempotent: true,
      maxInFlightRequests: 1,
      // DefaultPartitioner = murmur2 giống Java client và franz-go: cùng key thì cùng partition.
      createPartitioner: Partitioners.DefaultPartitioner,
      // Hết retry mà chưa gửi được thì send() lỗi -> ROLLBACK + backoff, không giữ transaction quá lâu.
      retry: { retries: 3 },
    });
    this.producer.on(this.producer.events.DISCONNECT, () => (this.connected = false));
    this.loop = new PollLoop('outbox relay', () => this.drain(), config.outbox.intervalMs, this.log);
  }

  onApplicationBootstrap() {
    // Kết nối Redpanda nằm trong vòng lặp: Redpanda chưa lên thì chỉ backoff, service vẫn khởi động.
    this.loop.start();
  }

  async onApplicationShutdown() {
    await this.loop.stop();
    await this.producer.disconnect();
  }

  /** Publish từng lô cho tới khi hết message chờ (lô cuối không đầy). */
  async drain() {
    for (;;) {
      const n = await this.publishBatch();
      if (n < this.config.outbox.batchSize) return;
    }
  }

  /** Publish một lô trong một transaction, trả về số message đã publish. */
  async publishBatch(): Promise<number> {
    if (!this.connected) {
      await this.producer.connect();
      this.connected = true;
    }
    const n = await this.prisma.$transaction(
      async (tx) => {
        const rows = await tx.$queryRaw<OutboxRow[]>`
          SELECT id, message_id::text, topic, key, type, payload::text AS payload
          FROM outbox
          WHERE published_at IS NULL
          ORDER BY id
          LIMIT ${this.config.outbox.batchSize}
          FOR UPDATE SKIP LOCKED`;
        if (!rows.length) return 0;

        // Gom theo topic, giữ thứ tự id trong từng topic.
        const byTopic = new Map<string, TopicMessages>();
        for (const m of rows) {
          let tm = byTopic.get(m.topic);
          if (!tm) byTopic.set(m.topic, (tm = { topic: m.topic, messages: [] }));
          tm.messages.push({
            key: m.key,
            value: m.payload,
            headers: { [HEADER_MESSAGE_ID]: m.message_id, [HEADER_TYPE]: m.type },
          });
        }
        await this.producer.sendBatch({ topicMessages: [...byTopic.values()], acks: -1 });

        const ids = rows.map((r) => r.id.toString());
        await tx.$executeRaw`UPDATE outbox SET published_at = now() WHERE id = ANY(${ids}::bigint[])`;
        return rows.length;
      },
      // mặc định 5s; send() có retry nên cần dư thời gian, quá hạn thì Prisma ROLLBACK
      { timeout: 30_000 },
    );
    if (n > 0) this.log.debug({ msg: 'outbox published', count: n });
    return n;
  }
}
