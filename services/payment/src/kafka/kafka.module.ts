import { Global, Logger, Module } from '@nestjs/common';
import { CompressionCodecs, CompressionTypes, Kafka, logLevel, type IHeaders, type LogEntry } from 'kafkajs';
import SnappyCodec from 'kafkajs-snappy';
import { APP_CONFIG, type AppConfig } from '../config';

// franz-go (relay của order) mặc định nén batch bằng snappy, kafkajs chỉ có sẵn gzip: thiếu codec
// này thì consumer crash ngay message đầu tiên ("Snappy compression not implemented").
// Đăng ký toàn cục, một lần khi module được import.
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

export const KAFKA = Symbol('KAFKA');

// Tên header đi kèm mỗi message, giống outbox relay của order (Go): consumer chống trùng và phân
// loại message bằng header, không cần parse payload.
export const HEADER_MESSAGE_ID = 'message_id';
export const HEADER_TYPE = 'type';

/** Header kafkajs có thể là Buffer, string hoặc mảng (header lặp lại); lấy giá trị đầu tiên. */
export function header(headers: IHeaders | undefined, key: string): string | undefined {
  const v = headers?.[key];
  const first = Array.isArray(v) ? v[0] : v;
  return first === undefined ? undefined : first.toString();
}

/** Log của kafkajs đi qua Logger của Nest để ra cùng định dạng JSON. */
function kafkaLogCreator() {
  const log = new Logger('Kafka');
  return ({ level, log: { message, ...extra } }: LogEntry) => {
    const entry = { msg: message, ...extra };
    if (level === logLevel.ERROR) log.error(entry);
    else if (level === logLevel.WARN) log.warn(entry);
    else if (level === logLevel.INFO) log.log(entry);
    else log.debug(entry);
  };
}

/** Một Kafka client cho cả service; producer (outbox relay) và consumer tự tạo từ client này. */
@Global()
@Module({
  providers: [
    {
      provide: KAFKA,
      inject: [APP_CONFIG],
      useFactory: (config: AppConfig) =>
        new Kafka({
          clientId: config.kafka.clientId,
          brokers: config.kafka.brokers,
          logLevel: logLevel.WARN,
          logCreator: kafkaLogCreator,
        }),
    },
  ],
  exports: [KAFKA],
})
export class KafkaModule {}
