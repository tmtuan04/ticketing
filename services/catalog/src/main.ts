import { ConsoleLogger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { MicroserviceOptions } from '@nestjs/microservices';
import { HealthImplementation } from 'grpc-health-check';
import { AppModule } from './app.module';
import { loadConfig } from './config';
import { grpcServerOptions } from './grpc';

async function bootstrap() {
  // .env chỉ dùng khi chạy local; biến môi trường có sẵn được ưu tiên
  try {
    process.loadEnvFile();
  } catch {
    // không có .env
  }
  const config = loadConfig();
  // SERVING ngay khi server lắng nghe: Prisma đã connect xong trong onModuleInit
  const health = new HealthImplementation({ '': 'SERVING' });
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule.forRoot(config, health), {
    ...grpcServerOptions(config, health),
    logger: new ConsoleLogger({ json: true, logLevels: config.logLevels }),
  });
  app.enableShutdownHooks();
  await app.listen();
}

void bootstrap();
