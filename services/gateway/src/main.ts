import { ConsoleLogger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from './app.module';
import { configureApp } from './common/http';
import { loadConfig } from './config';

async function bootstrap() {
  // .env chỉ dùng khi chạy local; biến môi trường có sẵn được ưu tiên
  try {
    process.loadEnvFile();
  } catch {
    // không có .env
  }
  const config = loadConfig();
  const app = await NestFactory.create<NestExpressApplication>(AppModule.forRoot(config), {
    logger: new ConsoleLogger({ json: true, logLevels: config.logLevels }),
  });
  configureApp(app, config);
  await app.listen(config.port);
}

void bootstrap();
