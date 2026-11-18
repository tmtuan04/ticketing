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
  // Đứng sau load balancer/ingress giữ kết nối keep-alive: Node mặc định đóng socket rảnh sau 5s,
  // nếu đóng đúng lúc LB đang gửi request kế tiếp trên socket đó thì LB nhận "connection reset" => 502.
  // Quy tắc: timeout keep-alive của backend phải DÀI hơn của LB (nginx upstream để 4s, xem
  // deploy/loadtest/nginx.conf). headersTimeout phải lớn hơn keepAliveTimeout.
  const server = app.getHttpServer();
  server.keepAliveTimeout = 65_000;
  server.headersTimeout = 66_000;
  // Backlog (hàng đợi kết nối đã bắt tay xong, chờ accept) mặc định của Node là 511. Load test 5.000
  // VU qua nginx làm tràn hàng đợi này khi event loop bận (ListenOverflows hàng chục nghìn trong
  // /proc/net/netstat), kernel chuyển sang SYN cookie rồi reset kết nối => nginx trả 502. Kernel còn
  // chặn trên bởi net.core.somaxconn (4096 trong container Docker). app.listen của Nest không nhận
  // backlog nên init rồi tự listen.
  await app.init();
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen({ port: config.port, backlog: 4096 }, resolve);
  });
}

void bootstrap();
