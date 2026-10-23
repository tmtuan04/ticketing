import { Server, ServerCredentials, loadPackageDefinition, status, type Metadata } from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';
import type { Type } from '@nestjs/common';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { Test } from '@nestjs/testing';
import { Redis } from 'ioredis';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { configureApp } from '../src/common/http';
import { loadConfig } from '../src/config';

export const PROTO_DIR = join(__dirname, '../../../proto');
export const ADMIN = { email: 'admin@e2e.local', password: 'admin-password' };

/** App đầy đủ như production, trỏ vào container của globalSetup. `env` ghi đè cấu hình. */
export async function createApp(env: Record<string, string> = {}, controllers: Type[] = []) {
  const config = loadConfig({
    JWT_ACCESS_SECRET: 'e2e-secret-0123456789abcdef0123456789',
    DATABASE_URL: process.env.E2E_DATABASE_URL,
    REDIS_URL: process.env.E2E_REDIS_URL,
    PROTO_DIR,
    LOG_LEVEL: 'error',
    ADMIN_EMAIL: ADMIN.email,
    ADMIN_PASSWORD: ADMIN.password,
    RATE_LIMIT_DEFAULT: '10000',
    RATE_LIMIT_AUTH: '10000',
    RATE_LIMIT_HOLD: '10000',
    RATE_LIMIT_ORDER: '10000',
    ...env,
  });
  const moduleRef = await Test.createTestingModule({
    imports: [AppModule.forRoot(config)],
    controllers,
  }).compile();
  const app = moduleRef.createNestApplication<NestExpressApplication>({ logger: config.logLevels });
  configureApp(app, config);
  await app.init();
  return app;
}

/** Xóa bộ đếm rate limit giữa các app dùng chung Redis. */
export async function flushRedis() {
  const redis = new Redis(process.env.E2E_REDIS_URL!);
  await redis.flushall();
  await redis.quit();
}

export const uniqueEmail = () => `user-${randomUUID()}@e2e.local`;

export async function registerUser(app: NestExpressApplication, email = uniqueEmail()) {
  const res = await request(app.getHttpServer())
    .post('/auth/register')
    .send({ email, password: 'password123' })
    .expect(201);
  return res.body as {
    accessToken: string;
    refreshToken: string;
    user: { id: string; email: string; role: string };
  };
}

export async function login(app: NestExpressApplication, email: string, password: string) {
  const res = await request(app.getHttpServer()).post('/auth/login').send({ email, password }).expect(200);
  return res.body as { accessToken: string; refreshToken: string };
}

type Handler = (req: any) => unknown;

export interface RecordedCall {
  method: string;
  request: any;
  metadata: Record<string, string>;
}

/** InventoryService giả, xem startFakeGrpc. */
export const startFakeInventory = () =>
  startFakeGrpc('ticketing/inventory/v1/inventory.proto', 'ticketing.inventory.v1', ['InventoryService']);

/** OrderService giả, xem startFakeGrpc. */
export const startFakeOrder = () =>
  startFakeGrpc('ticketing/order/v1/order.proto', 'ticketing.order.v1', ['OrderService']);

/** CatalogService + CatalogAdminService giả trên cùng một server, xem startFakeGrpc. */
export const startFakeCatalog = () =>
  startFakeGrpc('ticketing/catalog/v1/catalog.proto', 'ticketing.catalog.v1', [
    'CatalogService',
    'CatalogAdminService',
  ]);

/**
 * gRPC server giả chạy in-process: mỗi test gán handler cho RPC cần dùng (theo tên method, ví dụ HoldSeats).
 * Handler throw `grpcError(...)` để trả gRPC status.
 */
async function startFakeGrpc(protoPath: string, pkgName: string, serviceNames: string[]) {
  const def = loadSync(protoPath, {
    includeDirs: [PROTO_DIR],
    keepCase: false,
    longs: String,
    defaults: true,
    arrays: true,
  });
  let pkg = loadPackageDefinition(def) as any;
  for (const part of pkgName.split('.')) pkg = pkg[part];
  const handlers: Record<string, Handler> = {};
  const calls: RecordedCall[] = [];
  const server = new Server();

  for (const serviceName of serviceNames) {
    const service = pkg[serviceName].service;
    const impl: Record<string, (call: any, cb: (err: any, res?: any) => void) => void> = {};
    for (const name of Object.keys(service)) {
      const method = service[name].originalName ?? name;
      impl[name] = (call, cb) => {
        calls.push({ method: name, request: call.request, metadata: flatten(call.metadata) });
        const h = handlers[name];
        if (!h) return cb({ code: status.UNIMPLEMENTED, details: `${method} not stubbed` });
        Promise.resolve()
          .then(() => h(call.request))
          .then((res) => cb(null, res), (err) => cb(err));
      };
    }
    server.addService(service, impl);
  }
  const port = await new Promise<number>((resolve, reject) =>
    server.bindAsync('127.0.0.1:0', ServerCredentials.createInsecure(), (err, p) =>
      err ? reject(err) : resolve(p),
    ),
  );

  return {
    url: `127.0.0.1:${port}`,
    handlers,
    calls,
    reset() {
      for (const k of Object.keys(handlers)) delete handlers[k];
      calls.length = 0;
    },
    stop: () => new Promise<void>((resolve) => server.tryShutdown(() => resolve())),
  };
}

export const grpcError = (code: status, details: string) => ({ code, details });

function flatten(md: Metadata): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(md.getMap())) out[k] = String(v);
  return out;
}
