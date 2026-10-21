import {
  Metadata,
  Server,
  ServerCredentials,
  credentials,
  loadPackageDefinition,
  status,
  type Client,
  type ServiceError,
} from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';
import type { INestMicroservice } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { HealthImplementation } from 'grpc-health-check';
import { createServer } from 'node:net';
import { join } from 'node:path';
import { AppModule } from '../src/app.module';
import { loadConfig } from '../src/config';
import { grpcServerOptions, PROTO_LOADER_OPTIONS } from '../src/grpc';

export const PROTO_DIR = join(__dirname, '../../../proto');

function loadPackage(protoPath: string): any {
  return loadPackageDefinition(loadSync(protoPath, { includeDirs: [PROTO_DIR], ...PROTO_LOADER_OPTIONS }));
}

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.once('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address() as { port: number };
      srv.close(() => resolve(port));
    });
  });
}

/** Catalog thật (gRPC server như production) trỏ vào Postgres của globalSetup. */
export async function startCatalog(env: Record<string, string> = {}) {
  const port = await freePort();
  const config = loadConfig({
    DATABASE_URL: process.env.E2E_DATABASE_URL,
    PROTO_DIR,
    LOG_LEVEL: 'error',
    GRPC_TIMEOUT: '1s',
    ...env,
  });
  const health = new HealthImplementation({ '': 'SERVING' });
  const moduleRef = await Test.createTestingModule({ imports: [AppModule.forRoot(config, health)] }).compile();
  const app: INestMicroservice = moduleRef.createNestMicroservice({
    ...grpcServerOptions(config, health, `127.0.0.1:${port}`),
    logger: config.logLevels,
  });
  await app.listen();

  const pkg = loadPackage('ticketing/catalog/v1/catalog.proto').ticketing.catalog.v1;
  const addr = `127.0.0.1:${port}`;
  const read: Client = new pkg.CatalogService(addr, credentials.createInsecure());
  const admin: Client = new pkg.CatalogAdminService(addr, credentials.createInsecure());
  return {
    app,
    addr,
    read: rpcCaller(read),
    admin: rpcCaller(admin),
    async close() {
      read.close();
      admin.close();
      await app.close();
    },
  };
}

/** Gọi unary RPC dạng Promise: `await rpc('GetEvent', { eventId })`. */
function rpcCaller(client: Client) {
  return <T = any>(method: string, req: object, md = new Metadata()): Promise<T> =>
    new Promise((resolve, reject) => {
      const fn = (client as any)[method[0].toLowerCase() + method.slice(1)];
      fn.call(client, req, md, (err: ServiceError | null, res: T) => (err ? reject(err) : resolve(res)));
    });
}

/** Chờ RPC lỗi và trả về { code, details } để so sánh. */
export async function rpcError(p: Promise<unknown>): Promise<{ code: status; details: string }> {
  try {
    await p;
  } catch (e) {
    const err = e as ServiceError;
    return { code: err.code, details: err.details };
  }
  throw new Error('expected the RPC to fail');
}

type Handler = (req: any) => unknown;

export interface RecordedCall {
  method: string;
  request: any;
  metadata: Record<string, string>;
}

/**
 * InventoryService giả chạy in-process: mỗi test gán handler cho RPC cần dùng.
 * Handler throw `grpcError(...)` để trả gRPC status.
 */
export async function startFakeInventory() {
  const service = loadPackage('ticketing/inventory/v1/inventory.proto').ticketing.inventory.v1.InventoryService
    .service;
  const handlers: Record<string, Handler> = {};
  const calls: RecordedCall[] = [];

  const impl: Record<string, (call: any, cb: (err: any, res?: any) => void) => void> = {};
  for (const name of Object.keys(service)) {
    impl[name] = (call, cb) => {
      calls.push({ method: name, request: call.request, metadata: flatten(call.metadata) });
      const h = handlers[name];
      if (!h) return cb({ code: status.UNIMPLEMENTED, details: `${name} not stubbed` });
      Promise.resolve()
        .then(() => h(call.request))
        .then((res) => cb(null, res), (err) => cb(err));
    };
  }

  const server = new Server();
  server.addService(service, impl);
  const port = await new Promise<number>((resolve, reject) =>
    server.bindAsync('127.0.0.1:0', ServerCredentials.createInsecure(), (err, p) => (err ? reject(err) : resolve(p))),
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
