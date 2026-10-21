import { credentials, makeGenericClientConstructor, type ServiceDefinition } from '@grpc/grpc-js';
import { service } from 'grpc-health-check';

// Healthcheck cho docker compose (image không có grpc_health_probe): gọi grpc.health.v1.Health/Check.
// K8s (Phase 4) dùng gRPC probe native nên không cần file này.
const Health = makeGenericClientConstructor(service as unknown as ServiceDefinition, 'Health');
const client = new Health(`127.0.0.1:${process.env.GRPC_PORT || 50052}`, credentials.createInsecure()) as any;

client.check({ service: '' }, { deadline: Date.now() + 2_000 }, (err: Error | null, res?: { status: unknown }) => {
  const ok = !err && (res?.status === 'SERVING' || res?.status === 1);
  if (!ok) console.error(err?.message ?? `status ${res?.status}`);
  process.exit(ok ? 0 : 1);
});
