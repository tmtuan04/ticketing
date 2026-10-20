import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer } from '@testcontainers/redis';
import { execFileSync } from 'node:child_process';
import { join } from 'node:path';

// Một Postgres + một Redis cho cả bộ e2e (chạy --runInBand), migration bằng chính prisma CLI.
export default async function globalSetup() {
  const [pg, redis] = await Promise.all([
    new PostgreSqlContainer('postgres:16-alpine').withDatabase('auth_db').start(),
    new RedisContainer('redis:7-alpine').start(),
  ]);
  process.env.E2E_DATABASE_URL = pg.getConnectionUri();
  process.env.E2E_REDIS_URL = redis.getConnectionUrl();

  const gatewayDir = join(__dirname, '..');
  execFileSync(process.execPath, [require.resolve('prisma/build/index.js'), 'migrate', 'deploy'], {
    cwd: gatewayDir,
    env: { ...process.env, DATABASE_URL: pg.getConnectionUri() },
    stdio: 'pipe',
  });

  (globalThis as any).__E2E_CONTAINERS__ = [pg, redis];
}
