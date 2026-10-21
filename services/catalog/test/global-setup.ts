import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { execFileSync } from 'node:child_process';
import { join } from 'node:path';

// Một Postgres cho cả bộ e2e (chạy --runInBand), migration bằng chính prisma CLI.
export default async function globalSetup() {
  const pg = await new PostgreSqlContainer('postgres:16-alpine').withDatabase('catalog_db').start();
  process.env.E2E_DATABASE_URL = pg.getConnectionUri();

  execFileSync(process.execPath, [require.resolve('prisma/build/index.js'), 'migrate', 'deploy'], {
    cwd: join(__dirname, '..'),
    env: { ...process.env, DATABASE_URL: pg.getConnectionUri() },
    stdio: 'pipe',
  });

  (globalThis as any).__E2E_CONTAINERS__ = [pg];
}
