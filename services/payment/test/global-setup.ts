import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { RedpandaContainer } from '@testcontainers/redpanda';
import { Kafka, logLevel } from 'kafkajs';
import { execFileSync } from 'node:child_process';
import { join } from 'node:path';

export const TOPICS = ['payment.commands', 'payment.events'];

// Một Postgres + một Redpanda cho cả bộ e2e (chạy --runInBand). Migration bằng chính prisma CLI,
// topic tạo trước với 3 partition giống job redpanda-init của compose.
export default async function globalSetup() {
  const [pg, redpanda] = await Promise.all([
    new PostgreSqlContainer('postgres:16-alpine').withDatabase('payment_db').start(),
    new RedpandaContainer('redpandadata/redpanda:v24.2.7').start(),
  ]);
  process.env.E2E_DATABASE_URL = pg.getConnectionUri();
  process.env.E2E_KAFKA_BROKERS = redpanda.getBootstrapServers();

  execFileSync(process.execPath, [require.resolve('prisma/build/index.js'), 'migrate', 'deploy'], {
    cwd: join(__dirname, '..'),
    env: { ...process.env, DATABASE_URL: pg.getConnectionUri() },
    stdio: 'pipe',
  });

  const admin = new Kafka({ brokers: [redpanda.getBootstrapServers()], logLevel: logLevel.NOTHING }).admin();
  await admin.connect();
  await admin.createTopics({
    waitForLeaders: true,
    topics: TOPICS.map((topic) => ({ topic, numPartitions: 3, replicationFactor: 1 })),
  });
  await admin.disconnect();

  (globalThis as any).__E2E_CONTAINERS__ = [pg, redpanda];
}
