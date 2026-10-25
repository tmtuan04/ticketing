import { loadConfig } from './config';

const SECRET = 'x'.repeat(32);

describe('loadConfig', () => {
  it('uses defaults for everything except the webhook secret', () => {
    const c = loadConfig({ WEBHOOK_SECRET: SECRET });
    expect(c.port).toBe(3001);
    expect(c.kafka).toEqual({ brokers: ['localhost:19092'], clientId: 'payment', groupId: 'payment' });
    expect(c.outbox).toEqual({ intervalMs: 500, batchSize: 100 });
    expect(c.webhook).toEqual({
      secret: SECRET,
      url: 'http://127.0.0.1:3001/payments/webhook',
      toleranceSec: 300,
    });
  });

  it('derives the default webhook url from PORT and splits KAFKA_BROKERS', () => {
    const c = loadConfig({ WEBHOOK_SECRET: SECRET, PORT: '4000', KAFKA_BROKERS: 'a:9092, b:9092,' });
    expect(c.webhook.url).toBe('http://127.0.0.1:4000/payments/webhook');
    expect(c.kafka.brokers).toEqual(['a:9092', 'b:9092']);
  });

  it('reports every invalid variable at once', () => {
    expect(() =>
      loadConfig({ WEBHOOK_SECRET: 'short', LOG_LEVEL: 'verbose', OUTBOX_INTERVAL: 'fast', KAFKA_BROKERS: ',' }),
    ).toThrow(/WEBHOOK_SECRET.*\n.*LOG_LEVEL.*\n.*OUTBOX_INTERVAL.*\n.*KAFKA_BROKERS/s);
  });

  it('requires WEBHOOK_SECRET', () => {
    expect(() => loadConfig({})).toThrow(/WEBHOOK_SECRET: required/);
  });
});
