import { loadConfig, parseDuration } from './config';

const SECRET = 'x'.repeat(32);

describe('parseDuration', () => {
  it.each([
    ['250ms', 250],
    ['30s', 30_000],
    ['15m', 900_000],
    ['2h', 7_200_000],
    ['7d', 604_800_000],
  ])('%s => %d ms', (input, ms) => expect(parseDuration(input)).toBe(ms));

  it.each(['', '15', 'm', '1.5h', '-1s', '0s'])('rejects %p', (input) =>
    expect(parseDuration(input)).toBeUndefined(),
  );
});

describe('loadConfig', () => {
  it('uses defaults for everything except the JWT secret', () => {
    const c = loadConfig({ JWT_ACCESS_SECRET: SECRET });
    expect(c.port).toBe(3000);
    expect(c.jwt).toEqual({ accessSecret: SECRET, accessTtlSec: 900, refreshTtlSec: 604_800 });
    expect(c.rateLimit).toEqual({ windowMs: 60_000, default: 120, auth: 10, hold: 10, order: 10 });
    expect(c.trustProxy).toBe(false);
    expect(c.admin).toBeUndefined();
  });

  it('reports every invalid variable at once', () => {
    expect(() =>
      loadConfig({
        JWT_ACCESS_SECRET: 'short',
        PORT: 'abc',
        JWT_ACCESS_TTL: '15 minutes',
        ADMIN_EMAIL: 'a@b.c',
        LOG_LEVEL: 'verbose',
      }),
    ).toThrow(
      /JWT_ACCESS_SECRET.*\n.*LOG_LEVEL.*\n.*ADMIN_EMAIL.*\n.*PORT.*\n.*JWT_ACCESS_TTL/s,
    );
  });

  it('parses TRUST_PROXY as boolean, hop count or subnet list', () => {
    const tp = (v: string) => loadConfig({ JWT_ACCESS_SECRET: SECRET, TRUST_PROXY: v }).trustProxy;
    expect(tp('true')).toBe(true);
    expect(tp('1')).toBe(1);
    expect(tp('loopback, 10.0.0.0/8')).toBe('loopback, 10.0.0.0/8');
  });
});
