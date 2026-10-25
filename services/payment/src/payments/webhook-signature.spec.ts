import { signWebhook, verifyWebhook } from './webhook-signature';

const SECRET = 's'.repeat(32);
const BODY = '{"id":"evt_1","type":"charge.succeeded"}';
const NOW = 1_800_000_000;

describe('webhook signature', () => {
  const header = signWebhook(SECRET, BODY, NOW);

  it('accepts its own signature, as string or Buffer', () => {
    expect(header).toMatch(/^t=1800000000,v1=[0-9a-f]{64}$/);
    expect(verifyWebhook(SECRET, BODY, header, 300, NOW)).toBe(true);
    expect(verifyWebhook(SECRET, Buffer.from(BODY), header, 300, NOW + 299)).toBe(true);
  });

  it('rejects a tampered body or another secret', () => {
    expect(verifyWebhook(SECRET, BODY.replace('succeeded', 'failed'), header, 300, NOW)).toBe(false);
    expect(verifyWebhook('x'.repeat(32), BODY, header, 300, NOW)).toBe(false);
  });

  it('rejects a replay outside the tolerance window', () => {
    expect(verifyWebhook(SECRET, BODY, header, 300, NOW + 301)).toBe(false);
    expect(verifyWebhook(SECRET, BODY, header, 300, NOW - 301)).toBe(false);
  });

  it('rejects a timestamp changed after signing', () => {
    expect(verifyWebhook(SECRET, BODY, header.replace(`t=${NOW}`, `t=${NOW + 1}`), 300, NOW)).toBe(false);
  });

  it.each([undefined, '', 'garbage', 't=abc,v1=00', `t=${NOW}`, `t=${NOW},v1=xyz`])(
    'rejects malformed header %p',
    (h) => expect(verifyWebhook(SECRET, BODY, h, 300, NOW)).toBe(false),
  );
});
