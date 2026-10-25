import { InvalidMessageError, parsePaymentRequested } from './events';

const valid = {
  order_id: '0b1c2d3e-4f50-4a6b-8c7d-9e0f1a2b3c4d',
  user_id: '1c2d3e4f-5061-4b7c-9d8e-0f1a2b3c4d5e',
  amount: 2_000_000,
  currency: 'VND',
  // Go encode time.Time dạng RFC 3339 có nano giây
  payment_deadline: '2026-10-23T10:10:00.123456789Z',
};

const buf = (v: unknown) => Buffer.from(typeof v === 'string' ? v : JSON.stringify(v));

describe('parsePaymentRequested', () => {
  it('accepts the payload the Go order service produces', () => {
    expect(parsePaymentRequested(buf(valid))).toEqual(valid);
  });

  it.each([
    ['not JSON', '{oops', /valid JSON/],
    ['null payload', 'null', /object/],
    ['bad uuid', { ...valid, order_id: 'abc' }, /order_id must be a uuid/],
    ['fractional amount', { ...valid, amount: 1.5 }, /amount/],
    ['amount as string', { ...valid, amount: '100' }, /amount/],
    ['lowercase currency', { ...valid, currency: 'vnd' }, /currency/],
    ['bad deadline', { ...valid, payment_deadline: 'tomorrow' }, /payment_deadline/],
  ])('rejects %s', (_, payload, msg) => {
    const parse = () => parsePaymentRequested(buf(payload));
    expect(parse).toThrow(InvalidMessageError);
    expect(parse).toThrow(msg);
  });

  it('rejects an empty message', () => {
    expect(() => parsePaymentRequested(null)).toThrow(InvalidMessageError);
  });
});
