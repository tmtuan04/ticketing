import { status } from '@grpc/grpc-js';
import { RpcException } from '@nestjs/microservices';
import {
  decodePageToken,
  encodePageToken,
  parseCreateEvent,
  parseGenerateSeats,
  parseListEvents,
} from './validation';

const NOW = new Date('2026-10-20T00:00:00Z');
const VENUE = '6f1d6c1e-3b8a-4a52-9a54-0c6f5d6a1b2c';
const TIER = '0b7e8c55-2d1f-4f6e-8a3b-5c9d7e1f2a3b';
const ts = (iso: string) => ({ seconds: Date.parse(iso) / 1000, nanos: 0 });

function rpcCode(fn: () => unknown): status | undefined {
  try {
    fn();
  } catch (e) {
    if (e instanceof RpcException) return (e.getError() as { code: status }).code;
    throw e;
  }
  return undefined;
}

const validEvent = (overrides: Record<string, unknown> = {}) => ({
  name: ' Concert ',
  description: '',
  venueId: VENUE,
  startsAt: ts('2026-12-01T12:00:00Z'),
  tiers: [
    { name: 'VIP', priceAmount: '2000000' as unknown as number, currency: '' },
    { name: 'Standard', priceAmount: 500000, currency: 'vnd' },
  ],
  ...overrides,
});

describe('parseCreateEvent', () => {
  it('normalizes a valid request', () => {
    const e = parseCreateEvent(validEvent(), NOW);
    expect(e).toEqual({
      name: 'Concert',
      description: '',
      venueId: VENUE,
      startsAt: new Date('2026-12-01T12:00:00Z'),
      tiers: [
        { name: 'VIP', priceAmount: 2_000_000n, currency: 'VND' },
        { name: 'Standard', priceAmount: 500_000n, currency: 'VND' },
      ],
    });
  });

  it.each([
    ['missing name', { name: '  ' }],
    ['bad venue id', { venueId: 'nope' }],
    ['missing starts_at', { startsAt: null }],
    ['starts_at in the past', { startsAt: ts('2026-01-01T00:00:00Z') }],
    ['no tiers', { tiers: [] }],
    ['duplicate tier names', { tiers: [{ name: 'VIP', priceAmount: 1 }, { name: 'vip', priceAmount: 2 }] }],
    ['negative price', { tiers: [{ name: 'VIP', priceAmount: '-1' }] }],
    ['fractional price', { tiers: [{ name: 'VIP', priceAmount: 1.5 }] }],
    ['price too large', { tiers: [{ name: 'VIP', priceAmount: '1000000000001' }] }],
    ['bad currency', { tiers: [{ name: 'VIP', priceAmount: 1, currency: 'dong' }] }],
    ['mixed currencies', { tiers: [{ name: 'A', priceAmount: 1, currency: 'VND' }, { name: 'B', priceAmount: 1, currency: 'USD' }] }],
  ])('rejects %s', (_, overrides) => {
    expect(rpcCode(() => parseCreateEvent(validEvent(overrides) as any, NOW))).toBe(status.INVALID_ARGUMENT);
  });
});

describe('parseGenerateSeats', () => {
  const block = (o: Record<string, unknown> = {}) => ({ section: 'A', rowCount: 10, seatsPerRow: 20, tierId: TIER, ...o });

  it('accepts valid blocks', () => {
    expect(parseGenerateSeats({ eventId: VENUE, blocks: [block(), block({ section: 'B' })] }).blocks).toHaveLength(2);
  });

  it.each([
    ['no blocks', []],
    ['zero rows', [block({ rowCount: 0 })]],
    ['too many seats per row', [block({ seatsPerRow: 501 })]],
    ['missing tier', [block({ tierId: '' })]],
    ['duplicate section', [block(), block()]],
  ])('rejects %s', (_, blocks) => {
    expect(rpcCode(() => parseGenerateSeats({ eventId: VENUE, blocks: blocks as any }))).toBe(status.INVALID_ARGUMENT);
  });
});

describe('page token', () => {
  it('round-trips the cursor', () => {
    const cursor = { startsAt: new Date('2026-12-01T12:00:00.123Z'), id: VENUE };
    expect(decodePageToken(encodePageToken(cursor))).toEqual(cursor);
  });

  it.each(['garbage', Buffer.from('[1,2]').toString('base64url'), Buffer.from('{}').toString('base64url')])(
    'rejects a tampered token %s',
    (token) => {
      expect(rpcCode(() => decodePageToken(token))).toBe(status.INVALID_ARGUMENT);
    },
  );

  it('clamps page size', () => {
    expect(parseListEvents({ pageSize: 0, pageToken: '', includeDrafts: false }).pageSize).toBe(20);
    expect(parseListEvents({ pageSize: 1000, pageToken: '', includeDrafts: false }).pageSize).toBe(100);
  });
});
