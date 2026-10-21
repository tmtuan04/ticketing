import { invalidArg } from '../common/rpc';
import type {
  CreateEventRequest,
  CreateVenueRequest,
  GenerateSeatsRequest,
  ListEventsRequest,
  Timestamp,
} from './catalog.pb';

// Kiểm tra request gRPC và chuyển sang kiểu domain. Gateway chỉ kiểm tra hình dạng payload;
// quy tắc nghiệp vụ nằm ở đây để mọi client (gateway, script seed...) đều bị ràng buộc như nhau.

export const LIMITS = {
  tiersPerEvent: 20,
  blocksPerRequest: 50,
  rowsPerBlock: 500,
  seatsPerRow: 500,
  maxPrice: 1_000_000_000_000n,
  defaultPageSize: 20,
  maxPageSize: 100,
} as const;

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function uuid(field: string, v: string | undefined): string {
  if (!v) throw invalidArg(`${field} is required`);
  if (!UUID_RE.test(v)) throw invalidArg(`${field} must be a UUID`);
  return v.toLowerCase();
}

function text(field: string, v: string | undefined, max: number, optional = false): string {
  const s = (v ?? '').trim();
  if (!s && !optional) throw invalidArg(`${field} is required`);
  if (s.length > max) throw invalidArg(`${field} must be at most ${max} characters`);
  return s;
}

/** Timestamp từ proto-loader có seconds dạng string (longs: String). */
export function fromTimestamp(field: string, ts: Timestamp | null | undefined): Date {
  if (!ts) throw invalidArg(`${field} is required`);
  const ms = Number(ts.seconds) * 1000 + Math.floor((ts.nanos ?? 0) / 1e6);
  if (!Number.isFinite(ms)) throw invalidArg(`${field} is not a valid timestamp`);
  return new Date(ms);
}

export interface VenueInput {
  name: string;
  address: string;
}

export function parseCreateVenue(req: CreateVenueRequest): VenueInput {
  return { name: text('name', req.name, 200), address: text('address', req.address, 500, true) };
}

export interface TierInput {
  name: string;
  priceAmount: bigint;
  currency: string;
}

export interface EventInput {
  name: string;
  description: string;
  venueId: string;
  startsAt: Date;
  tiers: TierInput[];
}

export function parseCreateEvent(req: CreateEventRequest, now = new Date()): EventInput {
  const startsAt = fromTimestamp('starts_at', req.startsAt);
  if (startsAt <= now) throw invalidArg('starts_at must be in the future');

  const tiers = req.tiers ?? [];
  if (!tiers.length) throw invalidArg('at least one tier is required');
  if (tiers.length > LIMITS.tiersPerEvent) throw invalidArg(`at most ${LIMITS.tiersPerEvent} tiers per event`);
  const parsed = tiers.map((t, i) => ({
    name: text(`tiers[${i}].name`, t.name, 50),
    priceAmount: price(`tiers[${i}].price_amount`, t.priceAmount),
    currency: currency(`tiers[${i}].currency`, t.currency),
  }));
  const names = new Set(parsed.map((t) => t.name.toLowerCase()));
  if (names.size !== parsed.length) throw invalidArg('tier names must be unique');
  // một đơn hàng chỉ có một loại tiền
  if (new Set(parsed.map((t) => t.currency)).size > 1) throw invalidArg('all tiers must use the same currency');

  return {
    name: text('name', req.name, 200),
    description: text('description', req.description, 5000, true),
    venueId: uuid('venue_id', req.venueId),
    startsAt,
    tiers: parsed,
  };
}

/** int64 tới dưới dạng string (longs: String); số âm, số lẻ hay vượt giới hạn đều bị từ chối. */
function price(field: string, v: number | string | undefined): bigint {
  const s = String(v ?? '');
  if (!/^\d+$/.test(s)) throw invalidArg(`${field} must be a non-negative integer`);
  const n = BigInt(s);
  if (n > LIMITS.maxPrice) throw invalidArg(`${field} must be at most ${LIMITS.maxPrice}`);
  return n;
}

function currency(field: string, v: string | undefined): string {
  const c = (v ?? '').trim().toUpperCase() || 'VND';
  if (!/^[A-Z]{3}$/.test(c)) throw invalidArg(`${field} must be an ISO 4217 code`);
  return c;
}

export interface SeatBlockInput {
  section: string;
  rowCount: number;
  seatsPerRow: number;
  tierId: string;
}

export function parseGenerateSeats(req: GenerateSeatsRequest): { eventId: string; blocks: SeatBlockInput[] } {
  const eventId = uuid('event_id', req.eventId);
  const blocks = req.blocks ?? [];
  if (!blocks.length) throw invalidArg('at least one block is required');
  if (blocks.length > LIMITS.blocksPerRequest) {
    throw invalidArg(`at most ${LIMITS.blocksPerRequest} blocks per request`);
  }
  const parsed = blocks.map((b, i) => ({
    section: text(`blocks[${i}].section`, b.section, 20),
    rowCount: count(`blocks[${i}].row_count`, b.rowCount, LIMITS.rowsPerBlock),
    seatsPerRow: count(`blocks[${i}].seats_per_row`, b.seatsPerRow, LIMITS.seatsPerRow),
    tierId: uuid(`blocks[${i}].tier_id`, b.tierId),
  }));
  // hai block cùng section sẽ đè lên nhau (hàng/ghế đều đánh số từ 1)
  if (new Set(parsed.map((b) => b.section)).size !== parsed.length) {
    throw invalidArg('sections must be unique within a request');
  }
  return { eventId, blocks: parsed };
}

function count(field: string, v: number | undefined, max: number): number {
  if (!Number.isInteger(v) || v! < 1 || v! > max) throw invalidArg(`${field} must be between 1 and ${max}`);
  return v!;
}

export interface Cursor {
  startsAt: Date;
  id: string;
}

export interface ListEventsInput {
  pageSize: number;
  cursor?: Cursor;
  includeDrafts: boolean;
}

export function parseListEvents(req: ListEventsRequest): ListEventsInput {
  const size = req.pageSize ?? 0;
  if (!Number.isInteger(size) || size < 0) throw invalidArg('page_size must be a non-negative integer');
  return {
    pageSize: Math.min(size || LIMITS.defaultPageSize, LIMITS.maxPageSize),
    cursor: req.pageToken ? decodePageToken(req.pageToken) : undefined,
    includeDrafts: !!req.includeDrafts,
  };
}

/** Page token là con trỏ keyset (starts_at, id) của phần tử cuối trang trước, mã hóa base64url. */
export function encodePageToken(c: Cursor): string {
  return Buffer.from(JSON.stringify([c.startsAt.toISOString(), c.id])).toString('base64url');
}

export function decodePageToken(token: string): Cursor {
  try {
    const [s, id] = JSON.parse(Buffer.from(token, 'base64url').toString('utf8')) as unknown[];
    const startsAt = new Date(String(s));
    if (typeof s === 'string' && !Number.isNaN(startsAt.getTime()) && typeof id === 'string' && UUID_RE.test(id)) {
      return { startsAt, id };
    }
  } catch {
    // rơi xuống lỗi chung bên dưới
  }
  throw invalidArg('page_token is invalid');
}
