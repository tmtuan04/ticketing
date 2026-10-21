import { toIso } from '../common/proto';
import { SeatStatus } from '../inventory/inventory.pb';
import { EventStatus, type Event, type Seat } from './catalog.pb';

const EVENT_STATUS: Partial<Record<EventStatus, string>> = {
  [EventStatus.EVENT_STATUS_DRAFT]: 'draft',
  [EventStatus.EVENT_STATUS_PUBLISHED]: 'published',
};

const SEAT_STATUS: Partial<Record<SeatStatus, string>> = {
  [SeatStatus.SEAT_STATUS_AVAILABLE]: 'available',
  [SeatStatus.SEAT_STATUS_HELD]: 'held',
  [SeatStatus.SEAT_STATUS_SOLD]: 'sold',
};

export const isPublished = (e: Event) => e.status === EventStatus.EVENT_STATUS_PUBLISHED;

export function toEventView(e: Event) {
  return {
    id: e.id,
    name: e.name,
    description: e.description,
    venue: e.venue ? { id: e.venue.id, name: e.venue.name, address: e.venue.address } : null,
    startsAt: toIso(e.startsAt),
    status: EVENT_STATUS[e.status] ?? 'unknown',
    tiers: e.tiers.map((t) => ({
      id: t.id,
      name: t.name,
      // int64 tới dạng string; catalog chặn giá ở 1e12 nên đổi sang number an toàn
      priceAmount: Number(t.priceAmount),
      currency: t.currency,
    })),
  };
}

/** Sơ đồ ghế (catalog) ghép với trạng thái từng ghế (inventory). */
export function toSeatView(s: Seat, status: SeatStatus | undefined) {
  return {
    seatId: s.id,
    section: s.section,
    row: s.row,
    number: s.number,
    tierId: s.tierId,
    status: (status !== undefined && SEAT_STATUS[status]) || 'unknown',
  };
}
