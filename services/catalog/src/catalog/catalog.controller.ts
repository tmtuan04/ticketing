import { Controller } from '@nestjs/common';
import type { Metadata } from '@grpc/grpc-js';
import { EventStatus as DbEventStatus, type Seat as DbSeat, type Venue as DbVenue } from '../generated/prisma/client';
import { CatalogService, type EventWithRelations } from './catalog.service';
import {
  CatalogAdminServiceControllerMethods,
  CatalogServiceControllerMethods,
  EventStatus,
  type CatalogAdminServiceController,
  type CatalogServiceController,
  type CreateEventRequest,
  type CreateEventResponse,
  type CreateVenueRequest,
  type CreateVenueResponse,
  type Event,
  type GenerateSeatsRequest,
  type GenerateSeatsResponse,
  type GetEventRequest,
  type GetEventResponse,
  type ListEventsRequest,
  type ListEventsResponse,
  type ListSeatsRequest,
  type ListSeatsResponse,
  type PublishEventRequest,
  type PublishEventResponse,
  type Seat,
  type Timestamp,
  type Venue,
} from './catalog.pb';
import {
  encodePageToken,
  parseCreateEvent,
  parseCreateVenue,
  parseGenerateSeats,
  parseListEvents,
  uuid,
} from './validation';

@Controller()
@CatalogServiceControllerMethods()
export class CatalogController implements CatalogServiceController {
  constructor(private readonly catalog: CatalogService) {}

  async getEvent(req: GetEventRequest): Promise<GetEventResponse> {
    return { event: toEventPb(await this.catalog.getEvent(uuid('event_id', req.eventId))) };
  }

  async listEvents(req: ListEventsRequest): Promise<ListEventsResponse> {
    const { events, next } = await this.catalog.listEvents(parseListEvents(req));
    return { events: events.map(toEventPb), nextPageToken: next ? encodePageToken(next) : '' };
  }

  async listSeats(req: ListSeatsRequest): Promise<ListSeatsResponse> {
    const seats = await this.catalog.listSeats(uuid('event_id', req.eventId));
    return { seats: seats.map(toSeatPb) };
  }
}

/** API ghi; gateway đã kiểm tra role admin trước khi gọi. */
@Controller()
@CatalogAdminServiceControllerMethods()
export class CatalogAdminController implements CatalogAdminServiceController {
  constructor(private readonly catalog: CatalogService) {}

  async createVenue(req: CreateVenueRequest): Promise<CreateVenueResponse> {
    return { venue: toVenuePb(await this.catalog.createVenue(parseCreateVenue(req))) };
  }

  async createEvent(req: CreateEventRequest): Promise<CreateEventResponse> {
    return { event: toEventPb(await this.catalog.createEvent(parseCreateEvent(req))) };
  }

  async generateSeats(req: GenerateSeatsRequest): Promise<GenerateSeatsResponse> {
    const { eventId, blocks } = parseGenerateSeats(req);
    return { seatCount: await this.catalog.generateSeats(eventId, blocks) };
  }

  async publishEvent(req: PublishEventRequest, md?: Metadata): Promise<PublishEventResponse> {
    return { event: toEventPb(await this.catalog.publishEvent(uuid('event_id', req.eventId), md)) };
  }
}

function toVenuePb(v: DbVenue): Venue {
  return { id: v.id, name: v.name, address: v.address };
}

export function toEventPb(e: EventWithRelations): Event {
  return {
    id: e.id,
    name: e.name,
    description: e.description,
    venue: toVenuePb(e.venue),
    startsAt: toTimestamp(e.startsAt),
    status: e.status === DbEventStatus.published ? EventStatus.EVENT_STATUS_PUBLISHED : EventStatus.EVENT_STATUS_DRAFT,
    tiers: e.tiers.map((t) => ({
      id: t.id,
      name: t.name,
      // giá bị chặn ở 1e12 nên không mất chính xác khi đổi sang number
      priceAmount: Number(t.priceAmount),
      currency: t.currency,
    })),
  };
}

function toSeatPb(s: DbSeat): Seat {
  return { id: s.id, section: s.section, row: s.row, number: s.number, tierId: s.tierId };
}

function toTimestamp(d: Date): Timestamp {
  const ms = d.getTime();
  return { seconds: Math.floor(ms / 1000), nanos: (ms % 1000) * 1e6 };
}
