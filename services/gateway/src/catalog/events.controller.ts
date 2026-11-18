import { Controller, Get, NotFoundException, Param, ParseUUIDPipe, Query } from '@nestjs/common';
import type { Metadata } from '@grpc/grpc-js';
import { GrpcMeta, Public } from '../common/decorators';
import { InventoryClient } from '../inventory/inventory.client';
import type { SeatStatus } from '../inventory/inventory.pb';
import { PublishedCatalogCache } from './catalog.cache';
import { CatalogClient } from './catalog.client';
import { ListEventsQuery } from './dto';
import { isPublished, toEventView, toSeatView } from './views';

const uuid = new ParseUUIDPipe();

/** API công khai: chỉ thấy event đã publish, event draft trả 404 như không tồn tại. */
@Public()
@Controller('events')
export class EventsController {
  constructor(
    private readonly catalog: CatalogClient,
    private readonly cache: PublishedCatalogCache,
    private readonly inventory: InventoryClient,
  ) {}

  @Get()
  async list(@Query() q: ListEventsQuery, @GrpcMeta() md: Metadata) {
    const res = await this.catalog.listEvents(
      { pageSize: q.pageSize ?? 0, pageToken: q.pageToken ?? '', includeDrafts: false },
      md,
    );
    return { events: res.events.map(toEventView), nextPageToken: res.nextPageToken || null };
  }

  @Get(':eventId')
  async get(@Param('eventId', uuid) eventId: string, @GrpcMeta() md: Metadata) {
    const event = await this.cache.getEvent(eventId, md);
    if (!isPublished(event)) throw new NotFoundException('event not found');
    return toEventView(event);
  }

  /**
   * Sơ đồ ghế kèm trạng thái. Phần catalog (ghế, hạng vé) bất biến sau publish nên lấy từ cache,
   * trạng thái ghế thì luôn hỏi inventory. Gọi song song để không cộng dồn độ trễ.
   */
  @Get(':eventId/seats')
  async seats(@Param('eventId', uuid) eventId: string, @GrpcMeta() md: Metadata) {
    const [{ event, seats }, availability] = await Promise.all([
      this.cache.getSeatMap(eventId, md),
      this.inventory.listSeatAvailability({ eventId }, md),
    ]);
    if (!isPublished(event)) throw new NotFoundException('event not found');
    const status = new Map<string, SeatStatus>(availability.seats.map((s) => [s.seatId, s.status]));
    return { eventId, seats: seats.map((s) => toSeatView(s, status.get(s.id))) };
  }
}
