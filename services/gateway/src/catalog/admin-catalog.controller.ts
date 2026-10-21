import { Body, Controller, Get, HttpCode, HttpStatus, Param, ParseUUIDPipe, Post, Query } from '@nestjs/common';
import type { Metadata } from '@grpc/grpc-js';
import { GrpcMeta, Roles } from '../common/decorators';
import { fromDate } from '../common/proto';
import { CatalogClient } from './catalog.client';
import { CreateEventDto, CreateVenueDto, GenerateSeatsDto, ListEventsQuery } from './dto';
import { toEventView } from './views';

const uuid = new ParseUUIDPipe();

/** Quản trị event: tạo venue/event, sinh sơ đồ ghế, publish. Thấy cả event draft. */
@Roles('admin')
@Controller('admin')
export class AdminCatalogController {
  constructor(private readonly catalog: CatalogClient) {}

  @Post('venues')
  async createVenue(@Body() dto: CreateVenueDto, @GrpcMeta() md: Metadata) {
    const { venue } = await this.catalog.createVenue({ name: dto.name, address: dto.address ?? '' }, md);
    return venue;
  }

  @Post('events')
  async createEvent(@Body() dto: CreateEventDto, @GrpcMeta() md: Metadata) {
    const { event } = await this.catalog.createEvent(
      {
        name: dto.name,
        description: dto.description ?? '',
        venueId: dto.venueId,
        startsAt: fromDate(new Date(dto.startsAt)),
        tiers: dto.tiers.map((t) => ({ name: t.name, priceAmount: t.priceAmount, currency: t.currency ?? '' })),
      },
      md,
    );
    return toEventView(event!);
  }

  @Get('events')
  async listEvents(@Query() q: ListEventsQuery, @GrpcMeta() md: Metadata) {
    const res = await this.catalog.listEvents(
      { pageSize: q.pageSize ?? 0, pageToken: q.pageToken ?? '', includeDrafts: true },
      md,
    );
    return { events: res.events.map(toEventView), nextPageToken: res.nextPageToken || null };
  }

  @Get('events/:eventId')
  async getEvent(@Param('eventId', uuid) eventId: string, @GrpcMeta() md: Metadata) {
    const { event } = await this.catalog.getEvent({ eventId }, md);
    return toEventView(event!);
  }

  /** Idempotent: ghế đã có thì giữ nguyên. Trả về tổng số ghế của event. */
  @Post('events/:eventId/seats')
  @HttpCode(HttpStatus.OK)
  async generateSeats(
    @Param('eventId', uuid) eventId: string,
    @Body() dto: GenerateSeatsDto,
    @GrpcMeta() md: Metadata,
  ) {
    const { seatCount } = await this.catalog.generateSeats({ eventId, blocks: dto.blocks }, md);
    return { eventId, seatCount };
  }

  /** Idempotent: event đã publish trả về như cũ. Sau khi publish không sửa được sơ đồ ghế. */
  @Post('events/:eventId/publish')
  @HttpCode(HttpStatus.OK)
  async publish(@Param('eventId', uuid) eventId: string, @GrpcMeta() md: Metadata) {
    const { event } = await this.catalog.publishEvent({ eventId }, md);
    return toEventView(event!);
  }
}
