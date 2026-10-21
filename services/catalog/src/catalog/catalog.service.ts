import { Inject, Injectable } from '@nestjs/common';
import type { Metadata } from '@grpc/grpc-js';
import { failedPrecondition, forwardMetadata, invalidArg, notFound } from '../common/rpc';
import { APP_CONFIG, type AppConfig } from '../config';
import { EventStatus, Prisma } from '../generated/prisma/client';
import { InventoryClient } from '../inventory/inventory.client';
import { PrismaService } from '../prisma/prisma.service';
import type { Cursor, EventInput, ListEventsInput, SeatBlockInput, VenueInput } from './validation';

export const EVENT_INCLUDE = {
  venue: true,
  tiers: { orderBy: [{ priceAmount: 'desc' }, { name: 'asc' }] },
} satisfies Prisma.EventInclude;

export type EventWithRelations = Prisma.EventGetPayload<{ include: typeof EVENT_INCLUDE }>;

// So sánh "A" < "B" < "AA" và "2" < "10" như con người đọc sơ đồ ghế.
const natural = new Intl.Collator('en', { numeric: true });

@Injectable()
export class CatalogService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly inventory: InventoryClient,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {}

  createVenue(input: VenueInput) {
    return this.prisma.venue.create({ data: input });
  }

  async createEvent({ tiers, ...input }: EventInput): Promise<EventWithRelations> {
    const venue = await this.prisma.venue.findUnique({ where: { id: input.venueId }, select: { id: true } });
    if (!venue) throw notFound('venue not found');
    return this.prisma.event.create({
      data: { ...input, tiers: { create: tiers } },
      include: EVENT_INCLUDE,
    });
  }

  async getEvent(id: string): Promise<EventWithRelations> {
    const event = await this.prisma.event.findUnique({ where: { id }, include: EVENT_INCLUDE });
    if (!event) throw notFound('event not found');
    return event;
  }

  /** Phân trang keyset theo (starts_at, id): ổn định khi có event mới chen vào giữa các trang. */
  async listEvents({ pageSize, cursor, includeDrafts }: ListEventsInput) {
    const rows = await this.prisma.event.findMany({
      where: {
        ...(includeDrafts ? {} : { status: EventStatus.published }),
        ...(cursor && {
          OR: [{ startsAt: { gt: cursor.startsAt } }, { startsAt: cursor.startsAt, id: { gt: cursor.id } }],
        }),
      },
      include: EVENT_INCLUDE,
      orderBy: [{ startsAt: 'asc' }, { id: 'asc' }],
      take: pageSize + 1,
    });
    const events = rows.slice(0, pageSize);
    const last = events.at(-1);
    const next: Cursor | undefined = rows.length > pageSize && last ? { startsAt: last.startsAt, id: last.id } : undefined;
    return { events, next };
  }

  async listSeats(eventId: string) {
    const event = await this.prisma.event.findUnique({ where: { id: eventId }, select: { id: true } });
    if (!event) throw notFound('event not found');
    const seats = await this.prisma.seat.findMany({ where: { eventId } });
    return seats.sort(
      (a, b) => natural.compare(a.section, b.section) || natural.compare(a.row, b.row) || a.number - b.number,
    );
  }

  /**
   * Sinh ghế cho event còn draft. Idempotent: ghế (section, row, number) đã có thì giữ nguyên,
   * nên gọi lại cùng request không tạo trùng, còn tăng row_count thì chỉ thêm các hàng mới.
   * Trả về tổng số ghế của event.
   */
  async generateSeats(eventId: string, blocks: SeatBlockInput[]): Promise<number> {
    const max = this.config.maxSeatsPerEvent;
    const requested = blocks.reduce((sum, b) => sum + b.rowCount * b.seatsPerRow, 0);
    if (requested > max) throw invalidArg(`request has ${requested} seats, at most ${max} per event`);

    return this.prisma.$transaction(
      async (tx) => {
        const status = await lockEvent(tx, eventId);
        if (status !== EventStatus.draft) throw failedPrecondition('seat map is locked after publish');

        const tierIds = [...new Set(blocks.map((b) => b.tierId))];
        const found = await tx.ticketTier.count({ where: { eventId, id: { in: tierIds } } });
        if (found !== tierIds.length) throw invalidArg('tier_id does not belong to this event');

        for (const b of blocks) {
          await tx.$executeRaw`
            INSERT INTO seats (event_id, tier_id, section, "row", number)
            SELECT ${eventId}::uuid, ${b.tierId}::uuid, ${b.section}, r::text, n
            FROM generate_series(1, ${b.rowCount}::int) AS r,
                 generate_series(1, ${b.seatsPerRow}::int) AS n
            ON CONFLICT (event_id, section, "row", number) DO NOTHING`;
        }
        const total = await tx.seat.count({ where: { eventId } });
        // throw trong transaction => rollback toàn bộ các block vừa insert
        if (total > max) throw invalidArg(`event would have ${total} seats, at most ${max} allowed`);
        return total;
      },
      { timeout: 30_000 },
    );
  }

  /**
   * DRAFT -> PUBLISHED. Khóa hàng event (FOR UPDATE) trong suốt quá trình nên GenerateSeats chạy song song
   * phải chờ, danh sách ghế gửi sang inventory luôn là danh sách cuối cùng.
   *
   * Thứ tự: InitEventInventory trước, đổi status sau. Nếu inventory lỗi thì event vẫn draft và admin
   * gọi lại được; nếu inventory xong mà commit lỗi thì lần gọi lại chỉ init lại (idempotent).
   * Event đã publish thì trả về luôn, không gọi inventory lần nữa.
   */
  async publishEvent(eventId: string, md: Metadata | undefined): Promise<EventWithRelations> {
    await this.prisma.$transaction(
      async (tx) => {
        const status = await lockEvent(tx, eventId);
        if (status === EventStatus.published) return;

        const seats = await tx.seat.findMany({ where: { eventId }, select: { id: true }, orderBy: { id: 'asc' } });
        if (!seats.length) throw failedPrecondition('event has no seats');

        const { seatCount } = await this.inventory.initEventInventory(
          { eventId, seatIds: seats.map((s) => s.id) },
          forwardMetadata(md),
        );
        if (seatCount !== seats.length) {
          // không bao giờ xảy ra nếu chỉ catalog ghi vào inventory; dừng lại thay vì publish sơ đồ lệch
          throw new Error(`inventory has ${seatCount} seats for event ${eventId}, catalog has ${seats.length}`);
        }
        await tx.event.update({
          where: { id: eventId },
          data: { status: EventStatus.published, publishedAt: new Date() },
        });
      },
      // giữ transaction (và 1 connection) trong lúc gọi inventory: chấp nhận được với thao tác admin
      { timeout: this.config.grpcTimeoutMs + 10_000 },
    );
    return this.getEvent(eventId);
  }
}

async function lockEvent(tx: Prisma.TransactionClient, eventId: string): Promise<EventStatus> {
  const rows = await tx.$queryRaw<{ status: EventStatus }[]>`
    SELECT status FROM events WHERE id = ${eventId}::uuid FOR UPDATE`;
  if (!rows.length) throw notFound('event not found');
  return rows[0].status;
}
