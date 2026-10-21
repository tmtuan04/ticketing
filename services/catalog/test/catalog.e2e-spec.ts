import { Metadata, credentials, makeGenericClientConstructor, status, type ServiceDefinition } from '@grpc/grpc-js';
import { service as healthService } from 'grpc-health-check';
import { PrismaService } from '../src/prisma/prisma.service';
import { grpcError, rpcError, startCatalog, startFakeInventory } from './helpers';

const DRAFT = 1;
const PUBLISHED = 2;
const inFuture = (days: number) => ({ seconds: String(Math.floor(Date.now() / 1000) + days * 86400), nanos: 0 });

describe('Catalog (e2e)', () => {
  let catalog: Awaited<ReturnType<typeof startCatalog>>;
  let inventory: Awaited<ReturnType<typeof startFakeInventory>>;

  /** Inventory giả hành xử như thật: trả về số ghế đã nhận. */
  const acceptAll = () => {
    inventory.handlers.InitEventInventory = (req) => ({ seatCount: req.seatIds.length });
  };

  async function createEvent(days = 30, name = 'Concert') {
    const { venue } = await catalog.admin('CreateVenue', { name: 'My Dinh Stadium', address: 'Ha Noi' });
    const { event } = await catalog.admin('CreateEvent', {
      name,
      description: 'Flash sale',
      venueId: venue.id,
      startsAt: inFuture(days),
      tiers: [
        { name: 'VIP', priceAmount: '2000000', currency: 'VND' },
        { name: 'Standard', priceAmount: '500000' },
      ],
    });
    const tier = (n: string) => event.tiers.find((t: any) => t.name === n).id as string;
    return { event, vip: tier('VIP'), standard: tier('Standard') };
  }

  beforeAll(async () => {
    inventory = await startFakeInventory();
    catalog = await startCatalog({ INVENTORY_GRPC_URL: inventory.url, MAX_SEATS_PER_EVENT: '100' });
  });

  afterEach(() => inventory.reset());

  afterAll(async () => {
    await catalog.close();
    await inventory.stop();
  });

  it('creates an event with its venue and tiers as a draft', async () => {
    const { event } = await createEvent();
    expect(event).toMatchObject({
      name: 'Concert',
      status: DRAFT,
      venue: { name: 'My Dinh Stadium', address: 'Ha Noi' },
      // sắp theo giá giảm dần; currency rỗng mặc định là VND
      tiers: [
        { name: 'VIP', priceAmount: '2000000', currency: 'VND' },
        { name: 'Standard', priceAmount: '500000', currency: 'VND' },
      ],
    });
    const { event: fetched } = await catalog.read('GetEvent', { eventId: event.id });
    expect(fetched).toEqual(event);
  });

  it('rejects an unknown venue and invalid input', async () => {
    const missingVenue = await rpcError(
      catalog.admin('CreateEvent', {
        name: 'x',
        venueId: '00000000-0000-4000-8000-000000000000',
        startsAt: inFuture(1),
        tiers: [{ name: 'A', priceAmount: '1' }],
      }),
    );
    expect(missingVenue).toEqual({ code: status.NOT_FOUND, details: 'venue not found' });

    const bad = await rpcError(catalog.read('GetEvent', { eventId: 'nope' }));
    expect(bad.code).toBe(status.INVALID_ARGUMENT);
    const missing = await rpcError(catalog.read('GetEvent', { eventId: '00000000-0000-4000-8000-000000000000' }));
    expect(missing.code).toBe(status.NOT_FOUND);
  });

  describe('seat map', () => {
    it('generates seats per block and lists them in reading order', async () => {
      const { event, vip, standard } = await createEvent();
      const { seatCount } = await catalog.admin('GenerateSeats', {
        eventId: event.id,
        blocks: [
          { section: 'B', rowCount: 1, seatsPerRow: 2, tierId: standard },
          { section: 'A', rowCount: 10, seatsPerRow: 2, tierId: vip },
        ],
      });
      expect(seatCount).toBe(22);

      const { seats } = await catalog.read('ListSeats', { eventId: event.id });
      expect(seats).toHaveLength(22);
      const labels = seats.map((s: any) => `${s.section}-${s.row}-${s.number}`);
      // hàng sắp theo số: 2 đứng trước 10
      expect(labels.slice(0, 4)).toEqual(['A-1-1', 'A-1-2', 'A-2-1', 'A-2-2']);
      expect(labels.slice(-4)).toEqual(['A-10-1', 'A-10-2', 'B-1-1', 'B-1-2']);
      expect(seats.find((s: any) => s.section === 'B').tierId).toBe(standard);
    });

    it('is idempotent and only adds new rows when a block grows', async () => {
      const { event, vip } = await createEvent();
      const req = (rowCount: number) => ({
        eventId: event.id,
        blocks: [{ section: 'A', rowCount, seatsPerRow: 5, tierId: vip }],
      });
      expect((await catalog.admin('GenerateSeats', req(2))).seatCount).toBe(10);
      const before = (await catalog.read('ListSeats', { eventId: event.id })).seats;
      expect((await catalog.admin('GenerateSeats', req(2))).seatCount).toBe(10);
      expect((await catalog.admin('GenerateSeats', req(3))).seatCount).toBe(15);

      // ghế cũ giữ nguyên id (inventory đã có thể tham chiếu tới chúng)
      const after = (await catalog.read('ListSeats', { eventId: event.id })).seats;
      expect(after.slice(0, 10)).toEqual(before);
    });

    it('rejects a tier from another event', async () => {
      const { event } = await createEvent();
      const other = await createEvent();
      const err = await rpcError(
        catalog.admin('GenerateSeats', {
          eventId: event.id,
          blocks: [{ section: 'A', rowCount: 1, seatsPerRow: 1, tierId: other.vip }],
        }),
      );
      expect(err).toEqual({ code: status.INVALID_ARGUMENT, details: 'tier_id does not belong to this event' });
    });

    it('enforces MAX_SEATS_PER_EVENT and rolls back the whole request', async () => {
      const { event, vip, standard } = await createEvent();
      await catalog.admin('GenerateSeats', {
        eventId: event.id,
        blocks: [{ section: 'A', rowCount: 8, seatsPerRow: 10, tierId: vip }],
      });
      const tooMany = await rpcError(
        catalog.admin('GenerateSeats', {
          eventId: event.id,
          blocks: [
            { section: 'B', rowCount: 1, seatsPerRow: 10, tierId: standard },
            { section: 'C', rowCount: 2, seatsPerRow: 10, tierId: standard },
          ],
        }),
      );
      expect(tooMany.code).toBe(status.INVALID_ARGUMENT);
      expect((await catalog.read('ListSeats', { eventId: event.id })).seats).toHaveLength(80);
    });
  });

  describe('publish', () => {
    it('initializes inventory with every seat, then flips the status', async () => {
      acceptAll();
      const { event, vip } = await createEvent();
      await catalog.admin('GenerateSeats', {
        eventId: event.id,
        blocks: [{ section: 'A', rowCount: 2, seatsPerRow: 3, tierId: vip }],
      });
      const md = new Metadata();
      md.set('x-request-id', 'req-publish-1');

      const { event: published } = await catalog.admin('PublishEvent', { eventId: event.id }, md);
      expect(published.status).toBe(PUBLISHED);

      const seatIds = (await catalog.read('ListSeats', { eventId: event.id })).seats.map((s: any) => s.id);
      expect(inventory.calls).toHaveLength(1);
      const [call] = inventory.calls;
      expect(call.request.eventId).toBe(event.id);
      expect([...call.request.seatIds].sort()).toEqual([...seatIds].sort());
      expect(call.metadata['x-request-id']).toBe('req-publish-1');
    });

    it('is idempotent and locks the seat map afterwards', async () => {
      acceptAll();
      const { event, vip } = await createEvent();
      const block = { section: 'A', rowCount: 1, seatsPerRow: 2, tierId: vip };
      await catalog.admin('GenerateSeats', { eventId: event.id, blocks: [block] });
      await catalog.admin('PublishEvent', { eventId: event.id });

      const again = await catalog.admin('PublishEvent', { eventId: event.id });
      expect(again.event.status).toBe(PUBLISHED);
      expect(inventory.calls).toHaveLength(1);

      const locked = await rpcError(
        catalog.admin('GenerateSeats', { eventId: event.id, blocks: [{ ...block, section: 'B' }] }),
      );
      expect(locked).toEqual({ code: status.FAILED_PRECONDITION, details: 'seat map is locked after publish' });
    });

    it('refuses to publish an event without seats', async () => {
      acceptAll();
      const { event } = await createEvent();
      const err = await rpcError(catalog.admin('PublishEvent', { eventId: event.id }));
      expect(err).toEqual({ code: status.FAILED_PRECONDITION, details: 'event has no seats' });
      expect(inventory.calls).toHaveLength(0);
    });

    it('stays a draft when inventory is down, and a retry succeeds', async () => {
      const { event, vip } = await createEvent();
      await catalog.admin('GenerateSeats', {
        eventId: event.id,
        blocks: [{ section: 'A', rowCount: 1, seatsPerRow: 4, tierId: vip }],
      });

      inventory.handlers.InitEventInventory = () => {
        throw grpcError(status.UNAVAILABLE, 'connection refused');
      };
      const down = await rpcError(catalog.admin('PublishEvent', { eventId: event.id }));
      expect(down.code).toBe(status.UNAVAILABLE);
      expect((await catalog.read('GetEvent', { eventId: event.id })).event.status).toBe(DRAFT);

      acceptAll();
      const { event: published } = await catalog.admin('PublishEvent', { eventId: event.id });
      expect(published.status).toBe(PUBLISHED);
    });

    it('times out a slow inventory and keeps the event as a draft', async () => {
      const { event, vip } = await createEvent();
      await catalog.admin('GenerateSeats', {
        eventId: event.id,
        blocks: [{ section: 'A', rowCount: 1, seatsPerRow: 1, tierId: vip }],
      });
      inventory.handlers.InitEventInventory = () => new Promise(() => {});
      const err = await rpcError(catalog.admin('PublishEvent', { eventId: event.id }));
      expect(err.code).toBe(status.DEADLINE_EXCEEDED);
      expect((await catalog.read('GetEvent', { eventId: event.id })).event.status).toBe(DRAFT);
    });

    it('does not publish when inventory reports a different seat count', async () => {
      const { event, vip } = await createEvent();
      await catalog.admin('GenerateSeats', {
        eventId: event.id,
        blocks: [{ section: 'A', rowCount: 1, seatsPerRow: 3, tierId: vip }],
      });
      inventory.handlers.InitEventInventory = () => ({ seatCount: 99 });
      const err = await rpcError(catalog.admin('PublishEvent', { eventId: event.id }));
      // lỗi nội bộ không lộ chi tiết ra ngoài
      expect(err).toEqual({ code: status.INTERNAL, details: 'internal error' });
      expect((await catalog.read('GetEvent', { eventId: event.id })).event.status).toBe(DRAFT);
    });

    it('makes a concurrent GenerateSeats wait for publish, so inventory never misses a seat', async () => {
      const { event, vip, standard } = await createEvent();
      await catalog.admin('GenerateSeats', {
        eventId: event.id,
        blocks: [{ section: 'A', rowCount: 1, seatsPerRow: 2, tierId: vip }],
      });
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      inventory.handlers.InitEventInventory = async (req) => {
        await gate;
        return { seatCount: req.seatIds.length };
      };

      const publishing = catalog.admin('PublishEvent', { eventId: event.id });
      // chờ catalog gọi tới inventory, tức là đã giữ khóa hàng event
      while (!inventory.calls.length) await new Promise((r) => setTimeout(r, 10));
      const generating = rpcError(
        catalog.admin('GenerateSeats', {
          eventId: event.id,
          blocks: [{ section: 'B', rowCount: 1, seatsPerRow: 2, tierId: standard }],
        }),
      );
      await new Promise((r) => setTimeout(r, 100));
      release();

      expect((await publishing).event.status).toBe(PUBLISHED);
      expect((await generating).code).toBe(status.FAILED_PRECONDITION);
      expect((await catalog.read('ListSeats', { eventId: event.id })).seats).toHaveLength(2);
    });
  });

  describe('ListEvents', () => {
    beforeAll(async () => {
      await catalog.app.get(PrismaService).$executeRaw`TRUNCATE venues, events CASCADE`;
    });

    it('hides drafts by default and pages through events by start time', async () => {
      acceptAll();
      const names: string[] = [];
      for (const day of [5, 3, 4, 1, 2]) {
        const { event, vip } = await createEvent(day, `day-${day}`);
        names.push(event.name);
        await catalog.admin('GenerateSeats', {
          eventId: event.id,
          blocks: [{ section: 'A', rowCount: 1, seatsPerRow: 1, tierId: vip }],
        });
        if (day !== 4) await catalog.admin('PublishEvent', { eventId: event.id });
      }

      const page1 = await catalog.read('ListEvents', { pageSize: 2 });
      expect(page1.events.map((e: any) => e.name)).toEqual(['day-1', 'day-2']);
      expect(page1.nextPageToken).not.toBe('');
      const page2 = await catalog.read('ListEvents', { pageSize: 2, pageToken: page1.nextPageToken });
      expect(page2.events.map((e: any) => e.name)).toEqual(['day-3', 'day-5']);
      expect(page2.nextPageToken).toBe('');

      const all = await catalog.read('ListEvents', { includeDrafts: true });
      expect(all.events.map((e: any) => e.name)).toEqual(['day-1', 'day-2', 'day-3', 'day-4', 'day-5']);
    });

    it('rejects a tampered page token', async () => {
      const err = await rpcError(catalog.read('ListEvents', { pageToken: 'garbage' }));
      expect(err.code).toBe(status.INVALID_ARGUMENT);
    });
  });

  it('serves grpc.health.v1', async () => {
    const Health = makeGenericClientConstructor(healthService as unknown as ServiceDefinition, 'Health');
    const client = new Health(catalog.addr, credentials.createInsecure()) as any;
    const res = await new Promise<any>((resolve, reject) =>
      client.check({ service: '' }, (err: Error | null, r: unknown) => (err ? reject(err) : resolve(r))),
    );
    client.close();
    expect(res.status).toBe('SERVING');
  });
});
