import { status } from '@grpc/grpc-js';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import {
  ADMIN,
  createApp,
  grpcError,
  login,
  registerUser,
  startFakeCatalog,
  startFakeInventory,
} from './helpers';

const EVENT = randomUUID();
const VENUE = randomUUID();
const TIER = randomUUID();
const SEAT_A = randomUUID();
const SEAT_B = randomUUID();
const SEAT_C = randomUUID();

const DRAFT = 1;
const PUBLISHED = 2;

function pbEvent(overrides: Record<string, unknown> = {}) {
  return {
    id: EVENT,
    name: 'Concert',
    description: '',
    venue: { id: VENUE, name: 'My Dinh', address: 'Ha Noi' },
    startsAt: { seconds: String(Date.parse('2026-12-01T12:00:00Z') / 1000), nanos: 0 },
    status: PUBLISHED,
    // int64 tới dưới dạng string
    tiers: [{ id: TIER, name: 'VIP', priceAmount: '2000000', currency: 'VND' }],
    ...overrides,
  };
}

const eventView = {
  id: EVENT,
  name: 'Concert',
  description: '',
  venue: { id: VENUE, name: 'My Dinh', address: 'Ha Noi' },
  startsAt: '2026-12-01T12:00:00.000Z',
  status: 'published',
  tiers: [{ id: TIER, name: 'VIP', priceAmount: 2_000_000, currency: 'VND' }],
};

describe('Events & admin catalog (e2e, fake catalog + inventory)', () => {
  let app: NestExpressApplication;
  let catalog: Awaited<ReturnType<typeof startFakeCatalog>>;
  let inventory: Awaited<ReturnType<typeof startFakeInventory>>;
  let adminToken: string;
  let userToken: string;
  const http = () => request(app.getHttpServer());

  beforeAll(async () => {
    [catalog, inventory] = await Promise.all([startFakeCatalog(), startFakeInventory()]);
    app = await createApp({ CATALOG_GRPC_URL: catalog.url, INVENTORY_GRPC_URL: inventory.url, GRPC_TIMEOUT: '300ms' });
    adminToken = (await login(app, ADMIN.email, ADMIN.password)).accessToken;
    userToken = (await registerUser(app)).accessToken;
  });

  afterEach(() => {
    catalog.reset();
    inventory.reset();
  });

  afterAll(async () => {
    await app.close();
    await Promise.all([catalog.stop(), inventory.stop()]);
  });

  describe('public', () => {
    it('lists published events without authentication', async () => {
      catalog.handlers.ListEvents = () => ({ events: [pbEvent()], nextPageToken: 'next' });
      const res = await http().get('/events?pageSize=10&pageToken=abc').expect(200);
      expect(res.body).toEqual({ events: [eventView], nextPageToken: 'next' });
      expect(catalog.calls[0].request).toEqual({ pageSize: 10, pageToken: 'abc', includeDrafts: false });
    });

    it('returns null when there is no next page and validates the query', async () => {
      catalog.handlers.ListEvents = () => ({ events: [], nextPageToken: '' });
      expect((await http().get('/events').expect(200)).body.nextPageToken).toBeNull();
      await http().get('/events?pageSize=0').expect(400);
      await http().get('/events?pageSize=abc').expect(400);
    });

    it('hides draft events', async () => {
      catalog.handlers.GetEvent = () => ({ event: pbEvent({ status: DRAFT }) });
      await http().get(`/events/${EVENT}`).expect(404);
      catalog.handlers.GetEvent = () => ({ event: pbEvent() });
      expect((await http().get(`/events/${EVENT}`).expect(200)).body).toEqual(eventView);
    });

    it('maps catalog NOT_FOUND to 404', async () => {
      catalog.handlers.GetEvent = () => {
        throw grpcError(status.NOT_FOUND, 'event not found');
      };
      await http().get(`/events/${EVENT}`).expect(404);
    });

    it('merges the seat map with live availability', async () => {
      catalog.handlers.GetEvent = () => ({ event: pbEvent() });
      catalog.handlers.ListSeats = () => ({
        seats: [
          { id: SEAT_A, section: 'A', row: '1', number: 1, tierId: TIER },
          { id: SEAT_B, section: 'A', row: '1', number: 2, tierId: TIER },
          { id: SEAT_C, section: 'A', row: '1', number: 3, tierId: TIER },
        ],
      });
      inventory.handlers.ListSeatAvailability = () => ({
        seats: [
          { seatId: SEAT_B, status: 2 },
          { seatId: SEAT_A, status: 1 },
        ],
      });

      const res = await http().get(`/events/${EVENT}/seats`).expect(200);
      expect(res.body).toEqual({
        eventId: EVENT,
        seats: [
          { seatId: SEAT_A, section: 'A', row: '1', number: 1, tierId: TIER, status: 'available' },
          { seatId: SEAT_B, section: 'A', row: '1', number: 2, tierId: TIER, status: 'held' },
          // inventory không biết ghế này
          { seatId: SEAT_C, section: 'A', row: '1', number: 3, tierId: TIER, status: 'unknown' },
        ],
      });
    });

    it('does not expose the seat map of a draft event', async () => {
      catalog.handlers.GetEvent = () => ({ event: pbEvent({ status: DRAFT }) });
      catalog.handlers.ListSeats = () => ({ seats: [] });
      inventory.handlers.ListSeatAvailability = () => ({ seats: [] });
      await http().get(`/events/${EVENT}/seats`).expect(404);
    });
  });

  describe('admin', () => {
    it('requires the admin role', async () => {
      await http().post('/admin/venues').send({ name: 'x' }).expect(401);
      await http().post('/admin/venues').auth(userToken, { type: 'bearer' }).send({ name: 'x' }).expect(403);
      await http().get('/admin/events').auth(userToken, { type: 'bearer' }).expect(403);
      expect(catalog.calls).toHaveLength(0);
    });

    it('creates a venue', async () => {
      catalog.handlers.CreateVenue = (req) => ({ venue: { id: VENUE, ...req } });
      const res = await http()
        .post('/admin/venues')
        .auth(adminToken, { type: 'bearer' })
        .set('x-request-id', 'req-venue')
        .send({ name: 'My Dinh', address: 'Ha Noi' })
        .expect(201);
      expect(res.body).toEqual({ id: VENUE, name: 'My Dinh', address: 'Ha Noi' });
      expect(catalog.calls[0].metadata).toMatchObject({ 'x-request-id': 'req-venue' });
    });

    it('creates an event, converting startsAt and prices for gRPC', async () => {
      catalog.handlers.CreateEvent = () => ({ event: pbEvent({ status: DRAFT }) });
      const res = await http()
        .post('/admin/events')
        .auth(adminToken, { type: 'bearer' })
        .send({
          name: 'Concert',
          venueId: VENUE,
          startsAt: '2026-12-01T12:00:00.500Z',
          tiers: [{ name: 'VIP', priceAmount: 2_000_000 }],
        })
        .expect(201);
      expect(res.body.status).toBe('draft');
      expect(catalog.calls[0].request).toEqual({
        name: 'Concert',
        description: '',
        venueId: VENUE,
        startsAt: { seconds: String(Date.parse('2026-12-01T12:00:00Z') / 1000), nanos: 500_000_000 },
        tiers: [{ name: 'VIP', priceAmount: '2000000', currency: '' }],
      });
    });

    it.each([
      [{ name: 'x', venueId: 'nope', startsAt: '2026-12-01T12:00:00Z', tiers: [{ name: 'A', priceAmount: 1 }] }],
      [{ name: 'x', venueId: VENUE, startsAt: 'tomorrow', tiers: [{ name: 'A', priceAmount: 1 }] }],
      [{ name: 'x', venueId: VENUE, startsAt: '2026-12-01T12:00:00Z', tiers: [] }],
      [{ name: 'x', venueId: VENUE, startsAt: '2026-12-01T12:00:00Z', tiers: [{ name: 'A', priceAmount: 1.5 }] }],
      [{ name: 'x', venueId: VENUE, startsAt: '2026-12-01T12:00:00Z', tiers: [{ name: 'A', priceAmount: -1 }] }],
    ])('validates the event body %j before calling catalog', async (body) => {
      await http().post('/admin/events').auth(adminToken, { type: 'bearer' }).send(body).expect(400);
      expect(catalog.calls).toHaveLength(0);
    });

    it('passes catalog business errors through as 400', async () => {
      catalog.handlers.CreateEvent = () => {
        throw grpcError(status.INVALID_ARGUMENT, 'starts_at must be in the future');
      };
      const res = await http()
        .post('/admin/events')
        .auth(adminToken, { type: 'bearer' })
        .send({ name: 'x', venueId: VENUE, startsAt: '2020-01-01T00:00:00Z', tiers: [{ name: 'A', priceAmount: 1 }] })
        .expect(400);
      expect(res.body.message).toBe('starts_at must be in the future');
    });

    it('generates seats', async () => {
      catalog.handlers.GenerateSeats = () => ({ seatCount: 200 });
      const blocks = [{ section: 'A', rowCount: 10, seatsPerRow: 20, tierId: TIER }];
      const res = await http()
        .post(`/admin/events/${EVENT}/seats`)
        .auth(adminToken, { type: 'bearer' })
        .send({ blocks })
        .expect(200);
      expect(res.body).toEqual({ eventId: EVENT, seatCount: 200 });
      expect(catalog.calls[0].request).toEqual({ eventId: EVENT, blocks });

      await http()
        .post(`/admin/events/${EVENT}/seats`)
        .auth(adminToken, { type: 'bearer' })
        .send({ blocks: [{ section: 'A', rowCount: 0, seatsPerRow: 1, tierId: TIER }] })
        .expect(400);
    });

    it('publishes an event and maps a locked seat map to 409', async () => {
      catalog.handlers.PublishEvent = () => ({ event: pbEvent() });
      const res = await http().post(`/admin/events/${EVENT}/publish`).auth(adminToken, { type: 'bearer' }).expect(200);
      expect(res.body).toEqual(eventView);

      catalog.handlers.GenerateSeats = () => {
        throw grpcError(status.FAILED_PRECONDITION, 'seat map is locked after publish');
      };
      await http()
        .post(`/admin/events/${EVENT}/seats`)
        .auth(adminToken, { type: 'bearer' })
        .send({ blocks: [{ section: 'B', rowCount: 1, seatsPerRow: 1, tierId: TIER }] })
        .expect(409);
    });

    it('returns 503 when publish cannot reach inventory', async () => {
      catalog.handlers.PublishEvent = () => {
        throw grpcError(status.UNAVAILABLE, 'upstream unavailable');
      };
      await http().post(`/admin/events/${EVENT}/publish`).auth(adminToken, { type: 'bearer' }).expect(503);
    });

    it('sees drafts', async () => {
      catalog.handlers.ListEvents = () => ({ events: [pbEvent({ status: DRAFT })], nextPageToken: '' });
      catalog.handlers.GetEvent = () => ({ event: pbEvent({ status: DRAFT }) });
      const list = await http().get('/admin/events').auth(adminToken, { type: 'bearer' }).expect(200);
      expect(list.body.events[0].status).toBe('draft');
      expect(catalog.calls[0].request.includeDrafts).toBe(true);
      const one = await http().get(`/admin/events/${EVENT}`).auth(adminToken, { type: 'bearer' }).expect(200);
      expect(one.body.status).toBe('draft');
    });
  });
});
