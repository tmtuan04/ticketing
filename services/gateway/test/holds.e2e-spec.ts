import { status } from '@grpc/grpc-js';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import {
  createApp,
  flushRedis,
  grpcError,
  registerUser,
  startFakeInventory,
} from './helpers';

const EVENT = randomUUID();
const SEAT_A = randomUUID();
const SEAT_B = randomUUID();

const ts = (iso: string) => ({ seconds: String(Date.parse(iso) / 1000), nanos: 0 });

function pbHold(userId: string, overrides: Record<string, unknown> = {}) {
  return {
    id: randomUUID(),
    eventId: EVENT,
    userId,
    seatIds: [SEAT_A, SEAT_B],
    status: 1, // HOLD_STATUS_ACTIVE
    expiresAt: ts('2026-10-15T10:05:00Z'),
    createdAt: ts('2026-10-15T10:00:00Z'),
    ...overrides,
  };
}

describe('Holds (e2e, fake inventory)', () => {
  let app: NestExpressApplication;
  let inventory: Awaited<ReturnType<typeof startFakeInventory>>;
  let alice: Awaited<ReturnType<typeof registerUser>>;
  let bob: Awaited<ReturnType<typeof registerUser>>;
  const http = () => request(app.getHttpServer());
  const holdReq = (token: string, key: string | null = randomUUID()) => {
    const r = http().post(`/events/${EVENT}/holds`).auth(token, { type: 'bearer' });
    return key ? r.set('idempotency-key', key) : r;
  };

  beforeAll(async () => {
    inventory = await startFakeInventory();
    app = await createApp({ INVENTORY_GRPC_URL: inventory.url, GRPC_TIMEOUT: '300ms' });
    alice = await registerUser(app);
    bob = await registerUser(app);
  });

  afterEach(() => inventory.reset());

  afterAll(async () => {
    await app.close();
    await inventory.stop();
  });

  it('holds seats for the authenticated user and propagates request metadata', async () => {
    inventory.handlers.HoldSeats = (req) => ({ hold: pbHold(req.userId) });

    const res = await holdReq(alice.accessToken, 'key-1')
      .set('x-request-id', 'req-123')
      .send({ seatIds: [SEAT_A, SEAT_B] })
      .expect(201);

    expect(res.body).toMatchObject({
      eventId: EVENT,
      userId: alice.user.id,
      seatIds: [SEAT_A, SEAT_B],
      status: 'active',
      expiresAt: '2026-10-15T10:05:00.000Z',
      createdAt: '2026-10-15T10:00:00.000Z',
    });
    expect(res.headers['x-request-id']).toBe('req-123');

    const [call] = inventory.calls;
    // user_id lấy từ JWT, client không tự khai được
    expect(call.request).toEqual({
      eventId: EVENT,
      userId: alice.user.id,
      seatIds: [SEAT_A, SEAT_B],
      idempotencyKey: 'key-1',
    });
    expect(call.metadata).toMatchObject({ 'x-request-id': 'req-123', 'user-id': alice.user.id });
  });

  it('returns 409 with the seats that were taken', async () => {
    inventory.handlers.HoldSeats = () => ({ unavailable: { seatIds: [SEAT_B] } });
    const res = await holdReq(alice.accessToken).send({ seatIds: [SEAT_A, SEAT_B] }).expect(409);
    expect(res.body).toMatchObject({ message: 'some seats are not available', unavailableSeatIds: [SEAT_B] });
  });

  it('requires authentication and an Idempotency-Key', async () => {
    await http().post(`/events/${EVENT}/holds`).send({ seatIds: [SEAT_A] }).expect(401);
    await holdReq(alice.accessToken, null).send({ seatIds: [SEAT_A] }).expect(400);
    expect(inventory.calls).toHaveLength(0);
  });

  it.each([
    [{ seatIds: [] }],
    [{ seatIds: ['not-a-uuid'] }],
    [{ seatIds: [SEAT_A, SEAT_A] }],
    [{}],
  ])('validates the body %j before calling inventory', async (body) => {
    await holdReq(alice.accessToken).send(body).expect(400);
    expect(inventory.calls).toHaveLength(0);
  });

  it.each([
    [status.ALREADY_EXISTS, 'user already has an active hold for this event', 409],
    [status.INVALID_ARGUMENT, 'at most 8 seats per hold', 400],
    [status.FAILED_PRECONDITION, 'hold has expired', 409],
  ])('maps gRPC %s to HTTP %d with the inventory message', async (code, details, http) => {
    inventory.handlers.HoldSeats = () => {
      throw grpcError(code, details);
    };
    const res = await holdReq(alice.accessToken).send({ seatIds: [SEAT_A] }).expect(http);
    expect(res.body.message).toBe(details);
  });

  it('hides internal error details', async () => {
    inventory.handlers.HoldSeats = () => {
      throw grpcError(status.INTERNAL, 'pq: password authentication failed');
    };
    const res = await holdReq(alice.accessToken).send({ seatIds: [SEAT_A] }).expect(500);
    expect(JSON.stringify(res.body)).not.toContain('pq:');
  });

  it('returns 504 when inventory is too slow', async () => {
    inventory.handlers.HoldSeats = (req) =>
      new Promise((resolve) => setTimeout(() => resolve({ hold: pbHold(req.userId) }), 1_000));
    await holdReq(alice.accessToken).send({ seatIds: [SEAT_A] }).expect(504);
  });

  it('lets only the owner (or an admin) read a hold', async () => {
    const hold = pbHold(alice.user.id);
    inventory.handlers.GetHold = () => ({ hold });
    await http().get(`/holds/${hold.id}`).auth(alice.accessToken, { type: 'bearer' }).expect(200);
    await http().get(`/holds/${hold.id}`).auth(bob.accessToken, { type: 'bearer' }).expect(403);
  });

  it('maps NOT_FOUND to 404 and rejects malformed ids', async () => {
    inventory.handlers.GetHold = () => {
      throw grpcError(status.NOT_FOUND, 'hold not found');
    };
    await http().get(`/holds/${randomUUID()}`).auth(alice.accessToken, { type: 'bearer' }).expect(404);
    await http().get('/holds/123').auth(alice.accessToken, { type: 'bearer' }).expect(400);
  });

  it('releases a hold on behalf of the caller', async () => {
    const hold = pbHold(alice.user.id, { status: 2 });
    inventory.handlers.ReleaseHold = (req) => {
      if (req.userId !== alice.user.id) throw grpcError(status.PERMISSION_DENIED, 'hold belongs to another user');
      return { hold };
    };
    const res = await http().delete(`/holds/${hold.id}`).auth(alice.accessToken, { type: 'bearer' }).expect(200);
    expect(res.body.status).toBe('released');
    await http().delete(`/holds/${hold.id}`).auth(bob.accessToken, { type: 'bearer' }).expect(403);
  });
});

describe('Holds when inventory is down (e2e)', () => {
  let app: NestExpressApplication;

  beforeAll(async () => {
    // cổng không có ai lắng nghe
    app = await createApp({ INVENTORY_GRPC_URL: '127.0.0.1:1', GRPC_TIMEOUT: '2s' });
  });

  afterAll(async () => {
    await app.close();
  });

  it('returns 503 instead of hanging', async () => {
    const { accessToken } = await registerUser(app);
    await request(app.getHttpServer())
      .post(`/events/${EVENT}/holds`)
      .auth(accessToken, { type: 'bearer' })
      .set('idempotency-key', 'k')
      .send({ seatIds: [SEAT_A] })
      .expect(503);
  });
});

describe('Hold rate limit (e2e)', () => {
  let app: NestExpressApplication;
  let inventory: Awaited<ReturnType<typeof startFakeInventory>>;

  beforeAll(async () => {
    await flushRedis();
    inventory = await startFakeInventory();
    inventory.handlers.HoldSeats = () => ({ unavailable: { seatIds: [SEAT_A] } });
    app = await createApp({ INVENTORY_GRPC_URL: inventory.url, RATE_LIMIT_HOLD: '2' });
  });

  afterAll(async () => {
    await app.close();
    await inventory.stop();
  });

  it('applies the stricter hold limit per user', async () => {
    const alice = await registerUser(app);
    const bob = await registerUser(app);
    const hold = (token: string) =>
      request(app.getHttpServer())
        .post(`/events/${EVENT}/holds`)
        .auth(token, { type: 'bearer' })
        .set('idempotency-key', randomUUID())
        .send({ seatIds: [SEAT_A] });

    expect((await hold(alice.accessToken)).status).toBe(409);
    expect((await hold(alice.accessToken)).status).toBe(409);
    expect((await hold(alice.accessToken)).status).toBe(429);
    expect((await hold(bob.accessToken)).status).toBe(409);
    expect(inventory.calls).toHaveLength(3);
  });
});
