import { status } from '@grpc/grpc-js';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { ADMIN, createApp, flushRedis, grpcError, login, registerUser, startFakeOrder } from './helpers';

const EVENT = randomUUID();
const HOLD = randomUUID();
const SEAT = randomUUID();

const ts = (iso: string) => ({
  seconds: String(Date.parse(iso) / 1000),
  nanos: 0,
});

function pbOrder(userId: string, overrides: Record<string, unknown> = {}) {
  return {
    id: randomUUID(),
    userId,
    eventId: EVENT,
    holdId: HOLD,
    seatIds: [SEAT],
    amount: '2000000', // int64 => string (longs: String)
    currency: 'VND',
    status: 2, // ORDER_STATUS_AWAITING_PAYMENT
    paymentDeadline: ts('2026-10-15T10:10:00Z'),
    failureReason: '',
    createdAt: ts('2026-10-15T10:00:00Z'),
    updatedAt: ts('2026-10-15T10:00:00Z'),
    ...overrides,
  };
}

describe('Orders (e2e, fake order service)', () => {
  let app: NestExpressApplication;
  let order: Awaited<ReturnType<typeof startFakeOrder>>;
  let alice: Awaited<ReturnType<typeof registerUser>>;
  let bob: Awaited<ReturnType<typeof registerUser>>;
  let adminToken: string;
  const http = () => request(app.getHttpServer());
  const createReq = (token: string, key: string | null = randomUUID()) => {
    const r = http().post('/orders').auth(token, { type: 'bearer' });
    return key ? r.set('idempotency-key', key) : r;
  };

  beforeAll(async () => {
    order = await startFakeOrder();
    app = await createApp({ ORDER_GRPC_URL: order.url, GRPC_TIMEOUT: '300ms' });
    alice = await registerUser(app);
    bob = await registerUser(app);
    adminToken = (await login(app, ADMIN.email, ADMIN.password)).accessToken;
  });

  afterEach(() => order.reset());

  afterAll(async () => {
    await app.close();
    await order.stop();
  });

  it('creates an order for the authenticated user and propagates request metadata', async () => {
    order.handlers.CreateOrder = (req) => ({ order: pbOrder(req.userId) });

    const res = await createReq(alice.accessToken, 'order-key-1')
      .set('x-request-id', 'req-order')
      .send({ holdId: HOLD })
      .expect(201);

    expect(res.body).toMatchObject({
      userId: alice.user.id,
      eventId: EVENT,
      holdId: HOLD,
      seatIds: [SEAT],
      amount: 2_000_000,
      currency: 'VND',
      status: 'awaiting_payment',
      paymentDeadline: '2026-10-15T10:10:00.000Z',
      failureReason: null,
    });
    const [call] = order.calls;
    // user_id lấy từ JWT; client chỉ gửi holdId, không gửi giá
    expect(call.request).toEqual({
      userId: alice.user.id,
      holdId: HOLD,
      idempotencyKey: 'order-key-1',
    });
    expect(call.metadata).toMatchObject({
      'x-request-id': 'req-order',
      'user-id': alice.user.id,
    });
  });

  it('requires authentication, an Idempotency-Key and a valid holdId', async () => {
    await http().post('/orders').send({ holdId: HOLD }).expect(401);
    await createReq(alice.accessToken, null).send({ holdId: HOLD }).expect(400);
    await createReq(alice.accessToken).send({ holdId: 'nope' }).expect(400);
    // whitelist + forbidNonWhitelisted: client không nhét được amount vào body
    await createReq(alice.accessToken).send({ holdId: HOLD, amount: 1 }).expect(400);
    expect(order.calls).toHaveLength(0);
  });

  it.each([
    [status.NOT_FOUND, 'hold not found', 404],
    [status.PERMISSION_DENIED, 'hold belongs to another user', 403],
    [status.ALREADY_EXISTS, 'hold already has an order', 409],
    [status.FAILED_PRECONDITION, 'hold is no longer active', 409],
    [status.INVALID_ARGUMENT, 'invalid argument: idempotency key was used with a different request', 400],
  ])('maps gRPC %s to HTTP %d with the order message', async (code, details, httpStatus) => {
    order.handlers.CreateOrder = () => {
      throw grpcError(code, details);
    };
    const res = await createReq(alice.accessToken).send({ holdId: HOLD }).expect(httpStatus);
    expect(res.body.message).toBe(details);
  });

  it('returns 503 when order reports an upstream outage', async () => {
    order.handlers.CreateOrder = () => {
      throw grpcError(status.UNAVAILABLE, 'upstream unavailable');
    };
    await createReq(alice.accessToken).send({ holdId: HOLD }).expect(503);
  });

  it('lets only the owner (or an admin) read an order', async () => {
    const o = pbOrder(alice.user.id, { status: 4 }); // CONFIRMED
    order.handlers.GetOrder = () => ({ order: o });
    const res = await http().get(`/orders/${o.id}`).auth(alice.accessToken, { type: 'bearer' }).expect(200);
    expect(res.body.status).toBe('confirmed');
    await http().get(`/orders/${o.id}`).auth(bob.accessToken, { type: 'bearer' }).expect(403);
    await http().get(`/orders/${o.id}`).auth(adminToken, { type: 'bearer' }).expect(200);
  });

  it('maps NOT_FOUND to 404 and rejects malformed ids', async () => {
    order.handlers.GetOrder = () => {
      throw grpcError(status.NOT_FOUND, 'order not found');
    };
    await http().get(`/orders/${randomUUID()}`).auth(alice.accessToken, { type: 'bearer' }).expect(404);
    await http().get('/orders/123').auth(alice.accessToken, { type: 'bearer' }).expect(400);
  });
});

describe('Order rate limit (e2e)', () => {
  let app: NestExpressApplication;
  let order: Awaited<ReturnType<typeof startFakeOrder>>;

  beforeAll(async () => {
    await flushRedis();
    order = await startFakeOrder();
    order.handlers.CreateOrder = () => {
      throw grpcError(status.ALREADY_EXISTS, 'hold already has an order');
    };
    app = await createApp({ ORDER_GRPC_URL: order.url, RATE_LIMIT_ORDER: '2' });
  });

  afterAll(async () => {
    await app.close();
    await order.stop();
  });

  it('applies the order limit per user', async () => {
    const [a, b] = [await registerUser(app), await registerUser(app)];
    const create = (token: string) =>
      request(app.getHttpServer())
        .post('/orders')
        .auth(token, { type: 'bearer' })
        .set('idempotency-key', randomUUID())
        .send({ holdId: HOLD });
    await create(a.accessToken).expect(409);
    await create(a.accessToken).expect(409);
    await create(a.accessToken).expect(429);
    // user khác có bộ đếm riêng
    await create(b.accessToken).expect(409);
  });
});
