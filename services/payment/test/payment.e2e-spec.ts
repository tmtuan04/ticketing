import type { NestExpressApplication } from '@nestjs/platform-express';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import request from 'supertest';
import { PrismaService } from '../src/prisma/prisma.service';
import { signWebhook } from '../src/payments/webhook-signature';
import {
  commandProducer,
  createApp,
  eventCollector,
  paymentRequested,
  waitFor,
  WEBHOOK_SECRET,
} from './helpers';

describe('Payment (e2e, Postgres + Redpanda)', () => {
  let app: NestExpressApplication;
  let prisma: PrismaService;
  let commands: Awaited<ReturnType<typeof commandProducer>>;
  let events: Awaited<ReturnType<typeof eventCollector>>;
  const http = () => request(app.getHttpServer());

  /** Gửi payment.requested như order service và chờ intent xuất hiện qua REST. */
  async function requestPayment(overrides: Record<string, unknown> = {}) {
    const p = paymentRequested(overrides);
    await commands.send({
      messageId: randomUUID(),
      type: 'payment.requested',
      key: p.order_id,
      value: JSON.stringify(p),
    });
    const intent = await waitFor(async () => {
      const res = await http().get(`/payments/orders/${p.order_id}`);
      return res.status === 200 ? res.body : undefined;
    });
    return { p, intent };
  }

  const pay = (intentId: string, body: object, key: string | null = randomUUID()) => {
    const r = http().post(`/mock-gateway/pay/${intentId}`).send(body);
    return key ? r.set('idempotency-key', key) : r;
  };

  const intentStatus = (orderId: string, status: string) =>
    waitFor(async () => {
      const res = await http().get(`/payments/orders/${orderId}`);
      return res.body.status === status ? res.body : undefined;
    });

  const eventsFor = (orderId: string) => events.received.filter((e) => e.key === orderId);

  beforeAll(async () => {
    [commands, events] = await Promise.all([commandProducer(), eventCollector()]);
    app = await createApp();
    prisma = app.get(PrismaService);
  });

  afterAll(async () => {
    await app?.close();
    await Promise.all([commands?.stop(), events?.stop()]);
  });

  describe('payment.requested consumer', () => {
    it('creates one requires_payment intent per order', async () => {
      const { p, intent } = await requestPayment();
      expect(intent).toMatchObject({
        orderId: p.order_id,
        userId: p.user_id,
        amount: 2_000_000,
        currency: 'VND',
        status: 'requires_payment',
        failureReason: null,
      });
      expect(new Date(intent.paymentDeadline).toISOString()).toBe(p.payment_deadline);
    });

    it('processes a redelivered message once and dedups a re-sent command by order_id', async () => {
      const { p } = await requestPayment();
      const messageId = randomUUID();
      // cùng message_id (relay publish lại) và message_id mới (order gửi lại lệnh) cho cùng đơn
      await commands.send({ messageId, type: 'payment.requested', key: p.order_id, value: JSON.stringify(p) });
      await commands.send({ messageId, type: 'payment.requested', key: p.order_id, value: JSON.stringify(p) });
      await waitFor(() => prisma.inboxMessage.findUnique({ where: { messageId } }));
      // message sau cùng partition (cùng key) nên khi nó xong thì hai message trước cũng đã xong
      const marker = randomUUID();
      await commands.send({ messageId: marker, type: 'payment.requested', key: p.order_id, value: JSON.stringify(p) });
      await waitFor(() => prisma.inboxMessage.findUnique({ where: { messageId: marker } }));

      expect(await prisma.paymentIntent.count({ where: { orderId: p.order_id } })).toBe(1);
      expect(await prisma.inboxMessage.count({ where: { messageId: { in: [messageId, marker] } } })).toBe(2);
    });

    it('skips poison messages without blocking the partition', async () => {
      const orderId = randomUUID();
      await commands.send({ messageId: randomUUID(), type: 'payment.requested', key: orderId, value: '{oops' });
      await commands.send({ type: 'payment.requested', key: orderId, value: '{}' }); // không có message_id
      const unknown = randomUUID();
      await commands.send({ messageId: unknown, type: 'payment.unknown', key: orderId, value: '{}' });
      // các message trên cùng key nên cùng partition: lệnh hợp lệ phía sau vẫn được xử lý
      const { p } = await requestPayment({ order_id: orderId });
      expect(p.order_id).toBe(orderId);
      expect(await prisma.inboxMessage.findUnique({ where: { messageId: unknown } })).not.toBeNull();
    });
  });

  describe('GET /payments/orders/:orderId', () => {
    it('404 for an unknown order, 400 for a malformed id', async () => {
      await http().get(`/payments/orders/${randomUUID()}`).expect(404);
      await http().get('/payments/orders/not-a-uuid').expect(400);
    });
  });

  describe('mock gateway + webhook', () => {
    it('success: intent succeeded, payment.succeeded published via outbox', async () => {
      const { p, intent } = await requestPayment();
      const res = await pay(intent.id, { outcome: 'success', delayMs: 0 }).expect(202);
      expect(res.body).toMatchObject({ intentId: intent.id, outcome: 'success', amount: 2_000_000 });

      const done = await intentStatus(p.order_id, 'succeeded');
      expect(done.failureReason).toBeNull();

      const [evt] = await waitFor(() => (eventsFor(p.order_id).length ? eventsFor(p.order_id) : undefined));
      expect(evt.type).toBe('payment.succeeded');
      expect(evt.messageId).toMatch(/^[0-9a-f-]{36}$/);
      expect(evt.payload).toMatchObject({
        order_id: p.order_id,
        payment_intent_id: intent.id,
        amount: 2_000_000,
        currency: 'VND',
        failure_reason: '',
      });
      const outbox = await prisma.outboxMessage.findUniqueOrThrow({ where: { messageId: evt.messageId } });
      expect(outbox.publishedAt).not.toBeNull();
      const charge = await prisma.mockCharge.findUniqueOrThrow({ where: { intentId: intent.id } });
      expect(charge.deliveredAt).not.toBeNull();
    });

    it('fail: intent failed, payment.failed carries the reason', async () => {
      const { p, intent } = await requestPayment();
      await pay(intent.id, { outcome: 'fail' }).expect(202);
      const done = await intentStatus(p.order_id, 'failed');
      expect(done.failureReason).toBe('card_declined');

      const [evt] = await waitFor(() => (eventsFor(p.order_id).length ? eventsFor(p.order_id) : undefined));
      expect(evt.type).toBe('payment.failed');
      expect(evt.payload.failure_reason).toBe('card_declined');
    });

    it('timeout: no webhook is ever sent', async () => {
      const { p, intent } = await requestPayment();
      await pay(intent.id, { outcome: 'timeout' }).expect(202);
      await sleep(1_000);
      const res = await http().get(`/payments/orders/${p.order_id}`).expect(200);
      expect(res.body.status).toBe('requires_payment');
      expect(eventsFor(p.order_id)).toHaveLength(0);
    });

    it('waits delayMs before calling the webhook', async () => {
      const { p, intent } = await requestPayment();
      const start = Date.now();
      await pay(intent.id, { outcome: 'success', delayMs: 1_500 }).expect(202);
      await sleep(500);
      expect((await http().get(`/payments/orders/${p.order_id}`)).body.status).toBe('requires_payment');
      await intentStatus(p.order_id, 'succeeded');
      expect(Date.now() - start).toBeGreaterThanOrEqual(1_500);
    });

    it('Idempotency-Key: same key replays the charge, another key => 409', async () => {
      const { intent } = await requestPayment();
      const first = await pay(intent.id, { outcome: 'timeout' }, 'pay-key-1').expect(202);
      const again = await pay(intent.id, { outcome: 'timeout' }, 'pay-key-1').expect(202);
      expect(again.body.id).toBe(first.body.id);
      await pay(intent.id, { outcome: 'success' }, 'pay-key-2').expect(409);
    });

    it('concurrent pays for one intent create a single charge', async () => {
      const { intent } = await requestPayment();
      const results = await Promise.all(
        Array.from({ length: 5 }, () => pay(intent.id, { outcome: 'timeout' }, 'same-key')),
      );
      expect(results.map((r) => r.status)).toEqual([202, 202, 202, 202, 202]);
      expect(new Set(results.map((r) => r.body.id)).size).toBe(1);
    });

    it('rejects paying a finalized intent', async () => {
      const { p, intent } = await requestPayment();
      await pay(intent.id, { outcome: 'fail' }, 'k1').expect(202);
      await intentStatus(p.order_id, 'failed');
      // charge đã có => key khác bị 409 trước cả khi xét trạng thái intent
      await pay(intent.id, { outcome: 'success' }, 'k2').expect(409);
    });

    it('validates the request', async () => {
      const { intent } = await requestPayment();
      await pay(intent.id, { outcome: 'success' }, null).expect(400);
      await pay(intent.id, { outcome: 'maybe' }).expect(400);
      await pay(intent.id, { outcome: 'success', delayMs: -1 }).expect(400);
      await pay(intent.id, { outcome: 'success', delayMs: 3_600_000 }).expect(400);
      await pay(intent.id, { outcome: 'success', extra: 1 }).expect(400);
      await pay(randomUUID(), { outcome: 'success' }).expect(404);
    });
  });

  describe('POST /payments/webhook', () => {
    const event = (intentId: string, type = 'charge.succeeded', id = `evt_${randomUUID()}`) =>
      JSON.stringify({ id, type, created: 0, data: { payment_intent_id: intentId, failure_reason: 'x' } });
    const post = (body: string, signature?: string) => {
      const r = http().post('/payments/webhook').set('content-type', 'application/json');
      return (signature ? r.set('x-mock-signature', signature) : r).send(body);
    };

    it('rejects missing, wrong, stale or tampered signatures', async () => {
      const { intent } = await requestPayment();
      const body = event(intent.id);
      await post(body).expect(401);
      await post(body, signWebhook('wrong-secret-0123456789abcdef0123', body)).expect(401);
      await post(body, signWebhook(WEBHOOK_SECRET, body, Math.floor(Date.now() / 1000) - 600)).expect(401);
      await post(body.replace('succeeded', 'failed'), signWebhook(WEBHOOK_SECRET, body)).expect(401);
      expect((await http().get(`/payments/orders/${intent.orderId}`)).body.status).toBe('requires_payment');
    });

    it('400 for a correctly signed but malformed event', async () => {
      const body = JSON.stringify({ id: 'evt_x', type: 'charge.refunded', data: {} });
      await post(body, signWebhook(WEBHOOK_SECRET, body)).expect(400);
    });

    it('processes a duplicated delivery once, and keeps the first result', async () => {
      const { p, intent } = await requestPayment();
      const body = event(intent.id);
      const sig = signWebhook(WEBHOOK_SECRET, body);
      expect((await post(body, sig).expect(200)).body.result).toBe('processed');
      expect((await post(body, sig).expect(200)).body.result).toBe('duplicate');
      // sự kiện khác (id khác) báo failed sau khi đã succeeded: bỏ qua
      const late = event(intent.id, 'charge.failed');
      expect((await post(late, signWebhook(WEBHOOK_SECRET, late)).expect(200)).body.result).toBe('ignored');

      expect((await http().get(`/payments/orders/${p.order_id}`)).body.status).toBe('succeeded');
      expect(await prisma.outboxMessage.count({ where: { aggregateId: p.order_id } })).toBe(1);
    });

    it('404 for an unknown intent (and the event is not recorded)', async () => {
      const id = `evt_${randomUUID()}`;
      const body = event(randomUUID(), 'charge.succeeded', id);
      await post(body, signWebhook(WEBHOOK_SECRET, body)).expect(404);
      expect(await prisma.webhookEvent.findUnique({ where: { providerEventId: id } })).toBeNull();
    });
  });

  it('healthz', async () => {
    await http().get('/healthz').expect(200, { status: 'ok', postgres: 'up' });
  });
});
