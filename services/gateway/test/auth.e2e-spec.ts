import { Controller, Get } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import type { NestExpressApplication } from '@nestjs/platform-express';
import request from 'supertest';
import { JWT_AUDIENCE, JWT_ISSUER } from '../src/auth/auth.service';
import { Roles } from '../src/common/decorators';
import { ADMIN, createApp, flushRedis, login, registerUser, uniqueEmail } from './helpers';

@Controller('test-admin')
class AdminOnlyController {
  @Roles('admin')
  @Get()
  ok() {
    return { ok: true };
  }
}

describe('Auth (e2e)', () => {
  let app: NestExpressApplication;
  const http = () => request(app.getHttpServer());

  beforeAll(async () => {
    app = await createApp({}, [AdminOnlyController]);
  });

  afterAll(async () => {
    await app.close();
  });

  describe('register & login', () => {
    it('registers a user and returns a working token pair', async () => {
      const email = uniqueEmail();
      const res = await registerUser(app, email);
      expect(res.user).toMatchObject({ email, role: 'user' });
      expect(res).toMatchObject({ tokenType: 'Bearer', expiresIn: 900 });

      const me = await http().get('/auth/me').auth(res.accessToken, { type: 'bearer' }).expect(200);
      expect(me.body).toMatchObject({ id: res.user.id, email, role: 'user' });
      expect(me.body).not.toHaveProperty('passwordHash');
    });

    it('normalizes email case and rejects duplicates', async () => {
      const email = uniqueEmail();
      await registerUser(app, email);
      await http()
        .post('/auth/register')
        .send({ email: `  ${email.toUpperCase()} `, password: 'password123' })
        .expect(409);
      await login(app, email.toUpperCase(), 'password123');
    });

    it.each([
      [{ email: 'not-an-email', password: 'password123' }],
      [{ email: 'a@b.co', password: 'short' }],
      [{ email: 'a@b.co', password: 'password123', role: 'admin' }],
    ])('rejects invalid registration %j', async (body) => {
      await http().post('/auth/register').send(body).expect(400);
    });

    it('gives the same answer for a wrong password and an unknown email', async () => {
      const { user } = await registerUser(app);
      const wrong = await http().post('/auth/login').send({ email: user.email, password: 'nope-nope' });
      const unknown = await http().post('/auth/login').send({ email: uniqueEmail(), password: 'nope-nope' });
      expect(wrong.status).toBe(401);
      expect(unknown.status).toBe(401);
      expect(wrong.body.message).toEqual(unknown.body.message);
    });
  });

  describe('access token', () => {
    it('rejects missing, malformed and forged tokens', async () => {
      const { user } = await registerUser(app);
      await http().get('/auth/me').expect(401);
      await http().get('/auth/me').set('authorization', 'Basic abc').expect(401);
      await http().get('/auth/me').auth('garbage', { type: 'bearer' }).expect(401);

      // ký bằng secret khác để tự nâng quyền admin
      const forged = new JwtService({ secret: 'attacker-secret-0123456789abcdef0123' }).sign(
        { sub: user.id, role: 'admin' },
        { issuer: JWT_ISSUER, audience: JWT_AUDIENCE },
      );
      await http().get('/auth/me').auth(forged, { type: 'bearer' }).expect(401);
    });
  });

  describe('refresh token rotation', () => {
    it('issues a new pair and invalidates the old refresh token', async () => {
      const first = await registerUser(app);
      const res = await http().post('/auth/refresh').send({ refreshToken: first.refreshToken }).expect(200);
      expect(res.body.refreshToken).not.toEqual(first.refreshToken);
      await http().get('/auth/me').auth(res.body.accessToken, { type: 'bearer' }).expect(200);
    });

    it('revokes the whole family when a rotated token is reused', async () => {
      const first = await registerUser(app);
      const second = await http().post('/auth/refresh').send({ refreshToken: first.refreshToken }).expect(200);

      // kẻ tấn công dùng lại token cũ => cả family bị thu hồi, kể cả token mới của chủ tài khoản
      await http().post('/auth/refresh').send({ refreshToken: first.refreshToken }).expect(401);
      await http().post('/auth/refresh').send({ refreshToken: second.body.refreshToken }).expect(401);
    });

    it('lets only one of two concurrent refreshes win', async () => {
      const { refreshToken } = await registerUser(app);
      const results = await Promise.all(
        [1, 2].map(() => http().post('/auth/refresh').send({ refreshToken })),
      );
      expect(results.map((r) => r.status).sort()).toEqual([200, 401]);
    });

    it('keeps other sessions alive when one family is revoked', async () => {
      const { user, refreshToken: sessionA } = await registerUser(app);
      const sessionB = (await login(app, user.email, 'password123')).refreshToken;
      await http().post('/auth/logout').send({ refreshToken: sessionA }).expect(204);
      await http().post('/auth/refresh').send({ refreshToken: sessionA }).expect(401);
      await http().post('/auth/refresh').send({ refreshToken: sessionB }).expect(200);
    });

    it('treats logout with an unknown token as success', async () => {
      await http().post('/auth/logout').send({ refreshToken: 'does-not-exist' }).expect(204);
    });
  });

  describe('RBAC', () => {
    it('allows admins and forbids regular users', async () => {
      const { accessToken } = await registerUser(app);
      await http().get('/test-admin').auth(accessToken, { type: 'bearer' }).expect(403);

      const admin = await login(app, ADMIN.email, ADMIN.password);
      await http().get('/test-admin').auth(admin.accessToken, { type: 'bearer' }).expect(200, { ok: true });
    });
  });
});

describe('Rate limiting (e2e)', () => {
  let app: NestExpressApplication;
  const http = () => request(app.getHttpServer());

  beforeAll(async () => {
    await flushRedis();
    app = await createApp({ RATE_LIMIT_AUTH: '3', RATE_LIMIT_DEFAULT: '5' });
  });

  afterAll(async () => {
    await app.close();
  });

  it('limits login attempts per IP and returns Retry-After', async () => {
    const statuses: number[] = [];
    let last: request.Response | undefined;
    for (let i = 0; i < 4; i++) {
      last = await http().post('/auth/login').send({ email: uniqueEmail(), password: 'whatever1' });
      statuses.push(last.status);
    }
    expect(statuses).toEqual([401, 401, 401, 429]);
    expect(Number(last!.headers['retry-after'])).toBeGreaterThan(0);
  });

  it('counts authenticated requests per user, not per IP', async () => {
    await flushRedis();
    const alice = await registerUser(app);
    const bob = await registerUser(app);
    for (let i = 0; i < 5; i++) {
      await http().get('/auth/me').auth(alice.accessToken, { type: 'bearer' }).expect(200);
    }
    await http().get('/auth/me').auth(alice.accessToken, { type: 'bearer' }).expect(429);
    await http().get('/auth/me').auth(bob.accessToken, { type: 'bearer' }).expect(200);
  });

  it('does not rate limit the health check', async () => {
    for (let i = 0; i < 8; i++) await http().get('/healthz').expect(200);
  });
});
