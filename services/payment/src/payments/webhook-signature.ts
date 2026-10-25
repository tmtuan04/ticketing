import { createHmac, timingSafeEqual } from 'node:crypto';

// Chữ ký webhook kiểu Stripe: header `t=<unix giây>,v1=<hex HMAC-SHA256(secret, "<t>.<raw body>")>`.
// Ký trên body thô (đúng từng byte cổng gửi), không ký trên JSON đã parse lại vì thứ tự key hay
// khoảng trắng đổi là chữ ký sai. Có timestamp trong phần được ký nên kẻ nghe lén không phát lại
// (replay) được webhook cũ quá `toleranceSec`.

export const SIGNATURE_HEADER = 'x-mock-signature';

const hmac = (secret: string, t: number, body: Buffer | string) =>
  createHmac('sha256', secret).update(`${t}.`).update(body).digest('hex');

export function signWebhook(secret: string, body: Buffer | string, nowSec = Math.floor(Date.now() / 1000)) {
  return `t=${nowSec},v1=${hmac(secret, nowSec, body)}`;
}

export function verifyWebhook(
  secret: string,
  body: Buffer | string,
  header: string | undefined,
  toleranceSec: number,
  nowSec = Math.floor(Date.now() / 1000),
): boolean {
  if (!header) return false;
  const parts = new Map(
    header.split(',').map((kv) => {
      const i = kv.indexOf('=');
      return [kv.slice(0, i).trim(), kv.slice(i + 1).trim()] as const;
    }),
  );
  const t = Number(parts.get('t'));
  const sig = parts.get('v1');
  if (!Number.isInteger(t) || !sig || !/^[0-9a-f]{64}$/.test(sig)) return false;
  if (Math.abs(nowSec - t) > toleranceSec) return false;
  // So sánh thời gian hằng: không để lộ chữ ký đúng qua thời gian phản hồi.
  return timingSafeEqual(Buffer.from(hmac(secret, t, body), 'hex'), Buffer.from(sig, 'hex'));
}
