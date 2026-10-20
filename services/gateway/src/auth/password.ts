import { randomBytes, scrypt, timingSafeEqual, type ScryptOptions } from 'node:crypto';

// Hash mật khẩu bằng scrypt có sẵn trong Node (không cần native module).
// Tham số lưu cùng hash nên có thể tăng cost sau này mà hash cũ vẫn verify được.
// Định dạng: scrypt$N$r$p$<salt base64>$<hash base64>

const PARAMS = { N: 2 ** 15, r: 8, p: 1 };
const KEY_LEN = 64;
const SALT_LEN = 16;

function derive(password: string, salt: Buffer, opts: ScryptOptions): Promise<Buffer> {
  // maxmem phải > 128 * N * r (32 MiB với tham số mặc định)
  const maxmem = 256 * opts.N! * opts.r!;
  return new Promise((resolve, reject) =>
    scrypt(password, salt, KEY_LEN, { ...opts, maxmem }, (err, key) => (err ? reject(err) : resolve(key))),
  );
}

export async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(SALT_LEN);
  const key = await derive(password, salt, PARAMS);
  const { N, r, p } = PARAMS;
  return `scrypt$${N}$${r}$${p}$${salt.toString('base64')}$${key.toString('base64')}`;
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const parts = stored.split('$');
  if (parts.length !== 6 || parts[0] !== 'scrypt') return false;
  const [N, r, p] = parts.slice(1, 4).map(Number);
  const salt = Buffer.from(parts[4], 'base64');
  const expected = Buffer.from(parts[5], 'base64');
  const key = await derive(password, salt, { N, r, p });
  return key.length === expected.length && timingSafeEqual(key, expected);
}
