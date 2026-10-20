import { hashPassword, verifyPassword } from './password';

describe('password', () => {
  it('verifies the original password', async () => {
    const hash = await hashPassword('correct horse battery');
    expect(hash).toMatch(/^scrypt\$32768\$8\$1\$/);
    await expect(verifyPassword('correct horse battery', hash)).resolves.toBe(true);
  });

  it('rejects a wrong password', async () => {
    const hash = await hashPassword('correct horse battery');
    await expect(verifyPassword('correct horse batterY', hash)).resolves.toBe(false);
  });

  it('salts every hash', async () => {
    expect(await hashPassword('same')).not.toEqual(await hashPassword('same'));
  });

  it('rejects malformed hashes instead of throwing', async () => {
    await expect(verifyPassword('x', 'bcrypt$whatever')).resolves.toBe(false);
    await expect(verifyPassword('x', '')).resolves.toBe(false);
  });
});
