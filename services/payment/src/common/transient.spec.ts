import { Prisma } from '../generated/prisma/client';
import { isTransient } from './transient';

const known = (code: string) => new Prisma.PrismaClientKnownRequestError('x', { code, clientVersion: '7' });

describe('isTransient', () => {
  it.each([
    ['database unreachable', known('P1001'), true],
    ['pool timeout', known('P2024'), true],
    ['write conflict', known('P2034'), true],
    ['connection refused (wrapped)', new Error('x', { cause: Object.assign(new Error('y'), { code: 'ECONNREFUSED' }) }), true],
    ['pg connection lost', Object.assign(new Error('x'), { code: '08006' }), true],
    ['unique violation', known('P2002'), false],
    ['check violation from pg', Object.assign(new Error('x'), { code: '23514' }), false],
    ['plain bug', new TypeError('undefined is not a function'), false],
  ])('%s => %s', (_, err, want) => {
    expect(isTransient(err)).toBe(want);
  });
});
