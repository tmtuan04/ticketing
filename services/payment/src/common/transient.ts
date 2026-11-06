import { Prisma } from '../generated/prisma/client';

// Mã lỗi Prisma do DB tạm thời không dùng được: không kết nối được / mất kết nối (P1001, P1002,
// P1008, P1017), hết connection trong pool (P2024), xung đột ghi hoặc deadlock (P2034).
const TRANSIENT_PRISMA_CODES = new Set(['P1001', 'P1002', 'P1008', 'P1017', 'P2024', 'P2034']);
const TRANSIENT_NET_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN']);

/**
 * Lỗi do phụ thuộc (Postgres, mạng) tạm thời không dùng được: đáng retry mãi thay vì đẩy DLQ, vì
 * lỗi không nằm ở message. Cùng quy tắc với consumer.Transient bên order (Go).
 */
export function isTransient(err: unknown): boolean {
  if (err instanceof Prisma.PrismaClientInitializationError) return true;
  if (err instanceof Prisma.PrismaClientKnownRequestError && TRANSIENT_PRISMA_CODES.has(err.code)) return true;
  // Lỗi của driver pg đi thẳng qua adapter: mã mạng của Node, hoặc SQLSTATE 08xxx (mất kết nối),
  // 57P01 (server đang tắt), 40001/40P01 (serialization/deadlock).
  for (let e: any = err, depth = 0; e && depth < 5; e = e.cause, depth++) {
    const code = e.code;
    if (typeof code !== 'string') continue;
    if (TRANSIENT_NET_CODES.has(code) || code.startsWith('08') || ['57P01', '40001', '40P01'].includes(code)) {
      return true;
    }
  }
  return false;
}
