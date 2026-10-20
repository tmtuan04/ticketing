import type { Request } from 'express';

export type Role = 'user' | 'admin';

export interface AuthUser {
  id: string;
  role: Role;
}

export type AppRequest = Request & {
  /** gán bởi middleware requestId */
  id: string;
  /** gán bởi JwtAuthGuard */
  user?: AuthUser;
};
