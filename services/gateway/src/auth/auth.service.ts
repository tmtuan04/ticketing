import {
  ConflictException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
  UnauthorizedException,
  type OnApplicationBootstrap,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import type { Role } from '../common/request';
import { APP_CONFIG, type AppConfig } from '../config';
import { Prisma, type User } from '../generated/prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import type { LoginDto, RegisterDto } from './dto';
import { hashPassword, verifyPassword } from './password';

export const JWT_ISSUER = 'ticketing-gateway';
export const JWT_AUDIENCE = 'ticketing';

export interface AccessTokenPayload {
  sub: string;
  role: Role;
}

export interface TokenPair {
  accessToken: string;
  refreshToken: string;
  tokenType: 'Bearer';
  /** thời hạn access token, tính bằng giây */
  expiresIn: number;
}

export interface UserView {
  id: string;
  email: string;
  role: Role;
  createdAt: Date;
}

export type AuthResult = TokenPair & { user: UserView };

const INVALID_REFRESH = 'invalid refresh token';

/** Token đã bị request khác dùng trước (refresh song song hoặc bị đánh cắp). */
class RefreshTokenReused extends Error {}

@Injectable()
export class AuthService implements OnApplicationBootstrap {
  private readonly log = new Logger(AuthService.name);
  private dummyHash?: Promise<string>;

  constructor(
    private readonly prisma: PrismaService,
    private readonly jwt: JwtService,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {}

  async onApplicationBootstrap() {
    if (this.config.admin) await this.ensureAdmin(this.config.admin.email, this.config.admin.password);
  }

  async register(dto: RegisterDto): Promise<AuthResult> {
    const passwordHash = await hashPassword(dto.password);
    let user: User;
    try {
      user = await this.prisma.user.create({ data: { email: dto.email, passwordHash } });
    } catch (err) {
      if (isUniqueViolation(err)) throw new ConflictException('email is already registered');
      throw err;
    }
    return { ...(await this.issueTokens(user)), user: toView(user) };
  }

  async login(dto: LoginDto): Promise<AuthResult> {
    const user = await this.prisma.user.findUnique({ where: { email: dto.email } });
    // Email không tồn tại vẫn chạy scrypt với hash giả để thời gian phản hồi không lộ email nào đã đăng ký.
    const ok = await verifyPassword(dto.password, user?.passwordHash ?? (await this.getDummyHash()));
    if (!user || !ok) throw new UnauthorizedException('invalid email or password');
    return { ...(await this.issueTokens(user)), user: toView(user) };
  }

  /**
   * Xoay vòng refresh token. Token đã thu hồi mà bị dùng lại thì coi như bị lộ:
   * thu hồi cả family, buộc đăng nhập lại trên mọi thiết bị dùng family đó.
   */
  async refresh(token: string): Promise<TokenPair> {
    const record = await this.prisma.refreshToken.findUnique({
      where: { tokenHash: sha256(token) },
      include: { user: true },
    });
    if (!record) throw new UnauthorizedException(INVALID_REFRESH);
    if (record.revokedAt) {
      await this.revokeReusedFamily(record.familyId, record.userId);
      throw new UnauthorizedException(INVALID_REFRESH);
    }
    if (record.expiresAt <= new Date()) throw new UnauthorizedException(INVALID_REFRESH);

    let refreshToken: string;
    try {
      refreshToken = await this.prisma.$transaction(async (tx) => {
        // Điều kiện revokedAt = null: trong hai request song song chỉ một request thu hồi được token cũ.
        const { count } = await tx.refreshToken.updateMany({
          where: { id: record.id, revokedAt: null },
          data: { revokedAt: new Date() },
        });
        if (count === 0) throw new RefreshTokenReused();
        return this.createRefreshToken(tx, record.userId, record.familyId);
      });
    } catch (err) {
      if (!(err instanceof RefreshTokenReused)) throw err;
      await this.revokeReusedFamily(record.familyId, record.userId);
      throw new UnauthorizedException(INVALID_REFRESH);
    }
    return this.tokenPair(record.user, refreshToken);
  }

  /** Thu hồi family chứa token; token không hợp lệ thì bỏ qua (logout luôn thành công). */
  async logout(token: string): Promise<void> {
    const record = await this.prisma.refreshToken.findUnique({ where: { tokenHash: sha256(token) } });
    if (record) await this.revokeFamily(record.familyId);
  }

  async me(userId: string): Promise<UserView> {
    const user = await this.prisma.user.findUnique({ where: { id: userId } });
    if (!user) throw new NotFoundException('user not found');
    return toView(user);
  }

  private async issueTokens(user: User): Promise<TokenPair> {
    const token = await this.createRefreshToken(this.prisma, user.id, randomUUID());
    return this.tokenPair(user, token);
  }

  private async tokenPair(user: User, refreshToken: string): Promise<TokenPair> {
    const payload: AccessTokenPayload = { sub: user.id, role: user.role };
    const accessToken = await this.jwt.signAsync(payload, {
      expiresIn: this.config.jwt.accessTtlSec,
      issuer: JWT_ISSUER,
      audience: JWT_AUDIENCE,
    });
    return { accessToken, refreshToken, tokenType: 'Bearer', expiresIn: this.config.jwt.accessTtlSec };
  }

  /** Trả về token gốc; DB chỉ lưu sha256. */
  private async createRefreshToken(db: Prisma.TransactionClient, userId: string, familyId: string) {
    const token = randomBytes(32).toString('base64url');
    await db.refreshToken.create({
      data: {
        userId,
        familyId,
        tokenHash: sha256(token),
        expiresAt: new Date(Date.now() + this.config.jwt.refreshTtlSec * 1000),
      },
    });
    return token;
  }

  private async revokeReusedFamily(familyId: string, userId: string) {
    this.log.warn(`refresh token reuse detected, revoking family ${familyId} of user ${userId}`);
    await this.revokeFamily(familyId);
  }

  private async revokeFamily(familyId: string) {
    await this.prisma.refreshToken.updateMany({
      where: { familyId, revokedAt: null },
      data: { revokedAt: new Date() },
    });
  }

  private getDummyHash(): Promise<string> {
    this.dummyHash ??= hashPassword(randomBytes(16).toString('hex'));
    return this.dummyHash;
  }

  /** Tạo tài khoản admin từ ADMIN_EMAIL/ADMIN_PASSWORD; đã có thì chỉ đảm bảo role admin. */
  private async ensureAdmin(rawEmail: string, password: string) {
    const email = rawEmail.trim().toLowerCase();
    const existing = await this.prisma.user.findUnique({ where: { email } });
    if (existing) {
      if (existing.role !== 'admin') {
        await this.prisma.user.update({ where: { id: existing.id }, data: { role: 'admin' } });
      }
      return;
    }
    try {
      await this.prisma.user.create({
        data: { email, passwordHash: await hashPassword(password), role: 'admin' },
      });
      this.log.log(`created admin account ${email}`);
    } catch (err) {
      // replica khác vừa tạo xong
      if (!isUniqueViolation(err)) throw err;
    }
  }
}

function sha256(token: string): string {
  return createHash('sha256').update(token).digest('hex');
}

function toView(user: User): UserView {
  return { id: user.id, email: user.email, role: user.role, createdAt: user.createdAt };
}

function isUniqueViolation(err: unknown): boolean {
  return err instanceof Prisma.PrismaClientKnownRequestError && err.code === 'P2002';
}
