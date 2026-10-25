import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Controller('healthz')
export class HealthController {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Chỉ kiểm tra Postgres. Redpanda sập thì relay/consumer tự backoff và chạy lại khi Redpanda lên;
   * restart container cũng không giúp gì nên không đưa vào health.
   */
  @Get()
  async check() {
    try {
      await this.prisma.$queryRaw`SELECT 1`;
    } catch {
      throw new ServiceUnavailableException({ status: 'error', postgres: 'down' });
    }
    return { status: 'ok', postgres: 'up' };
  }
}
