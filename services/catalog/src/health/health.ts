import { Inject, Injectable, Module, type BeforeApplicationShutdown, type DynamicModule } from '@nestjs/common';
import { HealthImplementation } from 'grpc-health-check';

export const HEALTH = Symbol('HEALTH');

/**
 * grpc.health.v1 đăng ký vào gRPC server qua onLoadPackageDefinition (xem grpc.ts).
 * Khi tắt, chuyển NOT_SERVING trước để healthcheck/probe ngừng dồn request vào instance này.
 */
@Injectable()
class HealthShutdown implements BeforeApplicationShutdown {
  constructor(@Inject(HEALTH) private readonly health: HealthImplementation) {}

  beforeApplicationShutdown() {
    this.health.setStatus('', 'NOT_SERVING');
  }
}

@Module({})
export class HealthModule {
  static forRoot(health: HealthImplementation): DynamicModule {
    return {
      module: HealthModule,
      providers: [{ provide: HEALTH, useValue: health }, HealthShutdown],
    };
  }
}
