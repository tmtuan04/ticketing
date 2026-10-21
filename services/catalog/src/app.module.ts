import { Module, type DynamicModule } from '@nestjs/common';
import { APP_FILTER, APP_INTERCEPTOR } from '@nestjs/core';
import type { HealthImplementation } from 'grpc-health-check';
import { CatalogAdminController, CatalogController } from './catalog/catalog.controller';
import { CatalogService } from './catalog/catalog.service';
import { AppRpcExceptionFilter, RpcLoggingInterceptor } from './common/rpc';
import { AppConfigModule, type AppConfig } from './config';
import { HealthModule } from './health/health';
import { InventoryModule } from './inventory/inventory.module';
import { PrismaModule } from './prisma/prisma.service';

@Module({})
export class AppModule {
  static forRoot(config: AppConfig, health: HealthImplementation): DynamicModule {
    return {
      module: AppModule,
      imports: [AppConfigModule.forRoot(config), PrismaModule, InventoryModule, HealthModule.forRoot(health)],
      controllers: [CatalogController, CatalogAdminController],
      providers: [
        CatalogService,
        { provide: APP_INTERCEPTOR, useClass: RpcLoggingInterceptor },
        { provide: APP_FILTER, useClass: AppRpcExceptionFilter },
      ],
    };
  }
}
