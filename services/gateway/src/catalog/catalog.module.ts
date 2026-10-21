import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { APP_CONFIG, type AppConfig } from '../config';
import { InventoryModule } from '../inventory/inventory.module';
import { AdminCatalogController } from './admin-catalog.controller';
import { CATALOG_CLIENT, CatalogClient } from './catalog.client';
import { TICKETING_CATALOG_V1_PACKAGE_NAME } from './catalog.pb';
import { EventsController } from './events.controller';

@Module({
  imports: [
    InventoryModule,
    ClientsModule.registerAsync([
      {
        name: CATALOG_CLIENT,
        inject: [APP_CONFIG],
        useFactory: (config: AppConfig) => ({
          transport: Transport.GRPC,
          options: {
            package: TICKETING_CATALOG_V1_PACKAGE_NAME,
            protoPath: 'ticketing/catalog/v1/catalog.proto',
            url: config.catalogGrpcUrl,
            // giống InventoryModule: khớp với kiểu ts-proto sinh ra
            loader: { includeDirs: [config.protoDir], keepCase: false, longs: String, defaults: true, arrays: true },
          },
        }),
      },
    ]),
  ],
  controllers: [EventsController, AdminCatalogController],
  providers: [CatalogClient],
})
export class CatalogModule {}
