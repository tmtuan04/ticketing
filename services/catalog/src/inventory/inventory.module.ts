import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { APP_CONFIG, type AppConfig } from '../config';
import { PROTO_LOADER_OPTIONS } from '../grpc';
import { INVENTORY_CLIENT, InventoryClient } from './inventory.client';
import { TICKETING_INVENTORY_V1_PACKAGE_NAME } from './inventory.pb';

@Module({
  imports: [
    ClientsModule.registerAsync([
      {
        name: INVENTORY_CLIENT,
        inject: [APP_CONFIG],
        useFactory: (config: AppConfig) => ({
          transport: Transport.GRPC,
          options: {
            package: TICKETING_INVENTORY_V1_PACKAGE_NAME,
            protoPath: 'ticketing/inventory/v1/inventory.proto',
            url: config.inventoryGrpcUrl,
            loader: { includeDirs: [config.protoDir], ...PROTO_LOADER_OPTIONS },
          },
        }),
      },
    ]),
  ],
  providers: [InventoryClient],
  exports: [InventoryClient],
})
export class InventoryModule {}

