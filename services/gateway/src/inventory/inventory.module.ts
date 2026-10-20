import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { APP_CONFIG, type AppConfig } from '../config';
import { HoldsController } from './holds.controller';
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
            // protoPath tương đối với includeDirs
            protoPath: 'ticketing/inventory/v1/inventory.proto',
            url: config.inventoryGrpcUrl,
            loader: {
              includeDirs: [config.protoDir],
              // khớp với kiểu ts-proto sinh ra: field camelCase, int64 dạng string,
              // field rỗng vẫn có giá trị mặc định (repeated => [])
              keepCase: false,
              longs: String,
              defaults: true,
              arrays: true,
            },
          },
        }),
      },
    ]),
  ],
  controllers: [HoldsController],
  providers: [InventoryClient],
})
export class InventoryModule {}
