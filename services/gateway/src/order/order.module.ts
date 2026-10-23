import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { APP_CONFIG, type AppConfig } from '../config';
import { ORDER_CLIENT, OrderClient } from './order.client';
import { TICKETING_ORDER_V1_PACKAGE_NAME } from './order.pb';
import { OrdersController } from './orders.controller';

@Module({
  imports: [
    ClientsModule.registerAsync([
      {
        name: ORDER_CLIENT,
        inject: [APP_CONFIG],
        // Cùng cấu hình loader như InventoryModule (xem chú thích bên đó).
        useFactory: (config: AppConfig) => ({
          transport: Transport.GRPC,
          options: {
            package: TICKETING_ORDER_V1_PACKAGE_NAME,
            protoPath: 'ticketing/order/v1/order.proto',
            url: config.orderGrpcUrl,
            loader: {
              includeDirs: [config.protoDir],
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
  controllers: [OrdersController],
  providers: [OrderClient],
})
export class OrderModule {}
