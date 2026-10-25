import { Module, ValidationPipe, type DynamicModule } from '@nestjs/common';
import { APP_PIPE } from '@nestjs/core';
import { AppConfigModule, type AppConfig } from './config';
import { HealthController } from './health/health.controller';
import { KafkaModule } from './kafka/kafka.module';
import { MockGatewayController } from './mock-gateway/mock-gateway.controller';
import { MockGatewayService } from './mock-gateway/mock-gateway.service';
import { OutboxRelay } from './outbox/outbox.relay';
import { PaymentCommandsConsumer } from './payments/payment-commands.consumer';
import { PaymentsController } from './payments/payments.controller';
import { PaymentsService } from './payments/payments.service';
import { PrismaModule } from './prisma/prisma.service';

@Module({})
export class AppModule {
  static forRoot(config: AppConfig): DynamicModule {
    return {
      module: AppModule,
      imports: [AppConfigModule.forRoot(config), PrismaModule, KafkaModule],
      controllers: [HealthController, PaymentsController, MockGatewayController],
      providers: [
        PaymentsService,
        PaymentCommandsConsumer,
        OutboxRelay,
        MockGatewayService,
        {
          provide: APP_PIPE,
          useValue: new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }),
        },
      ],
    };
  }
}
