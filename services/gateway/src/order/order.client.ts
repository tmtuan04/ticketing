import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import type { Metadata } from '@grpc/grpc-js';
import type { ClientGrpc } from '@nestjs/microservices';
import { unary } from '../common/grpc-errors';
import { APP_CONFIG, type AppConfig } from '../config';
import {
  ORDER_SERVICE_NAME,
  type CreateOrderRequest,
  type GetOrderRequest,
  type OrderServiceClient,
} from './order.pb';

export const ORDER_CLIENT = 'ORDER_CLIENT';

/** Bọc OrderService client: mỗi call có timeout, trả Promise thay cho Observable. */
@Injectable()
export class OrderClient implements OnModuleInit {
  private svc: OrderServiceClient;

  constructor(
    @Inject(ORDER_CLIENT) private readonly client: ClientGrpc,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {}

  onModuleInit() {
    this.svc = this.client.getService<OrderServiceClient>(ORDER_SERVICE_NAME);
  }

  createOrder(req: CreateOrderRequest, md: Metadata) {
    return unary(this.svc.createOrder(req, md), this.config.grpcTimeoutMs);
  }

  getOrder(req: GetOrderRequest, md: Metadata) {
    return unary(this.svc.getOrder(req, md), this.config.grpcTimeoutMs);
  }
}
