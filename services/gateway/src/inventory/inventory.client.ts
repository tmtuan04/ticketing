import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import type { Metadata } from '@grpc/grpc-js';
import type { ClientGrpc } from '@nestjs/microservices';
import { unary } from '../common/grpc-errors';
import { APP_CONFIG, type AppConfig } from '../config';
import {
  INVENTORY_SERVICE_NAME,
  type GetHoldRequest,
  type HoldSeatsRequest,
  type InventoryServiceClient,
  type ListSeatAvailabilityRequest,
  type ReleaseHoldRequest,
} from './inventory.pb';

export const INVENTORY_CLIENT = 'INVENTORY_CLIENT';

/** Bọc InventoryService client: mỗi call có timeout, trả Promise thay cho Observable. */
@Injectable()
export class InventoryClient implements OnModuleInit {
  private svc: InventoryServiceClient;

  constructor(
    @Inject(INVENTORY_CLIENT) private readonly client: ClientGrpc,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {}

  onModuleInit() {
    this.svc = this.client.getService<InventoryServiceClient>(INVENTORY_SERVICE_NAME);
  }

  holdSeats(req: HoldSeatsRequest, md: Metadata) {
    return unary(this.svc.holdSeats(req, md), this.config.grpcTimeoutMs);
  }

  releaseHold(req: ReleaseHoldRequest, md: Metadata) {
    return unary(this.svc.releaseHold(req, md), this.config.grpcTimeoutMs);
  }

  getHold(req: GetHoldRequest, md: Metadata) {
    return unary(this.svc.getHold(req, md), this.config.grpcTimeoutMs);
  }

  listSeatAvailability(req: ListSeatAvailabilityRequest, md: Metadata) {
    return unary(this.svc.listSeatAvailability(req, md), this.config.grpcTimeoutMs);
  }
}
