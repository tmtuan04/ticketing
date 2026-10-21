import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import type { Metadata } from '@grpc/grpc-js';
import type { ClientGrpc } from '@nestjs/microservices';
import { unary } from '../common/rpc';
import { APP_CONFIG, type AppConfig } from '../config';
import {
  INVENTORY_SERVICE_NAME,
  type InitEventInventoryRequest,
  type InventoryServiceClient,
} from './inventory.pb';

export const INVENTORY_CLIENT = 'INVENTORY_CLIENT';

/** Catalog chỉ cần InitEventInventory (lúc publish). */
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

  initEventInventory(req: InitEventInventoryRequest, md: Metadata) {
    return unary(this.svc.initEventInventory(req, md), this.config.grpcTimeoutMs);
  }
}
