import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import type { Metadata } from '@grpc/grpc-js';
import type { ClientGrpc } from '@nestjs/microservices';
import { unary } from '../common/grpc-errors';
import { APP_CONFIG, type AppConfig } from '../config';
import {
  CATALOG_ADMIN_SERVICE_NAME,
  CATALOG_SERVICE_NAME,
  type CatalogAdminServiceClient,
  type CatalogServiceClient,
  type CreateEventRequest,
  type CreateVenueRequest,
  type GenerateSeatsRequest,
  type GetEventRequest,
  type ListEventsRequest,
  type ListSeatsRequest,
  type PublishEventRequest,
} from './catalog.pb';

export const CATALOG_CLIENT = 'CATALOG_CLIENT';

/** Bọc CatalogService + CatalogAdminService: mỗi call có timeout, trả Promise thay cho Observable. */
@Injectable()
export class CatalogClient implements OnModuleInit {
  private svc: CatalogServiceClient;
  private admin: CatalogAdminServiceClient;

  constructor(
    @Inject(CATALOG_CLIENT) private readonly client: ClientGrpc,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {}

  onModuleInit() {
    this.svc = this.client.getService<CatalogServiceClient>(CATALOG_SERVICE_NAME);
    this.admin = this.client.getService<CatalogAdminServiceClient>(CATALOG_ADMIN_SERVICE_NAME);
  }

  getEvent(req: GetEventRequest, md: Metadata) {
    return unary(this.svc.getEvent(req, md), this.config.grpcTimeoutMs);
  }

  listEvents(req: ListEventsRequest, md: Metadata) {
    return unary(this.svc.listEvents(req, md), this.config.grpcTimeoutMs);
  }

  listSeats(req: ListSeatsRequest, md: Metadata) {
    return unary(this.svc.listSeats(req, md), this.config.grpcTimeoutMs);
  }

  createVenue(req: CreateVenueRequest, md: Metadata) {
    return unary(this.admin.createVenue(req, md), this.config.grpcTimeoutMs);
  }

  createEvent(req: CreateEventRequest, md: Metadata) {
    return unary(this.admin.createEvent(req, md), this.config.grpcTimeoutMs);
  }

  generateSeats(req: GenerateSeatsRequest, md: Metadata) {
    return unary(this.admin.generateSeats(req, md), this.config.grpcTimeoutMs);
  }

  /** Publish gọi tiếp sang inventory nên được chờ lâu gấp đôi. */
  publishEvent(req: PublishEventRequest, md: Metadata) {
    return unary(this.admin.publishEvent(req, md), this.config.grpcTimeoutMs * 2);
  }
}
