import { Transport, type GrpcOptions } from '@nestjs/microservices';
import type { HealthImplementation } from 'grpc-health-check';
import { TICKETING_CATALOG_V1_PACKAGE_NAME } from './catalog/catalog.pb';
import type { AppConfig } from './config';

// khớp với kiểu ts-proto sinh ra: field camelCase, int64 dạng string,
// field rỗng vẫn có giá trị mặc định (repeated => [])
export const PROTO_LOADER_OPTIONS = { keepCase: false, longs: String, defaults: true, arrays: true };

/** gRPC server của catalog (dùng chung cho main.ts và e2e test), kèm grpc.health.v1. */
export function grpcServerOptions(config: AppConfig, health: HealthImplementation, url?: string): GrpcOptions {
  return {
    transport: Transport.GRPC,
    options: {
      package: TICKETING_CATALOG_V1_PACKAGE_NAME,
      // protoPath tương đối với includeDirs
      protoPath: 'ticketing/catalog/v1/catalog.proto',
      url: url ?? `0.0.0.0:${config.grpcPort}`,
      loader: { includeDirs: [config.protoDir], ...PROTO_LOADER_OPTIONS },
      onLoadPackageDefinition: (_pkg, server) => health.addToServer(server),
    },
  };
}
