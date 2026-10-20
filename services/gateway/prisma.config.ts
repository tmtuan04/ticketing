import { defineConfig } from 'prisma/config';

export default defineConfig({
  schema: 'prisma/schema.prisma',
  migrations: { path: 'prisma/migrations' },
  datasource: {
    // Mặc định trỏ vào Postgres của docker compose (port 5440), giống inventory.
    url:
      process.env.DATABASE_URL ??
      'postgresql://app:app@localhost:5440/auth_db?sslmode=disable',
  },
});
