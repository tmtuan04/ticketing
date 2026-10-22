# Build context là thư mục gốc repo: cần package-lock.json (npm workspaces), gen/ts và proto/.
FROM node:24-alpine AS build
# Mạng chập chờn khi build: cache npm dùng lại giữa các lần build (không nằm trong image) + retry nhiều hơn
ENV npm_config_fetch_retries=5 npm_config_fetch_retry_mintimeout=20000 npm_config_fetch_retry_maxtimeout=120000
WORKDIR /app
COPY package.json package-lock.json ./
COPY services/catalog/package.json services/catalog/
RUN --mount=type=cache,target=/root/.npm npm ci --workspace services/catalog

COPY gen/ts gen/ts
COPY services/catalog services/catalog
WORKDIR /app/services/catalog
RUN npx prisma generate && npx nest build

# Image chạy migration một lần (compose: catalog-migrate), dùng prisma CLI có trong devDependencies.
FROM build AS migrate
CMD ["npx", "prisma", "migrate", "deploy"]

FROM node:24-alpine AS runtime
ENV NODE_ENV=production PROTO_DIR=/app/proto \
    npm_config_fetch_retries=5 npm_config_fetch_retry_mintimeout=20000 npm_config_fetch_retry_maxtimeout=120000
WORKDIR /app
COPY package.json package-lock.json ./
COPY services/catalog/package.json services/catalog/
RUN --mount=type=cache,target=/root/.npm npm ci --workspace services/catalog --omit=dev
COPY proto proto
COPY --from=build /app/services/catalog/dist services/catalog/dist
WORKDIR /app/services/catalog
USER node
EXPOSE 50052
CMD ["node", "dist/services/catalog/src/main.js"]
