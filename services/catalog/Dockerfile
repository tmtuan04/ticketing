# Build context là thư mục gốc repo: cần package-lock.json (npm workspaces), gen/ts và proto/.
FROM node:24-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
COPY services/catalog/package.json services/catalog/
RUN npm ci --workspace services/catalog

COPY gen/ts gen/ts
COPY services/catalog services/catalog
WORKDIR /app/services/catalog
RUN npx prisma generate && npx nest build

# Image chạy migration một lần (compose: catalog-migrate), dùng prisma CLI có trong devDependencies.
FROM build AS migrate
CMD ["npx", "prisma", "migrate", "deploy"]

FROM node:24-alpine AS runtime
ENV NODE_ENV=production PROTO_DIR=/app/proto
WORKDIR /app
COPY package.json package-lock.json ./
COPY services/catalog/package.json services/catalog/
RUN npm ci --workspace services/catalog --omit=dev && npm cache clean --force
COPY proto proto
COPY --from=build /app/services/catalog/dist services/catalog/dist
WORKDIR /app/services/catalog
USER node
EXPOSE 50052
CMD ["node", "dist/services/catalog/src/main.js"]
