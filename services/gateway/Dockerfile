# Build context là thư mục gốc repo: cần package-lock.json (npm workspaces), gen/ts và proto/.
FROM node:24-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
COPY services/gateway/package.json services/gateway/
RUN npm ci --workspace services/gateway

COPY gen/ts gen/ts
COPY services/gateway services/gateway
WORKDIR /app/services/gateway
RUN npx prisma generate && npx nest build

# Image chạy migration một lần (compose: gateway-migrate), dùng prisma CLI có trong devDependencies.
FROM build AS migrate
CMD ["npx", "prisma", "migrate", "deploy"]

FROM node:24-alpine AS runtime
ENV NODE_ENV=production PROTO_DIR=/app/proto
WORKDIR /app
COPY package.json package-lock.json ./
COPY services/gateway/package.json services/gateway/
RUN npm ci --workspace services/gateway --omit=dev && npm cache clean --force
COPY proto proto
COPY --from=build /app/services/gateway/dist services/gateway/dist
WORKDIR /app/services/gateway
USER node
EXPOSE 3000
CMD ["node", "dist/services/gateway/src/main.js"]
