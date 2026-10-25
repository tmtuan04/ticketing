# Build context là thư mục gốc repo: cần package-lock.json (npm workspaces). Payment không dùng gRPC
# nên không cần gen/ts và proto/ như gateway/catalog.
FROM node:24-alpine AS build
# Mạng chập chờn khi build: cache npm dùng lại giữa các lần build (không nằm trong image) + retry nhiều hơn
ENV npm_config_fetch_retries=5 npm_config_fetch_retry_mintimeout=20000 npm_config_fetch_retry_maxtimeout=120000
WORKDIR /app
COPY package.json package-lock.json ./
COPY services/payment/package.json services/payment/
RUN --mount=type=cache,target=/root/.npm npm ci --workspace services/payment

COPY services/payment services/payment
WORKDIR /app/services/payment
RUN npx prisma generate && npx nest build

# Image chạy migration một lần (compose: payment-migrate), dùng prisma CLI có trong devDependencies.
FROM build AS migrate
CMD ["npx", "prisma", "migrate", "deploy"]

FROM node:24-alpine AS runtime
ENV NODE_ENV=production \
    npm_config_fetch_retries=5 npm_config_fetch_retry_mintimeout=20000 npm_config_fetch_retry_maxtimeout=120000
WORKDIR /app
COPY package.json package-lock.json ./
COPY services/payment/package.json services/payment/
RUN --mount=type=cache,target=/root/.npm npm ci --workspace services/payment --omit=dev
COPY --from=build /app/services/payment/dist services/payment/dist
WORKDIR /app/services/payment
USER node
EXPOSE 3001
CMD ["node", "dist/src/main.js"]
