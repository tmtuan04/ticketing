# Build context là thư mục web/ (không thuộc npm workspaces của các service).
FROM node:24-alpine AS build
ENV npm_config_fetch_retries=5 npm_config_fetch_retry_mintimeout=20000 npm_config_fetch_retry_maxtimeout=120000
WORKDIR /app
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci
COPY . .
RUN npm run build

# nginx phục vụ file tĩnh và chuyển tiếp /api -> gateway, /pay -> payment (cùng origin, không cần CORS).
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
