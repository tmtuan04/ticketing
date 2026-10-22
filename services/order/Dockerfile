# Build context là thư mục gốc repo (cần cả gen/go). Cùng cách build như inventory/Dockerfile
# (xem chú thích bên đó): multi-stage, binary tĩnh, image distroless.
FROM golang:1.27-alpine AS build
ENV GOWORK=off CGO_ENABLED=0
WORKDIR /src

COPY gen/go/go.mod gen/go/go.sum gen/go/
COPY services/order/go.mod services/order/go.sum services/order/
RUN cd services/order && go mod download

COPY gen/go gen/go
COPY services/order services/order
RUN cd services/order && go build -trimpath -ldflags="-s -w" -o /out/order ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/order /order
EXPOSE 50053
ENTRYPOINT ["/order"]
