# Build context là thư mục gốc repo (cần cả gen/go). Build không dùng go.work: go.mod của
# inventory đã replace gen/go về đường dẫn tương đối.
FROM golang:1.27-alpine AS build
ENV GOWORK=off CGO_ENABLED=0
WORKDIR /src

COPY gen/go/go.mod gen/go/go.sum gen/go/
COPY services/inventory/go.mod services/inventory/go.sum services/inventory/
RUN cd services/inventory && go mod download

COPY gen/go gen/go
COPY services/inventory services/inventory
RUN cd services/inventory && go build -trimpath -ldflags="-s -w" -o /out/inventory ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/inventory /inventory
EXPOSE 50051
ENTRYPOINT ["/inventory"]
