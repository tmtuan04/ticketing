# Build context là thư mục gốc repo (cần cả gen/go). Build không dùng go.work: go.mod của
# inventory đã replace gen/go về đường dẫn tương đối.
#
# Multi-stage build: stage "build" có đầy đủ Go toolchain để biên dịch; stage cuối chỉ chứa
# đúng một file binary -> image rất nhỏ (~vài chục MB) và ít bề mặt tấn công.

# ---- Stage 1: biên dịch ----
FROM golang:1.27-alpine AS build
# GOWORK=off: bỏ qua go.work ở gốc repo, chỉ dùng go.mod của service.
# CGO_ENABLED=0: binary thuần Go, link tĩnh, không phụ thuộc thư viện C -> chạy được trên distroless/static.
ENV GOWORK=off CGO_ENABLED=0
WORKDIR /src

# Copy go.mod/go.sum trước và tải dependency riêng một layer: chừng nào dependency không đổi,
# Docker dùng lại cache layer này, không phải tải lại mỗi lần sửa code.
COPY gen/go/go.mod gen/go/go.sum gen/go/
COPY services/inventory/go.mod services/inventory/go.sum services/inventory/
RUN cd services/inventory && go mod download

# Giờ mới copy source code và build.
# -trimpath: bỏ đường dẫn máy build khỏi binary; -ldflags="-s -w": bỏ debug symbol cho nhỏ.
COPY gen/go gen/go
COPY services/inventory services/inventory
RUN cd services/inventory && go build -trimpath -ldflags="-s -w" -o /out/inventory ./cmd/server

# ---- Stage 2: image chạy ----
# distroless/static: không có shell, không package manager, chạy bằng user nonroot.
# Vì không có curl nên healthcheck gọi chính binary: "/inventory healthcheck" (xem main.go).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/inventory /inventory
EXPOSE 50051
ENTRYPOINT ["/inventory"]
