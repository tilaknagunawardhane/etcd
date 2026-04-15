# ──────────────────────────────────────────────────────────────────────────────
#  Stage 1 — Build etcd binary from source
#  Build context MUST be the etcd repo root  (context: ..)
# ──────────────────────────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

ENV GOWORK=off
ENV CGO_ENABLED=0
ENV GOOS=linux

RUN apk add --no-cache git bash

WORKDIR /build

# Copy the entire etcd repo so all replace-target sibling modules exist.
COPY . .

# Remove any Windows-corrupted symlinks (plain-text files whose first 3 bytes
# are "../") that break 'go build' on Linux.
RUN find . -name "*.go" -type f -exec sh -c \
      'head -c 3 "$1" | grep -q "\.\.\/" && rm -f "$1"' _ {} \;

# Note: server/go.mod and other go.mod files declare "go 1.26". 
# The go command will automatically download the 1.26 toolchain if needed
# because GOTOOLCHAIN is not restricted.
RUN cd server && GOTOOLCHAIN=auto go mod download

# Build the etcd server binary.
RUN cd server && GOTOOLCHAIN=auto go build \
      -ldflags="-s -w" \
      -o /usr/local/bin/etcd \
      .

# ──────────────────────────────────────────────────────────────────────────────
#  Stage 2 — Minimal runtime image
# ──────────────────────────────────────────────────────────────────────────────
FROM alpine:3.18

RUN apk add --no-cache ca-certificates curl

COPY --from=builder /usr/local/bin/etcd /usr/local/bin/etcd

EXPOSE 2379 2380

HEALTHCHECK --interval=5s --timeout=3s --start-period=40s --retries=12 \
    CMD curl -sf http://localhost:2379/health || exit 1

ENTRYPOINT ["etcd"]
