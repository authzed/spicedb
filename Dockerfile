# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
# Note: Using Debian-based golang image (not alpine) to build with glibc for postgres-fdw support
FROM golang:1.25.5@sha256:8bbd14091f2c61916134fa6aeb8f76b18693fcb29a39ec6d8be9242c0a7e9260 AS spicedb-builder
WORKDIR /go/src/app
RUN apt-get update && apt-get install -y --no-install-recommends git gcc libc6-dev && rm -rf /var/lib/apt/lists/*
COPY . .
# https://github.com/odigos-io/go-rtml#about-ldflags-checklinkname0
# Build with CGO enabled for postgres-fdw support
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=1 go build -tags memoryprotection -v -ldflags=-checklinkname=0 -o spicedb ./cmd/spicedb

# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
FROM golang:1.25.5-alpine@sha256:ac09a5f469f307e5da71e766b0bd59c9c49ea460a528cc3e6686513d64a6f1fb AS health-probe-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
RUN git clone https://github.com/authzed/grpc-health-probe.git
WORKDIR /go/src/app/grpc-health-probe
RUN git checkout main
RUN CGO_ENABLED=0 go install -a -tags netgo -ldflags=-w

# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
# Note: Using glibc-dynamic base instead of static because the postgres-fdw command requires libc
FROM cgr.dev/chainguard/glibc-dynamic@sha256:530fc40b687b95f6c5e8a9b62da03306754da5ef45178e632b7486603bfb7096
COPY --from=health-probe-builder /go/bin/grpc-health-probe /bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENV PATH="$PATH:/usr/local/bin"
EXPOSE 50051
ENTRYPOINT ["spicedb"]
