# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
# Note: Using Debian-based golang image (not alpine) to build with glibc for postgres-fdw support
FROM golang:1.25.6@sha256:ce63a16e0f7063787ebb4eb28e72d477b00b4726f79874b3205a965ffd797ab2 AS spicedb-builder
WORKDIR /go/src/app
RUN apt-get update && apt-get install -y --no-install-recommends git gcc libc6-dev && rm -rf /var/lib/apt/lists/*
COPY . .
# https://github.com/odigos-io/go-rtml#about-ldflags-checklinkname0
# Build with CGO enabled for postgres-fdw support
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=1 go build -tags memoryprotection -v -ldflags=-checklinkname=0 -o spicedb ./cmd/spicedb

# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
FROM golang:1.25.6-alpine@sha256:660f0b83cf50091e3777e4730ccc0e63e83fea2c420c872af5c60cb357dcafb2 AS health-probe-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
RUN git clone https://github.com/authzed/grpc-health-probe.git
WORKDIR /go/src/app/grpc-health-probe
RUN git checkout main
RUN CGO_ENABLED=0 go install -a -tags netgo -ldflags=-w

# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
# Note: Using glibc-dynamic base instead of static because the postgres-fdw command requires libc
FROM cgr.dev/chainguard/glibc-dynamic@sha256:3f5dc064fa077619b186d21a426c07fb3868668ffe104db8ba3e46347a80a1d3
COPY --from=health-probe-builder /go/bin/grpc-health-probe /bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENV PATH="$PATH:/usr/local/bin"
EXPOSE 50051
ENTRYPOINT ["spicedb"]
