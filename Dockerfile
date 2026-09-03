# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
FROM golang:1.27.0-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS spicedb-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
COPY . .
# https://github.com/odigos-io/go-rtml#about-ldflags-checklinkname0
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 go build -tags memoryprotection -v -ldflags=-checklinkname=0 -o spicedb ./cmd/spicedb

# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
FROM cgr.dev/chainguard/static@sha256:96d02f455d5a73b817c0602910748609cf8471b1cc9522f78c75cedb1f67d072
# NOTE: the copy target location differs from Dockerfile.release for historical reasons. It's referenced in
# compose files and elsewhere so we're keeping it the way it is.
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.55 /ko-app/grpc-health-probe /bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENV PATH="$PATH:/usr/local/bin"
EXPOSE 50051
ENTRYPOINT ["spicedb"]
