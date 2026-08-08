# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
FROM golang:1.26.6-alpine@sha256:3889b425f035be855a72fb4755265311293b6d414521f0a519d819df32222d83 AS spicedb-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
COPY . .
# https://github.com/odigos-io/go-rtml#about-ldflags-checklinkname0
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 go build -tags memoryprotection -v -ldflags=-checklinkname=0 -o spicedb ./cmd/spicedb

# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
FROM cgr.dev/chainguard/static@sha256:f68e3a8244c7d0f4cd56635aaff8e6a533cf6cc3850d8fb339567a5782d6a0b0
# NOTE: the copy target location differs from Dockerfile.release for historical reasons. It's referenced in
# compose files and elsewhere so we're keeping it the way it is.
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.55 /ko-app/grpc-health-probe /bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENV PATH="$PATH:/usr/local/bin"
EXPOSE 50051
ENTRYPOINT ["spicedb"]
