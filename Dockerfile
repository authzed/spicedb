# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
FROM golang:1.26.4-alpine@sha256:f23e8b227fb4493eabe03bede4d5a32d04092da71962f1fb79b5f7d1e6c2a17f AS spicedb-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
COPY . .
# https://github.com/odigos-io/go-rtml#about-ldflags-checklinkname0
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 go build -tags memoryprotection -v -ldflags=-checklinkname=0 -o spicedb ./cmd/spicedb

# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
FROM cgr.dev/chainguard/static@sha256:77d8b8925dc27970ec2f48243f44c7a260d52c49cd778288e4ee97566e0cb75b
# NOTE: the copy target location differs from Dockerfile.release for historical reasons. It's referenced in
# compose files and elsewhere so we're keeping it the way it is.
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.49 /ko-app/grpc-health-probe /bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENV PATH="$PATH:/usr/local/bin"
EXPOSE 50051
ENTRYPOINT ["spicedb"]
