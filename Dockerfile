# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
FROM golang:1.27.3-alpine@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd AS spicedb-builder
WORKDIR /go/src/app
# Dependencies come before the source so that editing source does not invalidate
# the downloaded modules. This only pays off on a builder with a persistent cache
# (a Depot builder, or a developer's machine); on a throwaway CI runner it is free
# but does nothing.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# .dockerignore keeps .git out of the context, so there is no repository here to
# stamp a commit from. Saying so explicitly beats letting -buildvcs=auto quietly
# decide, and it means the builder no longer needs git installed.
# https://github.com/odigos-io/go-rtml#about-ldflags-checklinkname0
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 go build -tags memoryprotection -buildvcs=false -v -ldflags=-checklinkname=0 -o spicedb ./cmd/spicedb

# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
FROM cgr.dev/chainguard/static@sha256:9e2306b457ff71ce370043dc68c7b52bb1e6622c1c6c6714edbed57496f3116e
# NOTE: the copy target location differs from Dockerfile.release for historical reasons. It's referenced in
# compose files and elsewhere so we're keeping it the way it is.
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.61 /ko-app/grpc-health-probe /bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENV PATH="$PATH:/usr/local/bin"
EXPOSE 50051
ENTRYPOINT ["spicedb"]
