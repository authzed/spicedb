# use `docker buildx imagetools inspect <image>` to get the multi-platform sha256
FROM golang:1.27.1-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS spicedb-builder
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
FROM cgr.dev/chainguard/static@sha256:41e17ed83c594a64a9396b6ab96dd26d5ddc290dacf4c177464712ff21ad534f
# NOTE: the copy target location differs from Dockerfile.release for historical reasons. It's referenced in
# compose files and elsewhere so we're keeping it the way it is.
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.59 /ko-app/grpc-health-probe /bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENV PATH="$PATH:/usr/local/bin"
EXPOSE 50051
ENTRYPOINT ["spicedb"]
