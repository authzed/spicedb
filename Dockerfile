FROM golang:1.20-alpine3.16 AS spicedb-builder
ARG APP_VERSION
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
RUN go mod download
ADD . .
RUN CGO_ENABLED=0 go build -ldflags "-s -w -X github.com/jzelinskie/cobrautil/v2.Version=v${APP_VERSION}" -v ./cmd/spicedb/

FROM cgr.dev/chainguard/static:latest
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.12 /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENV PATH="$PATH:/usr/local/bin"
ENTRYPOINT ["spicedb"]
