FROM golang:1.19-alpine3.16 AS spicedb-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
COPY . .
RUN CGO_ENABLED=0 go build -v ./cmd/spicedb/

FROM distroless.dev/static
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.12 /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENTRYPOINT ["spicedb"]
