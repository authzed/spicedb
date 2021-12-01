FROM golang:1.17.3-alpine3.13 AS spicedb-builder
WORKDIR /go/src/app

# Prepare dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build ./cmd/spicedb/

FROM alpine:3.15.0

RUN [ ! -e /etc/nsswitch.conf ] && echo 'hosts: files dns' > /etc/nsswitch.conf
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.6 /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENTRYPOINT ["spicedb"]
