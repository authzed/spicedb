FROM golang:1.17.1-alpine3.13 AS build

ARG GRPC_HEALTH_PROBE_VERSION=0.3.6
RUN apk add curl
RUN curl -Lo /go/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64
RUN chmod +x /go/bin/grpc_health_probe

WORKDIR /go/src/app

# Prepare dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build ./cmd/spicedb/

FROM alpine:3.14.2

RUN [ ! -e /etc/nsswitch.conf ] && echo 'hosts: files dns' > /etc/nsswitch.conf
COPY --from=build /go/bin/grpc_health_probe /usr/local/bin/
COPY --from=build /go/src/app/spicedb /usr/local/bin/spicedb
ENTRYPOINT ["spicedb"]
