FROM golang:1.18-alpine3.15 AS spicedb-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
COPY . .
RUN go build -v ./cmd/spicedb/

FROM alpine:3.16.0
RUN [ ! -e /etc/nsswitch.conf ] && echo 'hosts: files dns' > /etc/nsswitch.conf
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.6 /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENTRYPOINT ["spicedb"]
