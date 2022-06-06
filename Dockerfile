FROM golang:1.18-alpine3.15 AS spicedb-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
COPY . ./
#Spice build
RUN go build -v ./cmd/spicedb/

FROM alpine:3.15
RUN [ ! -e /etc/nsswitch.conf ] && echo 'hosts: files dns' > /etc/nsswitch.conf
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.6 /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENTRYPOINT ["spicedb", "serve", "--grpc-preshared-key", "'realkeyhere'", "--datastore-engine=postgres", "--datastore-conn-uri='postgres://new:Happy456@/cloudsql/cog-analytics-backend:us-central1:authz-store/postgres?sslmode=disable'"]
