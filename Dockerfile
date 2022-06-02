FROM golang:1.18-alpine3.15 AS spicedb-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git && apk add wget && apk add yarn
COPY . .
#Spice build
RUN go build -v ./cmd/spicedb/

FROM alpine:3.15
RUN [ ! -e /etc/nsswitch.conf ] && echo 'hosts: files dns' > /etc/nsswitch.conf
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.6 /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENTRYPOINT ["spicedb"]

# Cloud Sql Proxy
RUN apk update && apk add wget && apk add yarn && add sh
RUN wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy
RUN chmod +x cloud_sql_proxy

COPY . .

RUN yarn

CMD ["sh", "-c", "./cloud_sql_proxy -instances=$CLOUD_SQL_CONNECTION_NAME=cog-analytics-backend:us-central1:authz-store=tcp:5433 & yarn start"]

