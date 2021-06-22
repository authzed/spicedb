FROM golang:1.16-alpine3.13 AS build

WORKDIR /go/src/app

# Prepare dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go get -d -v ./...
RUN go install -v ./...

FROM alpine:3.13

COPY ./contrib/grpc_health_probe-linux-amd64 /usr/local/bin
COPY --from=build /go/bin/spicedb /usr/local/bin/spicedb
COPY --from=build /go/bin/zed-testserver /usr/local/bin/zed-testserver
CMD ["spicedb"]
