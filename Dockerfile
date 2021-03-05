FROM golang:1.16-alpine3.13 AS build

WORKDIR /go/src/app
COPY . .
RUN go get -d -v ./...
RUN go install -v ./...

FROM alpine:3.13

COPY --from=build /go/bin/spicedb /usr/local/bin/spicedb
CMD ["spicedb"]
