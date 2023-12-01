FROM golang:1.21.4-alpine3.18 AS spicedb-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 go build -v ./cmd/...

FROM golang:1.21.4-alpine3.18 AS health-probe-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
RUN git clone https://github.com/grpc-ecosystem/grpc-health-probe.git
WORKDIR /go/src/app/grpc-health-probe
RUN git checkout 46b326771cb9e57af7a495973a180e388b1a516f
RUN CGO_ENABLED=0 go install -a -tags netgo -ldflags=-w

FROM cgr.dev/chainguard/static:latest
#COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.20 /ko-app/grpc-health-probe /usr/local/bin/grpc_health_probe
COPY --from=health-probe-builder /go/bin/grpc-health-probe /bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENV PATH="$PATH:/usr/local/bin"
EXPOSE 50051
ENTRYPOINT ["spicedb"]
