# use `crane digest <image>` to get the multi-platform sha256
FROM golang:1.24.4-alpine3.21@sha256:56a23791af0f77c87b049230ead03bd8c3ad41683415ea4595e84ce7eada121a AS spicedb-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 go build -v ./cmd/...

# use `crane digest <image>` to get the multi-platform sha256
FROM golang:1.24.4-alpine3.21@sha256:56a23791af0f77c87b049230ead03bd8c3ad41683415ea4595e84ce7eada121a AS health-probe-builder
WORKDIR /go/src/app
RUN apk update && apk add --no-cache git
RUN git clone https://github.com/authzed/grpc-health-probe.git
WORKDIR /go/src/app/grpc-health-probe
RUN git checkout master
RUN CGO_ENABLED=0 go install -a -tags netgo -ldflags=-w

# use `crane digest <image>` to get the multi-platform sha256
FROM cgr.dev/chainguard/static@sha256:092aad9f6448695b6e20333a8faa93fe3637bcf4e88aa804b8f01545eaf288bd
COPY --from=health-probe-builder /go/bin/grpc-health-probe /bin/grpc_health_probe
COPY --from=spicedb-builder /go/src/app/spicedb /usr/local/bin/spicedb
ENV PATH="$PATH:/usr/local/bin"
EXPOSE 50051
ENTRYPOINT ["spicedb"]
