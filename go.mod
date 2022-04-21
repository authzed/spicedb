module github.com/authzed/spicedb

go 1.16

require (
	cloud.google.com/go/spanner v1.30.1
	github.com/Masterminds/squirrel v1.5.2
	github.com/authzed/authzed-go v0.5.1-0.20220303182356-243e79ca06fa
	github.com/authzed/grpcutil v0.0.0-20220104222419-f813f77722e5
	github.com/aws/aws-sdk-go v1.43.31
	github.com/benbjohnson/clock v1.3.0
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/cespare/xxhash v1.1.0
	github.com/cncf/udpa/go v0.0.0-20220112060539-c52dc94e7fbe // indirect
	github.com/cncf/xds/go v0.0.0-20220330162227-eded343319d0 // indirect
	github.com/dalzilio/rudd v1.1.0
	github.com/dave/jennifer v1.5.0 // indirect
	github.com/dgraph-io/ristretto v0.1.0
	github.com/dlmiddlecote/sqlstats v1.0.2
	github.com/docker/docker v20.10.14+incompatible // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/ecordell/optgen v0.0.6
	github.com/emirpasic/gods v1.12.0
	github.com/envoyproxy/go-control-plane v0.10.1 // indirect
	github.com/envoyproxy/protoc-gen-validate v0.6.7
	github.com/fatih/color v1.13.0
	github.com/fatih/structs v1.1.0
	github.com/go-co-op/gocron v1.13.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.5.7
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2 v2.0.0-rc.2.0.20210831071041-dd1540ef8252
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.0.0-rc.2.0.20210831071041-dd1540ef8252
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.10.0
	github.com/hashicorp/go-memdb v1.3.2
	github.com/influxdata/tdigest v0.0.1
	github.com/jackc/pgconn v1.11.0
	github.com/jackc/pgtype v1.10.0
	github.com/jackc/pgx/v4 v4.15.0
	github.com/jmoiron/sqlx v1.3.4
	github.com/johannesboyne/gofakes3 v0.0.0-20220314170512-33c13122505e
	github.com/jwangsadinata/go-multimap v0.0.0-20190620162914-c29f3d7f33b6
	github.com/jzelinskie/cobrautil v0.0.11
	github.com/jzelinskie/stringz v0.0.1
	github.com/lib/pq v1.10.4
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/ngrok/sqlmw v0.0.0-20211220175533-9d16fdc47b31
	github.com/ory/dockertest/v3 v3.8.2-0.20220414165644-e38b9742dc7d
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.33.0
	github.com/rs/cors v1.8.2
	github.com/rs/zerolog v1.26.1
	github.com/scylladb/go-set v1.0.2
	github.com/sercand/kuberesolver/v3 v3.1.0
	github.com/shabbyrobe/gocovmerge v0.0.0-20190829150210-3e036491d500 // indirect
	github.com/shopspring/decimal v1.3.1
	github.com/spf13/cobra v1.4.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.11.0 // indirect
	github.com/stretchr/objx v0.3.0 // indirect
	github.com/stretchr/testify v1.7.1
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	go.buf.build/protocolbuffers/go/prometheus/prometheus v1.2.2
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.31.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.31.0
	go.opentelemetry.io/otel v1.6.3
	go.opentelemetry.io/otel/trace v1.6.3
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/goleak v1.1.12
	golang.org/x/net v0.0.0-20220418201149-a630d4f3e7a2 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/tools v0.1.10
	google.golang.org/api v0.74.0
	google.golang.org/genproto v0.0.0-20220414192740-2d67ff6cf2b4
	google.golang.org/grpc v1.45.0
	google.golang.org/protobuf v1.28.0
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	mvdan.cc/gofumpt v0.3.1
)

// TODO(jschorr): Remove once https://github.com/dgraph-io/ristretto/pull/286 is merged
replace github.com/dgraph-io/ristretto => github.com/josephschorr/ristretto v0.1.1-0.20211227180020-ae4c2c35d79d

// TODO(cjs) waiting for new cobrautil release and it being used in upstream spicedb
replace github.com/jzelinskie/cobrautil => github.com/jzelinskie/cobrautil v0.0.11-0.20220418210929-33f763228c87
