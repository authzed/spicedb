module github.com/authzed/spicedb

go 1.16

require (
	github.com/Masterminds/squirrel v1.5.0
	github.com/alecthomas/units v0.0.0-20210208195552-ff826a37aa15
	github.com/authzed/authzed-go v0.1.1-0.20210923172306-b4b512e4d359
	github.com/authzed/grpcutil v0.0.0-20210914195113-c0d8369e7e1f
	github.com/aws/aws-sdk-go v1.40.53
	github.com/benbjohnson/clock v1.1.0
	github.com/cespare/xxhash v1.1.0
	github.com/containerd/continuity v0.2.1 // indirect
	github.com/dgraph-io/ristretto v0.1.0
	github.com/docker/cli v20.10.9+incompatible // indirect
	github.com/docker/docker v20.10.9+incompatible // indirect
	github.com/emirpasic/gods v1.12.0
	github.com/envoyproxy/protoc-gen-validate v0.6.1
	github.com/fatih/color v1.13.0
	github.com/gogo/protobuf v1.3.2
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2 v2.0.0-rc.2
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.0.0-rc.2
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/go-memdb v1.3.2
	github.com/influxdata/tdigest v0.0.1
	github.com/jackc/pgconn v1.10.0
	github.com/jackc/pgtype v1.8.1
	github.com/jackc/pgx/v4 v4.13.0
	github.com/jmoiron/sqlx v1.3.4
	github.com/johannesboyne/gofakes3 v0.0.0-20210608054100-92d5d4af5fde
	github.com/jpillora/backoff v1.0.0
	github.com/jwangsadinata/go-multimap v0.0.0-20190620162914-c29f3d7f33b6
	github.com/jzelinskie/cobrautil v0.0.4
	github.com/jzelinskie/stringz v0.0.1
	github.com/lib/pq v1.10.3
	github.com/mitchellh/mapstructure v1.4.2 // indirect
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/ngrok/sqlmw v0.0.0-20200129213757-d5c93a81bec6
	github.com/ory/dockertest/v3 v3.8.0
	github.com/prometheus/client_golang v1.11.0
	github.com/rs/zerolog v1.25.0
	github.com/scylladb/go-set v1.0.2
	github.com/shopspring/decimal v1.2.0
	github.com/spf13/cobra v1.2.1
	github.com/stretchr/testify v1.7.0
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.24.0
	go.opentelemetry.io/otel v1.0.0
	go.opentelemetry.io/otel/exporters/jaeger v1.0.0 // indirect
	go.opentelemetry.io/otel/trace v1.0.0
	go.uber.org/goleak v1.1.12
	golang.org/x/net v0.0.0-20211015174653-db2dff38ab41 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20211015200801-69063c4bb744 // indirect
	golang.org/x/tools v0.1.6 // indirect
	google.golang.org/genproto v0.0.0-20210909211513-a8c4777a87af
	google.golang.org/grpc v1.41.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
)

// TODO(jschorr): Remove once https://github.com/dgraph-io/ristretto/pull/286 is merged
replace github.com/dgraph-io/ristretto => github.com/josephschorr/ristretto v0.1.1-0.20211008180146-169a43af0a0d
