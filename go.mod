module github.com/authzed/spicedb

go 1.16

require (
	github.com/Masterminds/squirrel v1.5.2
	github.com/alecthomas/units v0.0.0-20210927113745-59d0afb8317a
	github.com/authzed/authzed-go v0.4.1
	github.com/authzed/grpcutil v0.0.0-20211020204402-aba1876830e6
	github.com/aws/aws-sdk-go v1.42.25
	github.com/benbjohnson/clock v1.3.0
	github.com/cespare/xxhash v1.1.0
	github.com/containerd/continuity v0.2.1 // indirect
	github.com/dave/jennifer v1.4.1 // indirect
	github.com/dgraph-io/ristretto v0.1.0
	github.com/docker/docker v20.10.9+incompatible // indirect
	github.com/ecordell/optgen v0.0.5-0.20211217170453-18cdce036e35
	github.com/emirpasic/gods v1.12.0
	github.com/envoyproxy/protoc-gen-validate v0.6.2
	github.com/fatih/color v1.13.0
	github.com/fatih/structs v1.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2 v2.0.0-rc.2.0.20210831071041-dd1540ef8252
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.0.0-rc.2.0.20210831071041-dd1540ef8252
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.7.2
	github.com/hashicorp/go-memdb v1.3.2
	github.com/influxdata/tdigest v0.0.1
	github.com/jackc/pgconn v1.10.1
	github.com/jackc/pgtype v1.9.1
	github.com/jackc/pgx/v4 v4.14.1
	github.com/jmoiron/sqlx v1.3.4
	github.com/johannesboyne/gofakes3 v0.0.0-20210608054100-92d5d4af5fde
	github.com/jwangsadinata/go-multimap v0.0.0-20190620162914-c29f3d7f33b6
	github.com/jzelinskie/cobrautil v0.0.8-0.20220127174036-203b4541e16c
	github.com/jzelinskie/stringz v0.0.1
	github.com/lib/pq v1.10.4
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/ngrok/sqlmw v0.0.0-20210819213940-241da6c2def4
	github.com/ory/dockertest/v3 v3.8.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rs/zerolog v1.26.1
	github.com/scylladb/go-set v1.0.2
	github.com/sercand/kuberesolver/v3 v3.1.0
	github.com/shopspring/decimal v1.3.1
	github.com/spf13/cobra v1.3.0
	github.com/stretchr/objx v0.3.0 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.28.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.28.0
	go.opentelemetry.io/contrib/propagators/ot v1.3.0
	go.opentelemetry.io/otel v1.3.0
	go.opentelemetry.io/otel/trace v1.3.0
	go.uber.org/goleak v1.1.12
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e // indirect
	golang.org/x/tools v0.1.8
	google.golang.org/genproto v0.0.0-20211208223120-3a66f561d7aa
	google.golang.org/grpc v1.43.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	mvdan.cc/gofumpt v0.2.1
)

// TODO(jschorr): Remove once https://github.com/dgraph-io/ristretto/pull/286 is merged
replace github.com/dgraph-io/ristretto => github.com/josephschorr/ristretto v0.1.1-0.20211227180020-ae4c2c35d79d
