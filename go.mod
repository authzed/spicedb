module github.com/authzed/spicedb

go 1.16

require (
	github.com/Masterminds/squirrel v1.5.2
	github.com/alecthomas/units v0.0.0-20210927113745-59d0afb8317a
	github.com/authzed/authzed-go v0.3.1-0.20211220220442-a36f72252b43
	github.com/authzed/grpcutil v0.0.0-20211020204402-aba1876830e6
	github.com/aws/aws-sdk-go v1.42.16
	github.com/benbjohnson/clock v1.3.0
	github.com/cespare/xxhash v1.1.0
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
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
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.7.0
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-memdb v1.3.2
	github.com/influxdata/tdigest v0.0.1
	github.com/jackc/pgconn v1.10.1
	github.com/jackc/pgtype v1.9.1
	github.com/jackc/pgx/v4 v4.14.1
	github.com/jmoiron/sqlx v1.3.4
	github.com/johannesboyne/gofakes3 v0.0.0-20210608054100-92d5d4af5fde
	github.com/jwangsadinata/go-multimap v0.0.0-20190620162914-c29f3d7f33b6
	github.com/jzelinskie/cobrautil v0.0.7
	github.com/jzelinskie/stringz v0.0.1
	github.com/lib/pq v1.10.4
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/ngrok/sqlmw v0.0.0-20210819213940-241da6c2def4
	github.com/ory/dockertest/v3 v3.8.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rs/zerolog v1.26.0
	github.com/scylladb/go-set v1.0.2
	github.com/sercand/kuberesolver/v3 v3.1.0
	github.com/shopspring/decimal v1.3.1
	github.com/spf13/cobra v1.2.1
	github.com/spf13/viper v1.9.0 // indirect
	github.com/stretchr/objx v0.3.0 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.27.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.27.0
	go.opentelemetry.io/otel v1.2.0
	go.opentelemetry.io/otel/exporters/jaeger v1.1.0 // indirect
	go.opentelemetry.io/otel/trace v1.2.0
	go.uber.org/goleak v1.1.12
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/net v0.0.0-20211104170005-ce137452f963 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e // indirect
	golang.org/x/tools v0.1.8 // indirect
	google.golang.org/genproto v0.0.0-20211118181313-81c1377c94b1
	google.golang.org/grpc v1.42.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
)

// TODO(jschorr): Remove once https://github.com/dgraph-io/ristretto/pull/286 is merged
replace github.com/dgraph-io/ristretto => github.com/josephschorr/ristretto v0.1.1-0.20211008180146-169a43af0a0d
