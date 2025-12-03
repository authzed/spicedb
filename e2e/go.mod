module github.com/authzed/spicedb/e2e

go 1.25.5

// See: https://github.com/envoyproxy/go-control-plane/issues/1074
replace github.com/envoyproxy/go-control-plane => github.com/envoyproxy/go-control-plane v0.13.2

replace github.com/authzed/spicedb => ../

require (
	github.com/authzed/authzed-go v1.6.0
	github.com/authzed/grpcutil v0.0.0-20240123194739-2ea1e3d2d98b
	github.com/authzed/spicedb v1.29.5
	github.com/brianvoe/gofakeit/v6 v6.28.0
	github.com/ecordell/optgen v0.1.1
	github.com/jackc/pgx/v5 v5.7.6
	github.com/stretchr/testify v1.11.1
	golang.org/x/tools v0.38.0
	google.golang.org/grpc v1.76.0
	mvdan.cc/gofumpt v0.9.2
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.8-20250717185734-6c6e0d3c608e.1 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.0 // indirect
	github.com/authzed/cel-go v0.20.2 // indirect
	github.com/aws/aws-sdk-go-v2 v1.39.5 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.31.16 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.18.20 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.12 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.6.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.39.0 // indirect
	github.com/aws/smithy-go v1.23.1 // indirect
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/ccoveille/go-safecast/v2 v2.0.0 // indirect
	github.com/certifi/gocertifi v0.0.0-20210507211836-431795d63e8d // indirect
	github.com/creasty/defaults v1.8.0 // indirect
	github.com/dave/jennifer v1.7.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/fatih/structtag v1.2.0 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-logr/zerologr v1.2.3 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jzelinskie/stringz v0.0.3 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/onsi/ginkgo/v2 v2.23.4 // indirect
	github.com/onsi/gomega v1.38.0 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240917153116-6f2963f01587 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/stoewer/go-strcase v1.3.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel v1.38.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/trace v1.38.0 // indirect
	golang.org/x/crypto v0.45.0 // indirect
	golang.org/x/exp v0.0.0-20250819193227-8b4c13bb791b // indirect
	golang.org/x/mod v0.29.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sync v0.18.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250929231259-57b25ae835d4 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251022142026-3a174f9686a8 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	sigs.k8s.io/controller-runtime v0.22.3 // indirect
)
