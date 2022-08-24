module github.com/authzed/spicedb/e2e

go 1.18

require (
	github.com/authzed/authzed-go v0.6.1-0.20220829195957-23aec9014d2f
	github.com/authzed/grpcutil v0.0.0-20220104222419-f813f77722e5
	github.com/authzed/spicedb v1.5.0
	github.com/brianvoe/gofakeit/v6 v6.15.0
	github.com/ecordell/optgen v0.0.6
	github.com/jackc/pgtype v1.11.0
	github.com/jackc/pgx/v4 v4.16.1
	github.com/stretchr/testify v1.8.0
	golang.org/x/tools v0.1.12
	google.golang.org/grpc v1.48.0
	mvdan.cc/gofumpt v0.3.1
)

require (
	github.com/certifi/gocertifi v0.0.0-20210507211836-431795d63e8d // indirect
	github.com/dave/jennifer v1.5.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/envoyproxy/protoc-gen-validate v0.6.7 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.11.2 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.12.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	golang.org/x/crypto v0.0.0-20220427172511-eb4f295cb31f // indirect
	golang.org/x/mod v0.6.0-dev.0.20220419223038-86c51ed26bb4 // indirect
	golang.org/x/net v0.0.0-20220812174116-3211cb980234 // indirect
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4 // indirect
	golang.org/x/sys v0.0.0-20220818161305-2296e01440c6 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220819174105-e9f053255caa // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/authzed/spicedb => ../
