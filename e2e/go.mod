module github.com/authzed/spicedb/e2e

go 1.21

toolchain go1.21.1

require (
	github.com/authzed/authzed-go v0.10.1-0.20231003161414-9c9116f212b7
	github.com/authzed/grpcutil v0.0.0-20230908193239-4286bb1d6403
	github.com/authzed/spicedb v1.23.1
	github.com/brianvoe/gofakeit/v6 v6.23.0
	github.com/ecordell/optgen v0.0.10-0.20230609182709-018141bf9698
	github.com/jackc/pgx/v5 v5.4.2
	github.com/stretchr/testify v1.8.4
	golang.org/x/tools v0.11.0
	google.golang.org/grpc v1.57.0
	mvdan.cc/gofumpt v0.5.0
)

require (
	github.com/antlr/antlr4/runtime/Go/antlr/v4 v4.0.0-20230512164433-5d1fd1a340c9 // indirect
	github.com/authzed/cel-go v0.17.5 // indirect
	github.com/certifi/gocertifi v0.0.0-20210507211836-431795d63e8d // indirect
	github.com/creasty/defaults v1.7.0 // indirect
	github.com/dave/jennifer v1.6.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.0.2 // indirect
	github.com/fatih/structtag v1.2.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.16.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jzelinskie/stringz v0.0.2 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rs/zerolog v1.29.1 // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/stoewer/go-strcase v1.3.0 // indirect
	golang.org/x/crypto v0.12.0 // indirect
	golang.org/x/exp v0.0.0-20230801115018-d63ba01acd4b // indirect
	golang.org/x/mod v0.12.0 // indirect
	golang.org/x/net v0.14.0 // indirect
	golang.org/x/sync v0.3.0 // indirect
	golang.org/x/sys v0.11.0 // indirect
	golang.org/x/text v0.12.0 // indirect
	google.golang.org/genproto v0.0.0-20230724170836-66ad5b6ff146 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230724170836-66ad5b6ff146 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230724170836-66ad5b6ff146 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/authzed/spicedb => ../
