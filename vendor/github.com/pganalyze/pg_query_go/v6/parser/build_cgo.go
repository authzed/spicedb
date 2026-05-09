//go:build required
// +build required

package parser

// This file exists purely to prevent the Golang toolchain from stripping
// away the C source directories and files when `go mod vendor` is used
// to populate a `vendor/` directory of a project depending on `github.com/pganalyze/pg_query_go/v6`.
//
// How it works:
//  - Every directory which only includes C source files receives a gokeep.go file.
//  - Every directory we want to preserve is included here as a _ import.
//  - This file is given a build tag to exclude it from the regular build.
import (
	// Prevent Go tooling from stripping out the C source files.
	_ "github.com/pganalyze/pg_query_go/v6/parser/include"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/access"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/archive"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/catalog"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/commands"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/common"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/datatype"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/executor"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/foreign"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/jit"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/lib"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/libpq"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/mb"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/nodes"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/optimizer"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/parser"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/partitioning"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/port"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/port/atomics"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/port/win32"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/port/win32/arpa"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/port/win32/netinet"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/port/win32/sys"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/port/win32_msvc"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/port/win32_msvc/sys"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/portability"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/postmaster"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/regex"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/replication"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/rewrite"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/storage"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/tcop"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/tsearch"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/postgres/utils"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/protobuf"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/protobuf-c"
	_ "github.com/pganalyze/pg_query_go/v6/parser/include/xxhash"
)
