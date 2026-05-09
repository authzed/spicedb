# go-pgquery

go-pgquery is a drop-in replacement for [pg_query_go][1] which uses cgo to invoke the
[libpg_query][2] library. By default, the c code is compiled to a WebAssembly module 
and accessed with the pure Go runtime, [wazero][3]. This means that it is compatible with
any Go application, regardless of availability of cgo, and will not have toolchain issues
such as those that libpg_query commonly sees with MacOS upgrades.

Unlike other wasilibs libraries, this library cannot be used with TinyGo because it relies
on WebAssembly threads support, which is not expected in TinyGo for quite some time.

## Compatibility

The library is expected to be fully compatible with pg_query_go. The same tests from pg_query_go
are run, and in addition test cases from libpg_query are also imported to increase coverage.
There are no known behavior differences with upstream libraries.

## Stability

While all tests from both upstream sources currently execute, this is despite [issues][4] that were
found that only affected the WebAssembly toolchain. While coverage is expected to be high, it is
not perfect and it is possible future issues that only affect WebAssembly compilation will be
found. If encountering any issues, always feel free to file an issue.

## Usage

go-pgquery is a standard Go library package and can be added to a go.mod file. It will work fine in
any Go project.

```
go get github.com/wasilibs/go-pgquery
```

Because the library is a drop-in replacement for pg_query_go, it is sufficient to change
import statements.

```go
import "github.com/pganalyze/pg_query_go/v6/parser"
```

can be changed to

```go
import "github.com/wasilibs/go-pgquery"
```

Note that to allow users to write transformation logic targeting both libraries, this library
returns parse trees using the types from pg_query_go. This means you will have both libraries
in your requirements and will need to be careful calling the entry point functions like `Parse`
from go-pgquery, not pg_query_go. This may change in the future.

### cgo

This library also supports opting into using cgo to wrap libpg_query instead of using WebAssembly.
The build tag `pgquery_cgo` can be used to enable cgo support. Note, this is exactly the same
code as `pg_query_go` - if you only need cgo support, it is recommended to use the official
library instead of this one.

## Performance

Benchmarks are run against every commit in the [bench][5] workflow. GitHub action runners are highly
virtualized and do not have stable performance across runs, but the relative numbers within a run
should still be somewhat, though not precisely, informative.

One run looks like this

```
                              │ build/bench_default.txt │           build/bench.txt           │         build/bench_cgo.txt         │
                              │         sec/op          │    sec/op      vs base              │    sec/op     vs base               │
ParseSelect1-4                            24.574µ ± ∞ ¹   24.859µ ± ∞ ¹       ~ (p=0.421 n=5)   6.558µ ± ∞ ¹  -73.31% (p=0.008 n=5)
ParseSelect2-4                             82.56µ ± ∞ ¹    81.40µ ± ∞ ¹  -1.41% (p=0.032 n=5)   21.68µ ± ∞ ¹  -73.74% (p=0.008 n=5)
ParseCreateTable-4                        186.70µ ± ∞ ¹   186.20µ ± ∞ ¹       ~ (p=1.000 n=5)   51.30µ ± ∞ ¹  -72.52% (p=0.008 n=5)
ParseSelect1Parallel-4                    12.027µ ± ∞ ¹   11.793µ ± ∞ ¹       ~ (p=0.548 n=5)   3.294µ ± ∞ ¹  -72.61% (p=0.008 n=5)
ParseSelect2Parallel-4                     39.83µ ± ∞ ¹    39.23µ ± ∞ ¹  -1.50% (p=0.032 n=5)   11.41µ ± ∞ ¹  -71.36% (p=0.008 n=5)
ParseCreateTableParallel-4                 91.27µ ± ∞ ¹    90.28µ ± ∞ ¹       ~ (p=0.222 n=5)   27.76µ ± ∞ ¹  -69.58% (p=0.008 n=5)
RawParseSelect1-4                         22.439µ ± ∞ ¹   22.049µ ± ∞ ¹       ~ (p=0.548 n=5)   4.613µ ± ∞ ¹  -79.44% (p=0.008 n=5)
RawParseSelect2-4                          73.07µ ± ∞ ¹    74.15µ ± ∞ ¹       ~ (p=0.690 n=5)   15.50µ ± ∞ ¹  -78.80% (p=0.008 n=5)
RawParseCreateTable-4                     167.01µ ± ∞ ¹   167.32µ ± ∞ ¹       ~ (p=1.000 n=5)   36.12µ ± ∞ ¹  -78.38% (p=0.008 n=5)
RawParseSelect1Parallel-4                 10.667µ ± ∞ ¹   10.552µ ± ∞ ¹       ~ (p=0.095 n=5)   2.280µ ± ∞ ¹  -78.63% (p=0.008 n=5)
RawParseSelect2Parallel-4                 35.777µ ± ∞ ¹   35.309µ ± ∞ ¹       ~ (p=0.135 n=5)   7.798µ ± ∞ ¹  -78.20% (p=0.008 n=5)
RawParseCreateTableParallel-4              82.15µ ± ∞ ¹    81.99µ ± ∞ ¹       ~ (p=0.079 n=5)   18.68µ ± ∞ ¹  -77.26% (p=0.008 n=5)
FingerprintSelect1-4                      15.984µ ± ∞ ¹   15.631µ ± ∞ ¹       ~ (p=0.310 n=5)   2.465µ ± ∞ ¹  -84.58% (p=0.008 n=5)
FingerprintSelect2-4                      36.803µ ± ∞ ¹   36.140µ ± ∞ ¹       ~ (p=0.095 n=5)   5.006µ ± ∞ ¹  -86.40% (p=0.008 n=5)
FingerprintCreateTable-4                  60.191µ ± ∞ ¹   60.951µ ± ∞ ¹       ~ (p=0.151 n=5)   9.172µ ± ∞ ¹  -84.76% (p=0.008 n=5)
NormalizeSelect1-4                         9.864µ ± ∞ ¹   10.151µ ± ∞ ¹       ~ (p=0.095 n=5)   1.691µ ± ∞ ¹  -82.86% (p=0.008 n=5)
NormalizeSelect2-4                        21.154µ ± ∞ ¹   22.804µ ± ∞ ¹  +7.80% (p=0.008 n=5)   2.987µ ± ∞ ¹  -85.88% (p=0.008 n=5)
NormalizeCreateTable-4                    21.750µ ± ∞ ¹   22.841µ ± ∞ ¹  +5.02% (p=0.008 n=5)   4.312µ ± ∞ ¹  -80.17% (p=0.008 n=5)
```

We see that the WebAssembly version performs about 4-5x slower than cgo. This is a big difference and the largest we've
found in wasilibs. It may be because of inefficiency in the implementation of exception handling. If your application
requires online processing of high volume of queries, it may be better to continue with cgo. However, if it is an offline
tool that only parses a small number of queries, it may still be reasonable performance in exchange for a simplified
build toolchain - operations still complete within dozens of microseconds.

[1]: https://github.com/pganalyze/pg_query_go
[2]: https://github.com/pganalyze/libpg_query
[3]: https://wazero.io
[4]: https://github.com/wasilibs/go-pgquery/blob/main/buildtools/wasm/Dockerfile#L13
[5]: https://github.com/wasilibs/go-pgquery/actions/workflows/bench.yaml
