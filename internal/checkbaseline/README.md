# Local check baseline

This harness compares local graph dispatch with the local query planner on a
shared memdb or local PostgreSQL snapshot. All assertions are checked
before timing; comparisons with different relationship traces are labelled and
have no speed ratio in the report.

From the repository root, using an installed Go toolchain:

```sh
mkdir -p artifacts/check-baseline
GOPROXY=off GOSUMDB=off go run ./cmd/checkbaseline \
  -mode=measure -profile=both -samples=10 -repetitions=3 \
  -output=artifacts/check-baseline/results.json
python3 internal/checkbaseline/report/render.py \
  artifacts/check-baseline/results.json artifacts/check-baseline/report.html
open artifacts/check-baseline/report.html
```

Set `GOTOOLCHAIN=local` and use the cached toolchain binary directly if your Go
launcher attempts a toolchain download. `-dataset` and `-case` accept regular
expressions. `-mode=audit` skips timing. Output replacement requires `-overwrite`.
The HTML is standalone; JSON contains the complete inputs and query traces.

## Boundary and controls

- Classic concurrency and chunk size are 1. The serial reducer avoids speculative
  branch execution. No dispatch result cache is installed.
- QP uses schema execution order, targeted recursive checks, base-first exclusion,
  strict subject matching, exhaustive `.all` checks, direct-read early return,
  combined concrete/wildcard reads, omission of redundant single-type filters,
  and classic non-ellipsis reads for relations with exactly one indirect
  subject type/relation. The latter falls back to typed reads for pagination,
  nonconcrete resources, heterogeneous usersets, and custom query readers.
  All of these execution controls are opt-in; no advisor or queryopt pass runs.
- QP's build defers caveats to the final expression evaluation and coalesces trait
  variants sharing one physical read. **Deferred plans must not be consumed as
  authorization decisions without evaluating the expression.**
- The relationship projection fix and caveat pushdown correction also apply to
  default QP behavior. PR #3386's alias and recursive identity fixes are included.
- Both engines use the same snapshot. The memdb delay profile requests
  100 microseconds once per
  relationship query; operating-system scheduling determines actual delay.

One check means one resource/permission/subject. Timing includes fresh request
contexts, caveat runners and final decisions; schema compilation, dataset setup
and recording are outside timing. Prepared plans are reused. Work auditing runs
three times separately. Ten timing samples contain equal calibrated iteration
counts per engine; engine order alternates each sample. Bands are IQRs of sample
means, not request latency percentiles or confidence intervals.

## Interpretation

Loads mean relationship query API calls, including queries returning no rows.
Rows count consumed relationships. Logical bytes are the JSON encoding of each
consumed `tuple.Relationship`, including caveats, expiration and integrity; they
are not wire bytes, disk usage or physical rows examined. Snapshot input bytes
encode schema and exported relationships. Schema acquisition/access, actual schema datastore reader calls below the
cache, and CEL leaf evaluations are reported separately. Schema datastore loads
are API calls, not disk reads or individual SQL statements; logical schema
accesses can be cache hits. Older reports without this instrumentation show
unknown loads rather than zero. Preparation is a single measured observation
for all plans in a dataset/profile, repeated in its case rows, not an execution
sample and not intended to be summed across cases.

Trace parity requires the same relationship query count, filters/options,
consumed IDs/order, iterator completion, rows and logical bytes. Schema work can
differ even for matching traces. Residual differences include composite userset
queries, branch ordering and wildcard-only filters;
inspect the per-case trace before attributing a timing difference to CPU cost.

The catalog includes benchmark registry inputs, deterministic scale sweeps,
consistency assertions and selected steelthread examples. Fixtures without
assertions are explicit omissions. Cycles, depth errors and cancellation are
correctness tests, not latency measurements. Each comparison executes serially in one client process. Multi-request
throughput and distributed execution are outside this baseline.

## Validation

```sh
go test ./internal/checkbaseline ./cmd/checkbaseline ./pkg/query/... \
  ./internal/caveats/... ./internal/graph/... ./internal/dispatch/...
go test -tags=integration ./internal/services/integrationtesting/queryconsistency \
  -run TestQueryPlanConsistency -count=1
go test -race ./internal/checkbaseline ./internal/graph -run 'Test(Serial|Cycle|Reader|Audit|Engine|Timing|Mixed|Aligned|Direct|Wildcard|Deferred|Caveat)'
python3 -m unittest discover -s internal/checkbaseline/report -v
```

## Immutable report history and scaling

Reports published under `artifacts/check-baseline/history/runs/<run-id>/` are never
replaced. Each archive contains HTML, compact chart data, complete compressed raw
shards, source/configuration metadata and a checksummed manifest. The root
`history/index.html` works directly from disk and compares repeated compatible
cases. Source measurements and the report-renderer revision are recorded separately.

Run a new larger-data report from a clean committed worktree (use the installed
Go toolchain path for `--go`):

```sh
python3 internal/checkbaseline/report/run_suite.py \
  --go /path/to/go --id 20260929-large-memdb --title 'Larger datasets · memdb' \
  --catalog scaling --profile both
```

The scaling catalog separates fixed small hot subgraphs in 1K/100K/1M-row databases,
100K/1M-row dense direct relations, 5K/10K fanout, and 64/128 recursive depth. It also
remeasures original small anchor datasets. Background rows occupy unrelated document
resources (100 rows each); they do not add permission paths to the checked resource.

The helper runs one dataset per process, compresses its results after measurement,
and only publishes after all selected datasets succeed. This bounds retained input
memory without dropping raw data. `--dataset` narrows the selection. Failed partial
runs retain their artifacts and logs; `--resume` accepts only the same source and
configuration and verifies completed-shard hashes. No archived run can be resumed
or overwritten.

To preserve an existing measured report, use `history.py archive --root <history>
--id <unique-id> --title <title> --input <results.json[.gz]> --existing-report
<report.html>`. Multiple `--input` arguments combine compatible shards. `history.py
verify <run-directory>` verifies archive hashes; `history.py index --root <history>`
rebuilds the index without changing saved reports.

Historical comparisons require identical dataset hashes, requests and depth limits,
profiles, baseline settings, backend and machine/toolchain configuration. They show
each run's own median and IQR plus the ratio of independent-run medians. They do not
pair samples across runs. Changes in recorded work are flagged. New or incompatible
cases remain accessible in their own reports.


## Local PostgreSQL and single-store schema

The PostgreSQL backend requires an existing local server. The connection URI must
use loopback and target the maintenance database `postgres`; the harness creates
a uniquely named database for each dataset, migrates it, loads the data, runs
`VACUUM ANALYZE`, and drops only the database it created. Use a dedicated local
server with enough space for the largest dataset. The runner does not start or
download containers.

Set these environment variables:

- `CHECKBASELINE_POSTGRES_URI`: for example,
  `postgres://postgres@127.0.0.1:5432/postgres?sslmode=disable`.
- `CHECKBASELINE_BACKEND_METADATA`: JSON recording the server deployment.
  Required fields are `image`, `architecture`, `dockerVersion`, `transport`,
  `cpus`, `memoryBytes`, `vmCPUs`, and `vmMemoryBytes`. Supply actual values
  for the local deployment; do not reuse another machine's metadata.

The connection string is not written into reports. Unsupported `PGOPTIONS`,
`PGSERVICE`, and `PGSERVICEFILE` overrides are rejected. Reports record effective
PostgreSQL settings, physical relationship rows, table/index/database bytes, setup
time, and deployment metadata. Read/write pools each use one connection; SpiceDB
GC and heartbeat are disabled. PostgreSQL durability and buffer settings come
from the configured server and are recorded. Measurements reuse warm buffers;
there is no buffer flush between engines.

Use `--experimental-schema-mode=read-new-write-new` to give both engines the
unified schema reader with a shared standard 32 MiB schema cache. Timing uses
unwrapped readers; separate work audits instrument schema reads below the cache.
Schema mode/cache policy are recorded and participate in history compatibility.
The legacy schema mode remains available and is the default.

This command selects the 32-dataset size/depth suite, including eight families
with 1K/100K/1M background relationships, dense direct relations, recursion depth,
and small anchors:

```sh
python3 internal/checkbaseline/report/run_suite.py \
  --go /path/to/go --id postgres-aligned-v1 \
  --title 'Aligned relationship work · PostgreSQL' \
  --catalog scaling --backend postgres --profile postgres \
  --experimental-schema-mode=read-new-write-new \
  --samples 10 --repetitions 3 \
  --dataset '^(sized/|generated/(direct|arrow|recursive|exclusion)/small$|generated/direct/dense|generated/recursive/depth)'
```

Pass `--evidence /path/to/file` repeatedly to archive deployment or test evidence
alongside raw measurements. The complete scaling catalog also includes 5K/10K
fanout workloads; run these separately because their traversal cost can be much
larger. SQL integration tests opt in through the same two environment variables.

## Reading the report

The report includes a results summary, dataset-size/timing scatter plots, IQR
bands, sortable case columns, and paired loads/rows/logical-byte bars. Raw timing
samples, exact query traces, preparation costs, and physical SQL sizes remain
available. The history page combines saved runs and supports filtering by run,
workload family, backend profile, work parity, and dataset size.

A QP/classic ratio below one indicates lower QP check time. The summary gives
each case equal weight; it is not a workload-weighted throughput result.
Plans are prepared outside timing, while per-request execution and final caveat
evaluation are timed. Exact relationship parity does not imply identical CPU
work: classic still resolves schema from its cache while QP uses prepared plans.

Local run `20260929-postgres-aligned-work`, measured from source commit
`187950aef` before publication cleanup, recorded 84 matching cases across 32 datasets, up
to 1,000,000 relationships, with 1,680 timing samples and zero schema datastore
loads for both engines. These observations describe that workload and local
environment, not a guarantee for other schemas or deployments. Its report and
checksummed raw shards remain in the local archive
`artifacts/check-baseline/history/runs/20260929-postgres-aligned-work/`; they
are not included in the repository or measurements of this publication branch. The broader
fixture catalog retains explicitly differing work and withholds ratios for it.

Generated HTML, data, logs, and screenshots stay under the ignored
`artifacts/check-baseline/` directory. Commit the harness and methodology;
retain measurement archives separately with their original source commits.
