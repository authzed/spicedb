# Namespace/relation statistics experiment

Small, standalone investigation for https://github.com/authzed/spicedb/issues/3318.
It compares one query with ordinary statistics, `mcv` (most-common-value),
`dependencies`, and both extended statistics types together, all on
`relation_tuple(namespace, relation)`.

## Run

Requirements: Python 3, a running Docker daemon, and the Go toolchain required by
this checkout. No Python packages are needed. The first run may download the
PostgreSQL image and Go dependencies.

From the repository root:

```sh
python3 development/postgres/statistics/run.py --output-dir /tmp/spicedb-mcv-results
```

To repeat at one million rows with the same proportions and query:

```sh
python3 development/postgres/statistics/run.py --rows 1000000 --output-dir /tmp/spicedb-mcv-results-1m
```

Choose a new output directory for each run. Without `--output-dir`, the script
creates a fresh temporary results directory. It prints the report and saves
`report.md` and `plans.json`, including all five measured plans per phase and the
actual index definitions. To reuse a CLI already built from this checkout, pass
`--spicedb-bin /path/to/spicedb`.

The script starts its own temporary `postgres:18` container, exposes PostgreSQL
only on a randomly assigned localhost port, and runs this checkout's migrations
to `head`. It removes the container afterward; it never connects to an existing
database. The report records the exact PostgreSQL version and image ID.

## Test

1. Insert 100,000 distinct relationship rows by default (`--rows` changes this),
   interleaving three combinations:
   60% document/viewer, 10% document/editor, and 30% group/member.
2. Vacuum once, then analyze and measure the query with ordinary statistics.
3. Test MCV, dependencies, then both types together. For each configuration,
   create one statistics object, analyze, measure the identical query, and drop
   that statistics object. No extended statistics carry over to the next phase.
4. Report estimated/actual rows, plan/index, median execution and planning time,
   execution range, and shared buffer hits/reads. All raw plans are also saved.

```sql
SELECT * FROM relation_tuple
WHERE namespace = 'group' AND relation = 'member';
```

Without knowledge of the correlation, the simplified estimate is
`100000 * 0.30 * 0.30 = 9000`; the actual result is 30,000 rows. The experiment
checks whether each statistics type brings the estimate closer. It verifies that
all phases return exactly 30% of the generated rows (30,000 by default; 300,000 at one
million), but does not assume a particular plan or speedup. `--rows` must be a
positive multiple of 10 so the distribution remains exact.

Each phase has one warm-up and five measured executions. Parallel query and JIT
are disabled to make the initial comparison easier to interpret. Automatic
maintenance is disabled only on the disposable relationship table; all phases
explicitly refresh statistics. PostgreSQL's default statistics target is retained.
The fixed order and small repetition count make timings exploratory. Independent
ANALYZE samples can slightly change the estimates between configurations.

The saved `results/` and `results-1m/` directories contain the earlier baseline
versus MCV runs. [results-types-1m/report.md](results-types-1m/report.md) compares
all four configurations on one million rows. New runs include all four
configurations, and `plans.json` records the actual statistics definitions as
well as the execution plans.

This intentionally simplified query omits SpiceDB's visibility, resource/subject
ID, and expiration filters. In particular, it cannot rely on the partial index
that includes only undeleted rows. The experiment measures SQL execution in
PostgreSQL, not application/network latency. A better estimate may leave both
the plan and performance unchanged. Small timing differences are inconclusive;
real SpiceDB queries and broader workloads would be a separate follow-up.
