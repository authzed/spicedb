# CockroachDB relationship statistics experiment

Run a small before/after experiment on the real migrated SpiceDB schema. Requires
Python 3, Docker, and Go (or an existing SpiceDB CLI built from this checkout).

```sh
python3 development/crdb/statistics/run.py --rows 1000000 \
  --output-dir /tmp/spicedb-crdb-subject-stats
```

`--rows` defaults to 100,000 and must be a positive multiple of 10. `--image`
defaults to `cockroachdb/cockroach:v26.2.5`, matching this checkout's latest tested
CRDB version. `--spicedb-bin /path/to/spicedb` skips the build. The output directory
must not already contain a report or raw plans.

The script starts a disposable, localhost-only, insecure single-node container,
applies all SpiceDB migrations, and inserts interleaved synthetic relationships:

| Share | Resource type/relation | Subject type/relation |
|---|---|---|
| 60% | document/viewer | user/... |
| 10% | document/editor | user/... |
| 30% | group/member | group/member |

Resource IDs are unique integers as strings; subject IDs have a `subject-` prefix.
This represents group membership via another group's members. All relationships
have no caveats or expiration. The tested query is:

```sql
SELECT * FROM relation_tuple
WHERE namespace = 'group' AND relation = 'member' AND userset_relation = 'member';
```

It compares:

1. Fresh default statistics (same column selection as automatic collection).
2. Default statistics plus explicit `(namespace, relation)` statistics.
3. Default statistics plus explicit `(namespace, relation, userset_relation)` statistics.

CockroachDB already collects multicolumn statistics on index prefixes. The script
saves that baseline, preserves it between phases, and removes each candidate
before the next. It disables automatic collection on only this disposable table
and disables forecasts on the disposable cluster. Unlike PostgreSQL,
`CREATE STATISTICS` collects immediately; there is no separate `ANALYZE` step and
no PostgreSQL-style `mcv` or `dependencies` option.

CockroachDB 26.2 restricts internal statistics deletion. The script enables
`allow_unsafe_internals` only in the sessions that clear this disposable table's
statistics, using its table ID to scope the deletion.

Each phase runs one warm-up followed by five `EXPLAIN ANALYZE` executions. The
script validates the actual row count, prints a report, saves `report.md` and
`plans.json`, and removes the container, including on failure. Results include
server execution time, output-row estimates, operator/index choices, raw plans,
and collected statistics metadata. Existing data and indexes do not change
between phases.

The synthetic data and fixed phase order make timings preliminary. This is not a
SpiceDB request benchmark, production workload, or cross-database speed comparison.
Collection and maintenance overhead are unmeasured. Only generated data and local
disposable credentials are used; no external datastore connection is accepted.

[results-subject-1m/report.md](results-subject-1m/report.md) records the
one-million-row run. The baseline and pair estimated 27,544 matching rows;
the triple estimated 274,554, against 300,000 actual matches. All 15 measured
executions used the same primary-key range scan and filter. Median execution
times were 93–95 ms, so this run does not establish a speedup.

See [CockroachDB CREATE STATISTICS](https://www.cockroachlabs.com/docs/stable/create-statistics)
for default column selection, immediate collection, and named-statistics deletion.

## Recorded one-million-row result

[Report](results-subject-1m/report.md) and [raw plans](results-subject-1m/plans.json).
The default baseline collected 22 statistics, including index-prefix combinations,
but neither of our exact pair/triple candidates. Adding the pair left the result
estimate at 27,544; adding the triple improved it to 274,554 for 300,000 actual
rows. Both candidates recorded three distinct combinations and no multicolumn
histogram. All 15 measured queries retained the same primary-key range scan and
filter and decoded 300,000 KV rows. Medians of 95/93/94 ms do not demonstrate a
meaningful speedup in this small fixed-order experiment.
