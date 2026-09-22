# CockroachDB namespace/relation/subject-relation statistics experiment

- Server: CockroachDB CCL v26.2.5 (aarch64-unknown-linux-gnu, built 2026/07/28 18:55:27, go1.25.5)
- Image: `cockroachdb/cockroach:v26.2.5` (`sha256:771325a0586bf61d53322d24f5a6de8962568b0fc181fa45db364278e5961282`)
- Checkout HEAD: `6d1430953f37f76e4eb614624aad0920d172177f`; migration: `populate-schema-tables`.
- Host: Darwin 25.3.0 arm64.
- Dataset: 1,000,000 distinct relationships, 60% document/viewer/user#..., 10% document/editor/user#..., 30% group/member/group#member. IDs are interleaved; subject IDs use a separate subject- prefix. No caveats/expiration.
- Real SpiceDB migrations/indexes; one disposable single-node container, 128 MiB cache, 256 MiB SQL memory. Direct SQL, no SpiceDB request processing.
- Baseline: freshly collected default statistics, including automatic index-prefix multicolumn statistics. Background table statistics collection and forecasting disabled for controlled measurement. Baseline snapshots retained unchanged across phases.
- Sequence: baseline, explicit pair, explicit triple. CREATE STATISTICS completes collection before each candidate is measured; each candidate is removed afterward. One warm-up + five measured queries each. Timing is server execution time.

```sql
SELECT * FROM relation_tuple
WHERE namespace = 'group' AND relation = 'member' AND userset_relation = 'member';
```

Additional statistics tested separately:

```sql
CREATE STATISTICS candidate_stats ON namespace, relation FROM relation_tuple;
CREATE STATISTICS candidate_stats ON namespace, relation, userset_relation FROM relation_tuple;
```

| Statistics | Estimated result rows | Actual result rows | Median execution (ms) | Range (ms) | Operators | Index |
|---|---:|---:|---:|---:|---|---|
| baseline | 27,544 | 300,000 | 95.000 | 93.000–98.000 | filter → scan | relation_tuple@pk_relation_tuple |
| pair | 27,544 | 300,000 | 93.000 | 93.000–101.000 | filter → scan | relation_tuple@pk_relation_tuple |
| triple | 274,554 | 300,000 | 94.000 | 89.000–96.000 | filter → scan | relation_tuple@pk_relation_tuple |

CockroachDB CREATE STATISTICS uses its native statistics; it does not expose PostgreSQL's MCV/dependencies switches. See plans.json for actual collected column sets, distinct counts, histogram IDs, schema, and every measured plan. These timings are preliminary and are not comparable as a PostgreSQL-versus-CockroachDB performance benchmark.

This simple synthetic query omits SpiceDB's revision and ID filters. Statistics collection/maintenance cost is not measured.

Reference: https://www.cockroachlabs.com/docs/stable/create-statistics
