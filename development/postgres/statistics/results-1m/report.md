# PostgreSQL namespace/relation MCV experiment

- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
- Image: `postgres:18` (`sha256:86c951e05bf56c93d95d397747fb8820ac76cc3bedb78f43abd83eedbe3666ae`)
- Checkout HEAD: `615be11658e42138246fdba03e56d209443761b9`
- Migration: `populate-schema-tables`
- Host: Darwin 25.3.0 arm64
- Existing relation_tuple indexes: 8 (definitions in plans.json)
- Dataset: 1,000,000 interleaved, distinct relationships: 600,000 document/viewer, 100,000 document/editor, 300,000 group/member. All undeleted; no caveats or expiration.
- Protocol: one warm-up + five measured executions per phase; manual ANALYZE before each phase; same data and indexes throughout.
- Controls: parallel query and JIT disabled; automatic maintenance disabled on this disposable table. No SpiceDB server or PgBouncer is involved.

```sql
SELECT * FROM relation_tuple
WHERE namespace = 'group' AND relation = 'member';
```

Added between phases:

```sql
CREATE STATISTICS relation_tuple_namespace_relation_mcv (mcv)
ON namespace, relation FROM relation_tuple;
ANALYZE relation_tuple;
```

| Measurement | Before | After |
|---|---|---|
| Estimated matching rows | 93147 | 297767 |
| Actual matching rows | 300000.0 | 300000.0 |
| Plan | Bitmap Heap Scan | Bitmap Heap Scan |
| Index(es) | ix_relation_tuple_by_subject_relation | ix_relation_tuple_by_subject_relation |
| Median execution time (ms) | 38.955 | 38.697 |
| Median planning time (ms) | 0.225 | 0.254 |
| Execution range (ms) | 37.348–43.400 | 37.992–39.325 |
| Median shared blocks hit | 11608 | 11608 |
| Median shared blocks read | 0 | 0 |

This is a synthetic estimate-accuracy experiment. Five timings after one warm-up are preliminary, not evidence of an end-to-end SpiceDB speedup. ANALYZE samples the data, so estimates can vary between runs. The query deliberately omits SpiceDB's visibility, ID, and expiration filters.
