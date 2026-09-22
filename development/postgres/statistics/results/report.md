# PostgreSQL namespace/relation MCV experiment

- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
- Image: `postgres:18` (`sha256:86c951e05bf56c93d95d397747fb8820ac76cc3bedb78f43abd83eedbe3666ae`)
- Checkout HEAD: `f92a579f0225a5614346c0e6694f261709e21d35`
- Migration: `populate-schema-tables`
- Host: Darwin 25.3.0 arm64
- Existing relation_tuple indexes: 8 (definitions in plans.json)
- Dataset: 100,000 interleaved, distinct relationships: 60,000 document/viewer, 10,000 document/editor, 30,000 group/member. All undeleted; no caveats or expiration.
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
| Estimated matching rows | 8928 | 29940 |
| Actual matching rows | 30000.0 | 30000.0 |
| Plan | Bitmap Heap Scan | Bitmap Heap Scan |
| Index(es) | ix_relation_tuple_by_subject_relation | ix_relation_tuple_by_subject_relation |
| Median execution time (ms) | 4.679 | 4.855 |
| Median planning time (ms) | 0.241 | 0.251 |
| Execution range (ms) | 4.261–4.869 | 4.652–5.018 |
| Median shared blocks hit | 1165 | 1165 |
| Median shared blocks read | 0 | 0 |

This is a synthetic estimate-accuracy experiment. Five warm-cache timings are preliminary, not evidence of an end-to-end SpiceDB speedup. ANALYZE samples the data, so estimates can vary between runs. The query deliberately omits SpiceDB's visibility, ID, and expiration filters.
