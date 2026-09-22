# PostgreSQL namespace/relation statistics experiment

- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
- Image: `postgres:18` (`sha256:86c951e05bf56c93d95d397747fb8820ac76cc3bedb78f43abd83eedbe3666ae`)
- Checkout HEAD: `6d1430953f37f76e4eb614624aad0920d172177f`
- Migration: `populate-schema-tables`
- Host: Darwin 25.3.0 arm64
- Existing relation_tuple indexes: 8 (definitions in plans.json)
- Dataset: 1,000,000 interleaved, distinct relationships: 600,000 document/viewer, 100,000 document/editor, 300,000 group/member. All undeleted; no caveats or expiration.
- Subjects: document rows reference direct users (`user#...`); group rows reference group members (`group#member`). Subject IDs are prefixed with `subject-` to avoid self-references.
- Protocol: one warm-up + five measured executions per phase; manual ANALYZE before each phase; same data and indexes throughout.
- Order: baseline, pair_mcv, pair_dependencies, pair_both, triple_mcv, triple_dependencies, triple_both. Each extended statistics object is dropped before the next phase; configurations are not cumulative. Pair = (namespace, relation); triple adds userset_relation.
- Controls: parallel query and JIT disabled; automatic maintenance disabled on this disposable table. No SpiceDB server or PgBouncer is involved.

```sql
SELECT * FROM relation_tuple
WHERE namespace = 'group' AND relation = 'member'
  AND userset_relation = 'member';
```

Statistics tested separately (each followed by ANALYZE, measurements, then DROP STATISTICS):

```sql
CREATE STATISTICS public.relation_tuple_namespace_relation_stats (mcv) ON namespace, relation FROM relation_tuple;
CREATE STATISTICS public.relation_tuple_namespace_relation_stats (dependencies) ON namespace, relation FROM relation_tuple;
CREATE STATISTICS public.relation_tuple_namespace_relation_stats (dependencies, mcv) ON namespace, relation FROM relation_tuple;
CREATE STATISTICS public.relation_tuple_namespace_relation_stats (mcv) ON namespace, relation, userset_relation FROM relation_tuple;
CREATE STATISTICS public.relation_tuple_namespace_relation_stats (dependencies) ON namespace, relation, userset_relation FROM relation_tuple;
CREATE STATISTICS public.relation_tuple_namespace_relation_stats (dependencies, mcv) ON namespace, relation, userset_relation FROM relation_tuple;
```

| Measurement | baseline | pair_mcv | pair_dependencies | pair_both | triple_mcv | triple_dependencies | triple_both |
|---|---|---|---|---|---|---|---|
| Estimated matching rows | 25690 | 89102 | 89820 | 89660 | 296633 | 299000 | 299833 |
| Actual matching rows | 300000.0 | 300000.0 | 300000.0 | 300000.0 | 300000.0 | 300000.0 | 300000.0 |
| Plan | Bitmap Heap Scan | Bitmap Heap Scan | Bitmap Heap Scan | Bitmap Heap Scan | Bitmap Heap Scan | Bitmap Heap Scan | Bitmap Heap Scan |
| Index(es) | ix_relation_tuple_by_subject_relation | ix_relation_tuple_by_subject_relation | ix_relation_tuple_by_subject_relation | ix_relation_tuple_by_subject_relation | ix_relation_tuple_by_subject_relation | ix_relation_tuple_by_subject_relation | ix_relation_tuple_by_subject_relation |
| Median execution time (ms) | 41.014 | 38.900 | 39.994 | 39.416 | 39.261 | 39.790 | 39.199 |
| Median planning time (ms) | 0.244 | 0.254 | 0.254 | 0.272 | 0.255 | 0.266 | 0.258 |
| Execution range (ms) | 39.149–44.996 | 38.661–40.048 | 39.627–40.611 | 38.799–47.546 | 38.770–39.467 | 38.844–40.481 | 38.288–40.776 |
| Median shared blocks hit | 12593 | 12594 | 12594 | 12594 | 12594 | 12594 | 12594 |
| Median shared blocks read | 1 | 0 | 0 | 0 | 0 | 0 | 0 |

This is a synthetic estimate-accuracy experiment. Five timings after one warm-up are preliminary, not evidence of an end-to-end SpiceDB speedup. ANALYZE samples the data, so estimates can vary between runs. The query deliberately omits SpiceDB's visibility, ID, and expiration filters.
