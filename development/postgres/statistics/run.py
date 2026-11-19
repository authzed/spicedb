#!/usr/bin/env python3
"""Compare PostgreSQL statistics on namespace/relation, optionally with subject relation."""

import argparse
import json
import platform
from pathlib import Path
import statistics
import subprocess
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parents[3]
QUERY = """SELECT * FROM relation_tuple
WHERE namespace = 'group' AND relation = 'member'"""
STATISTICS_NAME = "relation_tuple_namespace_relation_stats"
CONFIGURATIONS = {
    "baseline": None,
    "mcv": "mcv",
    "dependencies": "dependencies",
    "both": "mcv, dependencies",
}
SEED = """
-- Only this disposable table has automatic maintenance disabled, so it cannot
-- refresh statistics between measurements. Every phase explicitly runs ANALYZE.
ALTER TABLE relation_tuple SET (autovacuum_enabled = false);
INSERT INTO relation_tuple (
    namespace, object_id, relation,
    userset_namespace, userset_object_id, userset_relation
)
SELECT
    CASE WHEN n % 10 < 7 THEN 'document' ELSE 'group' END,
    n::text,
    CASE WHEN n % 10 < 6 THEN 'viewer'
         WHEN n % 10 = 6 THEN 'editor' ELSE 'member' END,
    {subject_type}, {subject_id}, {subject_relation}
FROM generate_series(1, {rows}) AS n;
-- Equal maintenance state before all phases; data does not change afterward.
VACUUM relation_tuple;
"""


def run(args, *, sql=None, cwd=None):
    return subprocess.run(
        args, input=sql, text=True, stdout=subprocess.PIPE,
        check=True, cwd=cwd,
    ).stdout.strip()


def index_names(node):
    names = [node["Index Name"]] if "Index Name" in node else []
    for child in node.get("Plans", []):
        names.extend(index_names(child))
    return names


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="postgres:18")
    parser.add_argument("--rows", type=int, default=100000,
                        help="Total relationships; a positive multiple of 10 (default: 100000)")
    parser.add_argument("--subject-relation", action="store_true",
                        help="Use mixed subjects and compare pair vs triple statistics on a three-filter query")
    parser.add_argument("--spicedb-bin", type=Path,
                        help="Use an existing CLI built from this checkout instead of building it")
    parser.add_argument("--output-dir", type=Path,
                        help="Save report.md and plans.json (default: a new temporary directory)")
    args = parser.parse_args()
    if args.rows <= 0 or args.rows % 10:
        parser.error("--rows must be a positive multiple of 10 to preserve the 60/10/30 split")
    expected_matches = args.rows * 3 // 10
    query = QUERY
    column_groups = [("", "namespace, relation")]
    if args.subject_relation:
        query += "\n  AND userset_relation = 'member'"
        column_groups = [("pair_", "namespace, relation"),
                         ("triple_", "namespace, relation, userset_relation")]
    configurations = [("baseline", None, None)]
    for prefix, columns in column_groups:
        configurations.extend((prefix + phase, kinds, columns)
                              for phase, kinds in CONFIGURATIONS.items() if kinds)
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="spicedb-mcv-results-"))
    output.mkdir(parents=True, exist_ok=True)
    if any((output / name).exists() for name in ("report.md", "plans.json")):
        parser.error("output directory already contains report.md or plans.json")

    run(["docker", "info", "--format", "{{.ServerVersion}}"])
    container = "spicedb-mcv-" + uuid.uuid4().hex[:12]
    started = False
    plans = {}
    definitions = {}
    with tempfile.TemporaryDirectory(prefix="spicedb-mcv-build-") as build_dir:
        binary = args.spicedb_bin.resolve() if args.spicedb_bin else Path(build_dir) / "spicedb"
        if args.spicedb_bin is None:
            print("Building the checkout's migration CLI...", flush=True)
            subprocess.run(["go", "build", "-o", str(binary), "./cmd/spicedb"],
                           cwd=ROOT, check=True)
        try:
            print(f"Starting disposable {args.image}...", flush=True)
            run(["docker", "run", "--detach", "--rm", "--name", container,
                 "--publish", "127.0.0.1::5432",
                 "--env", "POSTGRES_USER=spicedb", "--env", "POSTGRES_PASSWORD=spicedb",
                 "--env", "POSTGRES_DB=spicedb", args.image,
                 "-c", "max_parallel_workers_per_gather=0", "-c", "jit=off"])
            started = True
            for _ in range(60):
                ready = subprocess.run(
                    ["docker", "exec", container, "pg_isready", "-h", "127.0.0.1",
                     "-U", "spicedb", "-d", "spicedb"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
                if ready.returncode == 0:
                    break
                time.sleep(1)
            else:
                raise RuntimeError("PostgreSQL did not become ready within 60 seconds")

            address = run(["docker", "port", container, "5432/tcp"])
            uri = f"postgres://spicedb:spicedb@{address}/spicedb?sslmode=disable"
            print("Applying the checkout's real migrations...", flush=True)
            subprocess.run([str(binary), "datastore", "migrate", "head",
                            "--datastore-engine", "postgres", "--datastore-conn-uri", uri],
                           check=True, cwd=ROOT)

            def sql(statement):
                return run(["docker", "exec", "-i", container, "psql", "-X", "-qAt",
                            "-v", "ON_ERROR_STOP=1", "-U", "spicedb", "-d", "spicedb"],
                           sql=statement)

            sql(SEED.format(
                rows=args.rows,
                subject_type="CASE WHEN n % 10 < 7 THEN 'user' ELSE 'group' END"
                if args.subject_relation else "'user'",
                subject_id="'subject-' || n::text" if args.subject_relation else "n::text",
                subject_relation="CASE WHEN n % 10 < 7 THEN '...' ELSE 'member' END"
                if args.subject_relation else "'...'",
            ))
            version = sql("SELECT version();")
            migration = sql("SELECT version_num FROM alembic_version;")
            distribution = json.loads(sql("""SELECT json_agg(s) FROM (
                SELECT namespace, relation, userset_namespace, userset_relation,
                       count(*) AS rows FROM relation_tuple
                GROUP BY namespace, relation, userset_namespace, userset_relation
                ORDER BY namespace, relation, userset_namespace, userset_relation
            ) s;"""))
            indexes = json.loads(sql("""SELECT json_agg(s) FROM (
                SELECT indexname, indexdef FROM pg_indexes
                WHERE schemaname = 'public' AND tablename = 'relation_tuple'
                ORDER BY indexname
            ) s;"""))
            for phase, kinds, columns in configurations:
                print(f"Measuring {phase} statistics...", flush=True)
                if kinds:
                    sql(f"CREATE STATISTICS {STATISTICS_NAME} ({kinds}) "
                        f"ON {columns} FROM relation_tuple;")
                sql("ANALYZE relation_tuple;")
                definitions[phase] = sql(
                    "SELECT pg_get_statisticsobjdef(oid) FROM pg_statistic_ext "
                    f"WHERE stxname = '{STATISTICS_NAME}';"
                )
                samples = []
                for iteration in range(6):  # One warm-up, then five measured executions.
                    plan = json.loads(sql("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query))[0]
                    if plan["Plan"]["Actual Rows"] != expected_matches:
                        raise RuntimeError(f"Unexpected matching row count in {phase}: {plan}")
                    if iteration:
                        samples.append(plan)
                plans[phase] = samples
                # Each configuration is tested alone, never on top of a prior one.
                if kinds:
                    sql(f"DROP STATISTICS {STATISTICS_NAME};")

            rows = []
            for label, field in (("Estimated matching rows", "Plan Rows"),
                                 ("Actual matching rows", "Actual Rows"),
                                 ("Plan", "Node Type")):
                rows.append((label, *(str(plans[p][0]["Plan"][field]) for p in plans)))
            rows.append(("Index(es)", *(", ".join(index_names(plans[p][0]["Plan"])) or "none"
                                        for p in plans)))
            for label, field in (("Median execution time (ms)", "Execution Time"),
                                 ("Median planning time (ms)", "Planning Time")):
                rows.append((label, *(f"{statistics.median(s[field] for s in plans[p]):.3f}"
                                      for p in plans)))
            rows.append(("Execution range (ms)", *(
                f"{min(s['Execution Time'] for s in plans[p]):.3f}–"
                f"{max(s['Execution Time'] for s in plans[p]):.3f}" for p in plans)))
            for label, field in (("Median shared blocks hit", "Shared Hit Blocks"),
                                 ("Median shared blocks read", "Shared Read Blocks")):
                rows.append((label, *(str(statistics.median(s["Plan"].get(field, 0)
                                                          for s in plans[p])) for p in plans)))

            commit = run(["git", "rev-parse", "HEAD"], cwd=ROOT)
            image_id = run(["docker", "inspect", "--format", "{{.Image}}", container])
            metadata = {"postgres": version, "commit": commit, "image": args.image,
                        "image_id": image_id, "migration": migration,
                        "rows": args.rows, "query": query,
                        "subject_relation": args.subject_relation,
                        "statistics_definitions": definitions,
                        "distribution": distribution, "indexes": indexes, "plans": plans}
            (output / "plans.json").write_text(json.dumps(metadata, indent=2) + "\n")
            report = "\n".join([
                "# PostgreSQL namespace/relation statistics experiment", "",
                f"- PostgreSQL: {version}", f"- Image: `{args.image}` (`{image_id}`)",
                f"- Checkout HEAD: `{commit}`", f"- Migration: `{migration}`",
                f"- Host: {platform.system()} {platform.release()} {platform.machine()}",
                f"- Existing relation_tuple indexes: {len(indexes)} (definitions in plans.json)",
                f"- Dataset: {args.rows:,} interleaved, distinct relationships: "
                f"{args.rows * 6 // 10:,} document/viewer, {args.rows // 10:,} document/editor, "
                f"{expected_matches:,} group/member. All undeleted; no caveats or expiration.",
                ("- Subjects: document rows reference direct users (`user#...`); group rows "
                 "reference group members (`group#member`). Subject IDs are prefixed with "
                 "`subject-` to avoid self-references."
                 if args.subject_relation else "- Subjects: all rows reference direct users (`user#...`)."),
                "- Protocol: one warm-up + five measured executions per phase; manual ANALYZE "
                "before each phase; same data and indexes throughout.",
                "- Order: " + ", ".join(plans) + ". Each extended statistics object "
                "is dropped before the next phase; configurations are not cumulative. "
                "Pair = (namespace, relation); triple adds userset_relation.",
                "- Controls: parallel query and JIT disabled; automatic maintenance disabled "
                "on this disposable table. No SpiceDB server or PgBouncer is involved.", "",
                "```sql", query + ";", "```", "", "Statistics tested separately "
                "(each followed by ANALYZE, measurements, then DROP STATISTICS):", "",
                "```sql", *(definition + ";" for definition in definitions.values() if definition),
                "```", "",
                "| Measurement | " + " | ".join(plans) + " |",
                "|" + "---|" * (len(plans) + 1),
                *("| " + " | ".join(row) + " |" for row in rows), "",
                "This is a synthetic estimate-accuracy experiment. Five timings after one warm-up are "
                "preliminary, not evidence of an end-to-end SpiceDB speedup. ANALYZE samples "
                "the data, so estimates can vary between runs. The query deliberately omits "
                "SpiceDB's visibility, ID, and expiration filters.", "",
            ])
            (output / "report.md").write_text(report)
            print("\n" + report)
            print(f"Saved report and raw plans to {output.resolve()}")
        finally:
            if started:
                subprocess.run(["docker", "rm", "--force", container], check=True,
                               stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
