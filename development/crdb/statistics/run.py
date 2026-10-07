#!/usr/bin/env python3
"""Compare default CockroachDB statistics with explicit pair/triple statistics."""

import argparse
import csv
import io
import json
from pathlib import Path
import platform
import re
import statistics
import subprocess
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parents[3]
QUERY = """SELECT * FROM relation_tuple
WHERE namespace = 'group' AND relation = 'member' AND userset_relation = 'member'"""
PHASES = {
    "baseline": None,
    "pair": "namespace, relation",
    "triple": "namespace, relation, userset_relation",
}


def run(args, *, sql=None, cwd=None):
    return subprocess.run(args, input=sql, text=True, stdout=subprocess.PIPE,
                          check=True, cwd=cwd).stdout.strip()


def duration_ms(plan, label):
    # Match only the top-level timing, not individual operator timings.
    match = re.search(r"^" + label + r": ([\d.]+)(µs|ms|s)$", plan, re.MULTILINE)
    if not match:
        raise RuntimeError(f"Missing {label} in plan: {plan}")
    return float(match[1]) * {"µs": 0.001, "ms": 1, "s": 1000}[match[2]]


def metrics(plan):
    def row_count(label):
        match = re.search(label + r": ([\d,]+)", plan)
        if not match:
            raise RuntimeError(f"Missing {label} in plan: {plan}")
        return int(match[1].replace(",", ""))

    return {
        "estimated_rows": row_count("estimated row count"),
        "actual_rows": row_count("actual row count"),
        "execution_ms": duration_ms(plan, "execution time"),
        "planning_ms": duration_ms(plan, "planning time"),
        "operators": re.findall(r"• ([^\n]+)", plan),
        "indexes": re.findall(r"table: ([^\n]+)", plan),
        "kv_rows": row_count("rows decoded from KV"),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="cockroachdb/cockroach:v26.2.5")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--spicedb-bin", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if args.rows <= 0 or args.rows % 10:
        parser.error("--rows must be a positive multiple of 10")
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="spicedb-crdb-statistics-"))
    output.mkdir(parents=True, exist_ok=True)
    if any((output / name).exists() for name in ("report.md", "plans.json")):
        parser.error("output directory already contains report.md or plans.json")
    run(["docker", "info", "--format", "{{.ServerVersion}}"])
    container = "spicedb-crdb-statistics-" + uuid.uuid4().hex[:12]
    started = False
    with tempfile.TemporaryDirectory(prefix="spicedb-crdb-build-") as build_dir:
        binary = args.spicedb_bin.resolve() if args.spicedb_bin else Path(build_dir) / "spicedb"
        if args.spicedb_bin is None:
            subprocess.run(["go", "build", "-o", str(binary), "./cmd/spicedb"], cwd=ROOT, check=True)
        try:
            print(f"Starting disposable {args.image}...", flush=True)
            run(["docker", "run", "--detach", "--rm", "--name", container,
                 "--publish", "127.0.0.1::26257", args.image,
                 "start-single-node", "--insecure", "--cache=128MiB", "--max-sql-memory=256MiB"])
            started = True

            def sql(statement, database="spicedb", internal=False):
                # v26.2 protects internal metadata. This opt-in is used only to
                # delete statistics for the disposable experiment's one table.
                if internal:
                    statement = "SET allow_unsafe_internals = true;\n" + statement
                raw = run(["docker", "exec", "-i", container, "cockroach", "sql",
                           "--insecure", f"--database={database}", "--format=csv"], sql=statement)
                rows = list(csv.reader(io.StringIO(raw)))
                return rows[1:] if internal else rows

            def scalar(statement):
                return sql(statement)[1][0]

            def plan_sql(statement):
                return "\n".join(row[0] for row in sql(statement)[1:] if row)

            for _ in range(60):
                ready = subprocess.run(["docker", "exec", container, "cockroach", "sql",
                                        "--insecure", "--execute=SELECT 1"],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if ready.returncode == 0:
                    break
                time.sleep(1)
            else:
                raise RuntimeError("CockroachDB not ready within 60 seconds")
            sql("CREATE DATABASE spicedb;", database="defaultdb")
            address = run(["docker", "port", container, "26257/tcp"])
            uri = f"postgresql://root@{address}/spicedb?sslmode=disable"
            subprocess.run([str(binary), "datastore", "migrate", "head",
                            "--datastore-engine", "cockroachdb", "--datastore-conn-uri", uri],
                           check=True, cwd=ROOT)
            initial_stats = sql("SHOW STATISTICS FOR TABLE relation_tuple;")
            # Freeze background refresh only in this disposable table. Baseline
            # collection still chooses the same columns as automatic collection.
            sql("ALTER TABLE relation_tuple SET (sql_stats_automatic_collection_enabled = false);")
            sql("SET CLUSTER SETTING sql.stats.forecasts.enabled = false;")
            print(f"Loading {args.rows:,} synthetic relationships...", flush=True)
            for start in range(1, args.rows + 1, 10000):
                end = min(args.rows, start + 9999)
                sql(f"""INSERT INTO relation_tuple
                    (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)
                    SELECT CASE WHEN n%10<7 THEN 'document' ELSE 'group' END, n::text,
                    CASE WHEN n%10<6 THEN 'viewer' WHEN n%10=6 THEN 'editor' ELSE 'member' END,
                    CASE WHEN n%10<7 THEN 'user' ELSE 'group' END, 'subject-' || n::text,
                    CASE WHEN n%10<7 THEN '...' ELSE 'member' END
                    FROM generate_series({start}, {end}) AS n;""")
            # Remove empty-table snapshots/automatic stats, then collect a fresh
            # default set. DELETE is scoped to this table in the disposable DB.
            table_id = scalar("SELECT 'relation_tuple'::REGCLASS::OID;")
            sql(f'DELETE FROM system.table_statistics WHERE "tableID" = {int(table_id)};', internal=True)
            sql("CREATE STATISTICS baseline_stats FROM relation_tuple;")
            baseline_stats = sql("SHOW STATISTICS FOR TABLE relation_tuple;")
            distribution = sql("SELECT namespace, relation, userset_namespace, userset_relation, count(*) "
                               "FROM relation_tuple GROUP BY 1,2,3,4 ORDER BY 1,2,3,4;")
            phases = {}
            for phase, columns in PHASES.items():
                print(f"Measuring {phase} statistics...", flush=True)
                definition = (f"CREATE STATISTICS candidate_stats ON {columns} FROM relation_tuple;"
                              if columns else None)
                if definition:
                    # Unlike PostgreSQL, CREATE STATISTICS collects immediately;
                    # no separate ANALYZE is required, and the call waits for completion.
                    sql(definition)
                stats = sql("SHOW STATISTICS FOR TABLE relation_tuple;")
                candidates = [r for r in stats[1:] if r[0] == "candidate_stats"]
                if bool(candidates) != bool(columns):
                    raise RuntimeError(f"Unexpected candidate statistics in {phase}: {stats}")
                if [r for r in stats[1:] if r[0] != "candidate_stats"] != baseline_stats[1:]:
                    raise RuntimeError(f"Baseline statistics changed in {phase}")
                samples = []
                for iteration in range(6):
                    plan = plan_sql("EXPLAIN ANALYZE " + QUERY + ";")
                    sample = metrics(plan)
                    if sample["actual_rows"] != args.rows * 3 // 10:
                        raise RuntimeError(f"Wrong actual row count: {sample}")
                    if iteration:
                        samples.append({"metrics": sample, "plan": plan})
                phases[phase] = {"definition": definition, "statistics": stats, "samples": samples}
                if columns:
                    sql(f'DELETE FROM system.table_statistics WHERE "tableID" = {int(table_id)} '
                        "AND name = 'candidate_stats';", internal=True)
                    remaining = sql("SHOW STATISTICS FOR TABLE relation_tuple;")
                    if any(r[0] == "candidate_stats" for r in remaining[1:]):
                        raise RuntimeError("Candidate statistics were not removed")
                    if remaining != baseline_stats:
                        raise RuntimeError("Baseline statistics changed after candidate removal")

            version = scalar("SELECT version();")
            migration = scalar("SELECT version_num FROM schema_version;")
            commit = run(["git", "rev-parse", "HEAD"], cwd=ROOT)
            image_id = run(["docker", "inspect", "--format", "{{.Image}}", container])
            metadata = {"version": version, "migration": migration, "commit": commit,
                        "image": args.image, "image_id": image_id, "rows": args.rows,
                        "query": QUERY, "initial_statistics": initial_stats,
                        "baseline_statistics": baseline_stats, "distribution": distribution,
                        "schema": sql("SHOW CREATE TABLE relation_tuple;"), "phases": phases}
            (output / "plans.json").write_text(json.dumps(metadata, indent=2) + "\n")
            table = []
            for phase, data in phases.items():
                samples = [s["metrics"] for s in data["samples"]]
                first = samples[0]
                table.append(f"| {phase} | {first['estimated_rows']:,} | {first['actual_rows']:,} | "
                             f"{statistics.median(s['execution_ms'] for s in samples):.3f} | "
                             f"{min(s['execution_ms'] for s in samples):.3f}–"
                             f"{max(s['execution_ms'] for s in samples):.3f} | "
                             f"{' → '.join(first['operators'])} | {', '.join(first['indexes'])} |")
            report = "\n".join([
                "# CockroachDB namespace/relation/subject-relation statistics experiment", "",
                f"- Server: {version}", f"- Image: `{args.image}` (`{image_id}`)",
                f"- Checkout HEAD: `{commit}`; migration: `{migration}`.",
                f"- Host: {platform.system()} {platform.release()} {platform.machine()}.",
                f"- Dataset: {args.rows:,} distinct relationships, 60% document/viewer/user#..., "
                "10% document/editor/user#..., 30% group/member/group#member. "
                "IDs are interleaved; subject IDs use a separate subject- prefix. No caveats/expiration.",
                "- Real SpiceDB migrations/indexes; one disposable single-node container, "
                "128 MiB cache, 256 MiB SQL memory. Direct SQL, no SpiceDB request processing.",
                "- Baseline: freshly collected default statistics, including automatic index-prefix "
                "multicolumn statistics. Background table statistics collection and forecasting disabled "
                "for controlled measurement. Baseline snapshots retained unchanged across phases.",
                "- Sequence: baseline, explicit pair, explicit triple. CREATE STATISTICS completes "
                "collection before each candidate is measured; each candidate is removed afterward. "
                "One warm-up + five measured queries each. Timing is server execution time.", "",
                "```sql", QUERY + ";", "```", "", "Additional statistics tested separately:", "",
                "```sql", *(p["definition"] for p in phases.values() if p["definition"]), "```", "",
                "| Statistics | Estimated result rows | Actual result rows | Median execution (ms) | Range (ms) | Operators | Index |",
                "|---|---:|---:|---:|---:|---|---|", *table, "",
                "CockroachDB CREATE STATISTICS uses its native statistics; it does not expose "
                "PostgreSQL's MCV/dependencies switches. See plans.json for actual collected column sets, "
                "distinct counts, histogram IDs, schema, and every measured plan. These timings are preliminary "
                "and are not comparable as a PostgreSQL-versus-CockroachDB performance benchmark.", "",
                "This simple synthetic query omits SpiceDB's revision and ID filters. "
                "Statistics collection/maintenance cost is not measured.", "",
                "Reference: https://www.cockroachlabs.com/docs/stable/create-statistics", "",
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
