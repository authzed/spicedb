import copy
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

import history
import run_suite


def artifact():
    sample = dict(NSPerOp=1000, BytesPerOp=20, AllocsPerOp=1, Iterations=2)
    engine = dict(
        Name="classic", Decision={"Outcome": "allow"}, Work=[{"Events": []}], Samples=[sample] * 10, Preparation=sample
    )
    qp = copy.deepcopy(engine)
    qp["Name"] = "qp"
    return dict(
        Version=1,
        Created="2026-09-29T00:00:00Z",
        Policy={"Version": 1},
        Repetitions=1,
        Samples=10,
        Provenance={
            "commit": "test",
            "dirty": "",
            "go": "go1",
            "os": "test",
            "arch": "test",
            "cpu_model": "cpu",
            "gomaxprocs": "1",
            "classic": "serial",
            "qp": "baseline",
            "gogc": "100",
            "gomemlimit": "off",
            "godebug": "",
            "build_settings": "{}",
        },
        Inputs={},
        Omissions=[],
        Results=[
            dict(
                Dataset={
                    "ID": "fixture",
                    "Family": "test",
                    "Hash": "abc",
                    "Relationships": 1,
                    "Resources": 1,
                    "Subjects": 1,
                    "InputBytes": 20,
                    "SchemaBytes": 1,
                },
                Case={
                    "ID": "hit",
                    "Query": {"ResourceID": "d"},
                    "Expected": {"Outcome": "allow"},
                    "ClassicDepth": 50,
                    "QPDepth": 50,
                },
                Profile="memdb",
                Valid=True,
                Status="relationship work matched",
                Differences=[],
                Engines=[engine, qp],
            )
        ],
    )


def write_artifact(root, data=None):
    source = root / "input.json"
    source.write_text(json.dumps(data if data is not None else artifact()))
    return source


def keys_differ(a, b):
    return history.comparison_key(a, a["rows"][0]) != history.comparison_key(b, b["rows"][0])


def test_archive_is_immutable_and_checksums_detect_tampering(tmp_path):
    source = write_artifact(tmp_path)
    run = history.archive(tmp_path / "history", "first", "First run", [source])
    assert (run / "report.html").exists()
    history.verify(run)
    original = (run / "report.html").read_bytes()
    with pytest.raises(FileExistsError):
        history.archive(tmp_path / "history", "first", "Overwrite", [source])
    assert (run / "report.html").read_bytes() == original
    (run / "report.html").write_text("changed")
    with pytest.raises(ValueError):
        history.verify(run)


def test_comparison_identity_covers_input_request_policy_and_machine():
    a = history.merge_inputs([artifact()])
    assert not keys_differ(a, copy.deepcopy(a))
    for field in ["Hash", "ID"]:
        b = copy.deepcopy(a)
        b["rows"][0]["dataset"][field] = "changed"
        assert keys_differ(a, b)
    for section, field in [
        ("policy", "Version"),
        ("provenance", "cpu_model"),
        ("provenance", "qp"),
        ("provenance", "gogc"),
        ("provenance", "build_settings"),
        ("provenance", "schema_mode"),
        ("provenance", "schema_cache"),
    ]:
        b = copy.deepcopy(a)
        b[section][field] = "changed"
        assert keys_differ(a, b)
    b = copy.deepcopy(a)
    b["rows"][0]["case"]["Query"]["ResourceID"] = "other"
    assert keys_differ(a, b)


def test_merge_rejects_duplicate_cases_and_conflicting_source():
    a = artifact()
    with pytest.raises(ValueError):
        history.merge_inputs([a, a])
    b = copy.deepcopy(a)
    b["Results"][0]["Dataset"]["ID"] = "other"
    b["Provenance"]["commit"] = "other"
    with pytest.raises(ValueError):
        history.merge_inputs([a, b])


@pytest.mark.parametrize("change", ["invalid", "partial"])
def test_invalid_or_partial_measurements_cannot_publish(change):
    a = artifact()
    if change == "invalid":
        a["Results"][0]["Valid"] = False
    else:
        a["Results"][0]["Engines"][0]["Samples"] = []
    with pytest.raises(ValueError):
        history.merge_inputs([a])


@pytest.mark.parametrize("field", ["cpu_model", "gogc", "gomemlimit", "build_settings"])
def test_unknown_cpu_or_runtime_settings_withhold_comparison_keys(field):
    data = history.merge_inputs([artifact()])
    data["provenance"].pop(field)
    assert history.comparison_key(data, data["rows"][0]) is None


def test_exclusive_run_lock_rejects_second_owner(tmp_path):
    path = tmp_path / "run.lock"
    with history.file_lock(path, blocking=False):
        with pytest.raises(BlockingIOError):
            with history.file_lock(path, blocking=False):
                pass
    with history.file_lock(path, blocking=False):
        pass


def test_interrupted_log_is_not_overwritten(tmp_path):
    log = tmp_path / "000.log"
    log.write_text("interrupted attempt evidence")
    with pytest.raises(FileExistsError):
        run_suite.ensure_new_shard(tmp_path / "000.json", tmp_path / "000.json.gz", log)
    assert log.read_text() == "interrupted attempt evidence"


def test_concurrent_index_rebuilds_are_serialized(tmp_path):
    history.archive(tmp_path / "history", "first", "First", [write_artifact(tmp_path)])
    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(lambda _: history.rebuild(tmp_path / "history"), range(4)))
    assert all(len(r) == 1 for r in results)
    assert "First" in (tmp_path / "history" / "index.html").read_text()


@pytest.mark.parametrize("settings", ["", "{}", '{"shared_buffers":"512MB"}'])
def test_incomplete_postgres_configuration_cannot_publish_or_compare(settings):
    a = artifact()
    a["Provenance"].update(backend="postgres", backend_version="17.2", backend_settings=settings)
    assert not history.configuration_known(history.render.compact(a))
    with pytest.raises(ValueError):
        history.merge_inputs([a])


def test_schema_loads_are_separate_and_unrecorded_history_is_unknown():
    a = artifact()
    for engine in a["Results"][0]["Engines"]:
        engine["Work"][0]["Events"] = [dict(Operation="schema-load")]
    assert history.render.compact(a)["rows"][0]["engines"][0]["metrics"]["schemaLoads"] is None
    a["Provenance"]["schema_load_instrumentation"] = "datastore reader v1"
    metrics = history.render.compact(a)["rows"][0]["engines"][0]["metrics"]
    assert metrics["schemaLoads"]["median"] == 1
    assert metrics["schema"]["median"] == 0


def test_sql_metadata_and_combined_chart_preserve_size_dimensions(tmp_path):
    a = artifact()
    settings = {
        k: "recorded"
        for k in (
            "shared_buffers",
            "work_mem",
            "effective_cache_size",
            "fsync",
            "autovacuum",
            "jit",
            "plan_cache_mode",
            "enable_indexscan",
            "enable_indexonlyscan",
            "enable_bitmapscan",
            "enable_seqscan",
            "pool_read_max",
            "pool_write_max",
            "cache_policy",
            "preparation",
        )
    }
    settings["container"] = json.dumps(
        dict(
            image="sha256:test",
            architecture="arm64",
            dockerVersion="29",
            transport="loopback",
            cpus=4,
            memoryBytes=100,
            vmCPUs=10,
            vmMemoryBytes=200,
        )
    )
    a["Provenance"].update(backend="postgres", backend_version="17.2", backend_settings=json.dumps(settings))
    a["Results"][0]["Profile"] = "postgres"
    a["Results"][0]["Dataset"]["Database"] = {
        "TableBytes": 8192,
        "IndexBytes": 16384,
        "DatabaseBytes": 100000,
        "RelationshipRows": 1,
    }
    row = history.summary(history.merge_inputs([a]))["rows"][0]
    assert row["inputBytes"] == 20
    assert row["family"] == "test"
    assert row["database"]["TableBytes"] == 8192
    history.archive(tmp_path / "history", "postgres", "Postgres", [write_artifact(tmp_path, a)])
    html = (tmp_path / "history" / "index.html").read_text()
    assert 'id="combinedChart"' in html
    assert 'id="graphProfile"' in html


def test_run_ids_cannot_escape_history_directory(tmp_path):
    with pytest.raises(ValueError):
        history.archive(tmp_path, "../escape", "bad", [])
