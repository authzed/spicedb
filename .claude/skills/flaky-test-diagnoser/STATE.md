# State Management

File-based state for the flaky-test-diagnoser run. Everything the coordinator decides is written to disk before proceeding; nothing is reconstructed from memory on resume.

## Directory Layout

Created in the current working directory at the start of every invocation:

```
flaky-diag-{run_id}/
├── state.json                        # authoritative state; written before every stage transition
├── hypotheses/
│   ├── H-001.md                      # per-hypothesis evaluation (see FORMAT.md)
│   ├── H-002.md
│   └── H-003.md
├── runs/
│   ├── run-001.log                   # individual test invocation
│   ├── run-002.log
│   └── ...
├── experiments/
│   ├── multi-run-initial.md          # N>=10 baseline
│   ├── isolation.md                  # isolation vs in-suite
│   ├── bisection-step-01.md          # ordering bisection steps
│   ├── timing-parallel-vs-serial.md
│   ├── exp-007.md                    # distinguishing experiments (one per surviving hypothesis)
│   └── rgr-H-001.md                  # red-green-red demo, if a hypothesis is promoted
├── logs/
│   ├── coordinator.jsonl             # decision audit trail
│   └── stage-transitions.jsonl       # one line per stage transition
└── report.md                         # final report; only written after termination label decided
```

`run_id` format: `YYYYMMDD-HHMMSS` using UTC, computed once at invocation start.

## state.json Schema

```json
{
  "run_id": "20260416-153022",
  "skill": "flaky-test-diagnoser",
  "created_at": "2026-04-16T15:30:22Z",
  "current_stage": "bootstrap | detect_runner | confirm_flakiness | generate_hypotheses | gather_evidence | distinguishing_experiments | rebuttal | red_green_red | report",
  "generation": 17,
  "target": {
    "test_identifier": "tests/test_foo.py::test_bar",
    "user_description": "fails ~30% of the time in CI but passes locally",
    "working_directory": "/abs/path/to/repo"
  },
  "runner": {
    "detected": "pytest",
    "version": "7.4.3",
    "single_test_command_template": "pytest {TEST} -v --tb=short",
    "multi_run_template": "for i in $(seq 1 {N}); do ... ; done",
    "parallel_config": "xdist workers=4"
  },
  "flakiness_confirmed": {
    "status": "confirmed | not_reproduced | consistently_broken",
    "experiment_id": "multi-run-initial",
    "n_runs": 10,
    "fail_rate": 0.30,
    "results": "F P F P P F P F P P"
  },
  "hypotheses": {
    "H-001": {
      "category": "ORDERING",
      "status": "active | falsified | confirmed | deferred",
      "uncertainty_level": "high | medium | low",
      "evidence_for_count": 2,
      "evidence_against_count": 1,
      "distinguishing_experiment_id": "exp-007",
      "distinguishing_experiment_verdict": "supports | contradicts | indeterminate | not_run",
      "spawn_time_iso": "2026-04-16T15:35:00Z",
      "completion_time_iso": null,
      "promoted_to_root_cause": false,
      "file_path": "hypotheses/H-001.md"
    }
  },
  "experiments": {
    "exp-007": {
      "purpose": "discriminate H-001 from H-002",
      "hypotheses_tested": ["H-001", "H-002"],
      "status": "planned | running | complete | failed",
      "n_runs": 10,
      "started_at": "...",
      "completed_at": "...",
      "file_path": "experiments/exp-007.md"
    }
  },
  "budget": {
    "max_total_runs": 200,
    "runs_consumed": 47,
    "max_wall_clock_seconds": 1800,
    "wall_clock_consumed_seconds": 312
  },
  "rebuttal_round": {
    "leader_hypothesis_id": "H-001",
    "challenger_hypothesis_id": "H-002",
    "outcome": "leader_held | leader_fell | merged | not_run"
  },
  "red_green_red": {
    "hypothesis_id": "H-001",
    "status": "not_started | red_baseline | green_fix | red_revert | complete | failed",
    "phases_complete": ["red_baseline"],
    "file_path": "experiments/rgr-H-001.md"
  },
  "invariants": {
    "coordinator_never_promoted_without_distinguishing_experiment": true,
    "all_evidence_fresh_this_session": true,
    "no_test_code_modified_except_reverted_instrumentation": true
  },
  "termination": {
    "label": "root_cause_isolated_with_repro | narrowed_to_N_hypotheses | inconclusive_after_N_runs | blocked_by_environment | null",
    "reason": "Free-text rationale consistent with the label",
    "decided_at": "2026-04-16T16:02:14Z",
    "selected_hypothesis_id": "H-001 | null",
    "surviving_hypothesis_ids": ["H-001"]
  }
}
```

## Generation counter

`generation` is incremented on every write to `state.json`. Atomic write pattern: write to `state.json.tmp`, fsync, rename to `state.json`. Partial writes are detected by comparing `generation` against per-hypothesis file timestamps.

## Stage transition log

Each stage transition appends one JSON line to `logs/stage-transitions.jsonl`:

```json
{"generation": 12, "from": "confirm_flakiness", "to": "generate_hypotheses", "timestamp": "2026-04-16T15:34:10Z", "evidence_files": ["experiments/multi-run-initial.md"], "guard_passed": true}
```

`guard_passed: true` means the transition's pre-condition (e.g. flakiness confirmed, at least 3 hypotheses exist) was validated against file contents before incrementing stage. `guard_passed: false` transitions MUST NOT occur — coordinator refuses and records `guard_failed` with the failing predicate.

## Resume protocol

If the user reinvokes the skill in a directory containing an existing `flaky-diag-{run_id}/`:

1. Read `state.json`.
2. Compute the last stage with a `guard_passed: true` transition log entry.
3. Re-verify that stage's evidence files still exist and are unchanged (sha256 stored in log).
4. Resume from the next stage — do not replay completed experiments. Trust the recorded `runs/` and `experiments/` artifacts.
5. If re-verification fails (corrupted state, missing file, sha mismatch), emit `blocked_by_environment` with the specific corruption identified — do not attempt partial recovery.

## When multiple runs exist in the same CWD

If multiple `flaky-diag-*/` directories exist:

- Default: create a new `flaky-diag-{now}/`. Do not merge.
- If the user specifies `--resume {run_id}`: resume that run only.
- Never write to an older run_id's directory during a new invocation.

## Invariants checked on every state write

Before `state.json` is written, the coordinator must verify:

- `generation` strictly increases
- Every hypothesis listed in `state.json.hypotheses` has a matching file at `hypotheses/{id}.md`
- Every experiment listed has a matching file at `experiments/{id}.md`
- No `promoted_to_root_cause: true` hypothesis exists without a completed red-green-red entry (`red_green_red.status: complete`)
- No `termination.label: root_cause_isolated_with_repro` without a `selected_hypothesis_id` and `red_green_red.status: complete`
- If `termination.label` is set, `current_stage: report` and the report file exists

A failed invariant blocks the write and escalates to `blocked_by_environment` with the failing invariant name. The coordinator does not self-recover — it reports.
