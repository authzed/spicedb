---
name: flaky-test-diagnoser
description: Systematically diagnoses why a test is flaky by running multi-run experiments, isolation tests, ordering permutations, and timing analysis. Use when the user says a test is flaky, intermittent, non-deterministic, randomly failing, passes sometimes, or asks to debug test flakiness.
---

# Flaky Test Diagnoser

Runs competing-hypothesis experiments to isolate the true root cause of a flaky test. Produces a diagnosis with reproducible evidence — or an honest inconclusive label. Never promotes a guess to "root cause."

This is a tracing skill, not a fixer. The goal is to explain **why** the test is intermittent with falsifiable evidence, not to jump into patching code.

## Workflow

1. **Bootstrap run state** — create `flaky-diag-{run_id}/` in CWD with `state.json`, `hypotheses/`, `runs/`, `experiments/`. See [STATE.md](STATE.md).
2. **Detect test runner** — identify the project's test framework and runner command from config files. See [RUNNERS.md](RUNNERS.md).
3. **Confirm flakiness** — run the target test N>=10 times in isolation, record pass/fail per run, compute fail rate. Record each run under `runs/`. See [EXPERIMENTS.md](EXPERIMENTS.md).
4. **Generate competing hypotheses** — produce >=3 deliberately different hypotheses from the 6 categories (ORDERING, TIMING, SHARED_STATE, EXTERNAL_DEPENDENCY, RESOURCE_LEAK, NON_DETERMINISM). Each hypothesis gets its own file in `hypotheses/` following the schema in [FORMAT.md](FORMAT.md). See [ANALYSIS.md](ANALYSIS.md) for category signatures.
5. **Gather evidence for AND against each hypothesis** — run the structured experiments (isolation, ordering bisection, timing, environment, code reads) and record findings as `evidence_for` / `evidence_against` entries per hypothesis. See [EXPERIMENTS.md](EXPERIMENTS.md).
6. **Design a distinguishing experiment per surviving hypothesis** — an experiment whose outcome uniquely discriminates that hypothesis from its rivals. An experiment that "merely supports" all hypotheses does NOT count. See [FORMAT.md](FORMAT.md).
7. **Run distinguishing experiments** — record outcomes under `experiments/`. Down-rank hypotheses whose distinctive prediction failed. See [GOLDEN-RULES.md](GOLDEN-RULES.md) for the falsifiability gate.
8. **Rebuttal round** — let the second-ranked hypothesis present its best rebuttal against the leader. The leader must answer with evidence, not assertion.
9. **Attempt red-green-red demonstration** — if a leader has been selected, describe the minimal fix and have the user apply it (or apply it in an experimental branch). Rerun the multi-run protocol (N>=10). Only label `root_cause_isolated_with_repro` if the red-green-red cycle succeeds.
10. **Emit structured output and diagnosis report** — each hypothesis gets a `STRUCTURED_OUTPUT_START` / `STRUCTURED_OUTPUT_END` block. Final report follows [REPORT.md](REPORT.md).

## Honest termination labels

Pick exactly one. Never invent synonyms.

| Label | Meaning | Requires |
|---|---|---|
| `root_cause_isolated_with_repro` | Single hypothesis confirmed; fix demonstrated with red-green-red across N>=10 runs | Distinguishing experiment passed; all rivals empirically falsified; fix verified |
| `narrowed_to_N_hypotheses` | Could not discriminate between N surviving hypotheses; additional probes recommended | Each surviving hypothesis has evidence_for; no distinguishing experiment available within budget |
| `inconclusive_after_N_runs` | Could not reproduce flakiness or no hypothesis gathered sufficient evidence | N>=20 runs executed; no consistent signal |
| `blocked_by_environment` | Cannot execute experiments (runner broken, missing deps, no shell access, tests require secrets) | Specific blocker identified with the exact error output |

Never use: "diagnosed", "likely root cause", "probably", "should be", "root cause found" as a terminal label without a red-green-red demo. See [GOLDEN-RULES.md](GOLDEN-RULES.md).

## Self-review checklist

Before delivering the report, verify ALL:

- [ ] State file exists at `flaky-diag-{run_id}/state.json` with `generation` >= 1
- [ ] >=3 competing hypotheses were generated, each with its own file in `hypotheses/`
- [ ] Every hypothesis has both `evidence_for` AND `evidence_against` entries (never empty)
- [ ] Every hypothesis has an explicit `uncertainty_level` in {high, medium, low}
- [ ] Every hypothesis has a `distinguishing_experiment` field (or is explicitly marked "no distinguisher available within budget")
- [ ] Flakiness was confirmed: at least one fail AND at least one pass in N>=10 runs
- [ ] Isolation vs in-suite comparison completed (both were run)
- [ ] If a hypothesis is promoted to `root_cause_isolated_with_repro`: a red-green-red demonstration exists with N>=10 post-fix runs (all green)
- [ ] Report uses one of the 4 honest termination labels — never "diagnosed" without red-green-red
- [ ] Each hypothesis evaluation has a `STRUCTURED_OUTPUT_START` / `STRUCTURED_OUTPUT_END` block per [FORMAT.md](FORMAT.md)
- [ ] Anti-rationalization counter-table was consulted before finalizing the label ([GOLDEN-RULES.md](GOLDEN-RULES.md))

## Golden rules

Hard rules. See [GOLDEN-RULES.md](GOLDEN-RULES.md) for the full text, anti-rationalization counter-table, and the falsifiability gate.

1. **Never promote a hypothesis to root cause without a distinguishing experiment** that empirically rejects all rivals.
2. **"Diagnosed" requires red-green-red.** Test fails → fix applied → test passes deterministically across N>=10 runs. Without it, the label is `narrowed_to_N_hypotheses`.
3. **Always run isolation before ordering.** If the test fails in isolation, ordering analysis is irrelevant.
4. **Bisect, never brute-force** when searching for an interfering test.
5. **Capture exact commands verbatim.** Every experiment logs its shell command so the user can reproduce it.
6. **Minimum 10 runs for any statistical claim.** Flaky tests can have fail rates under 10%.
7. **Never modify test code during diagnosis** beyond temporary instrumentation that is reverted before the report is emitted.
8. **Evidence for AND against every hypothesis.** A hypothesis with only supporting evidence is unfalsifiable and must be flagged.
9. **Coordinator does not self-approve.** The hypothesis ranking is based on experiment outcomes recorded in files, not coordinator opinion.
10. **Structured output is the contract.** Per-hypothesis evaluations live between `STRUCTURED_OUTPUT_START`/`STRUCTURED_OUTPUT_END` markers; free-text commentary is ignored when computing the verdict.

## Cancellation / resume

If interrupted mid-run: on next invocation read `flaky-diag-{run_id}/state.json`, identify the highest-`generation` completed stage, and replay from the next stage. Never recompute completed experiments — trust the recorded run artifacts. See [STATE.md](STATE.md) for the replay protocol.

## Reference files

| File | Contents |
|------|----------|
| [RUNNERS.md](RUNNERS.md) | Test runner detection, command templates for pytest/jest/junit/go test/cargo test, and how to target a single test |
| [EXPERIMENTS.md](EXPERIMENTS.md) | Multi-run protocol, isolation protocol, ordering bisection algorithm, timing instrumentation, red-green-red demo protocol |
| [ANALYSIS.md](ANALYSIS.md) | Root cause categories, code pattern matching for flakiness signals, environment factor checklist, decision matrix |
| [FORMAT.md](FORMAT.md) | Per-hypothesis evaluation schema, `STRUCTURED_OUTPUT_START` markers, distinguishing-experiment template |
| [STATE.md](STATE.md) | `state.json` schema, per-stage state layout, resume protocol |
| [GOLDEN-RULES.md](GOLDEN-RULES.md) | Full golden rules text, falsifiability gate, anti-rationalization counter-table |
| [REPORT.md](REPORT.md) | Diagnosis report output template with honest termination labels |
