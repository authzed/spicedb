# Experiment Protocols

Step-by-step protocols for each experiment phase. Each protocol feeds evidence into one or more hypotheses per the schema in [FORMAT.md](FORMAT.md).

## Contents
- Multi-run protocol (confirm flakiness)
- Isolation protocol (alone vs in-suite)
- Ordering bisection protocol (find interfering test)
- Timing analysis protocol (race conditions, timeout sensitivity)
- Environment analysis protocol (parallelism, external deps)
- Distinguishing experiment protocol (per-hypothesis discriminators)
- Red-green-red demonstration protocol (mandatory for `root_cause_isolated_with_repro`)

Every protocol writes results to `flaky-diag-{run_id}/experiments/{exp_id}.md` and individual run logs to `flaky-diag-{run_id}/runs/run-{NNN}.log`. Experiments attach `hypotheses_tested: [H-001, H-002, ...]` metadata so outcomes flow back into the hypothesis files.

---

## Multi-Run Protocol

**Goal:** Confirm the test is flaky and measure its fail rate. This is the baseline for every downstream hypothesis.

### Steps

1. Construct the single-test run command from [RUNNERS.md](RUNNERS.md) for the detected runner.
2. Run the test N=10 times using the loop template. Append one `runs/run-{NNN}.log` per invocation with the full `COMMAND`, `EXIT`, `DURATION_MS`, and `STDOUT_TAIL_20`/`STDERR_TAIL_20` fields (see [FORMAT.md](FORMAT.md) run record format).
3. Parse exits: `EXIT: 0` = pass; non-zero = fail.
4. Compute fail rate: `fail_count / total_runs * 100`.
5. Write `experiments/multi-run-initial.md` with the `exp_id: multi-run-initial`, `n_runs`, and `results` fields.
6. Set `state.json.flakiness_confirmed` accordingly.

### Decision tree

| Result | Next step | State impact |
|---|---|---|
| 0 failures in 10 runs | Increase to N=20. If still 0 failures, set `flakiness_confirmed.status: not_reproduced`. Ask the user for the original failing CI log so we can record the environment delta. | Candidate termination: `inconclusive_after_N_runs` or `blocked_by_environment` if user can't provide logs. |
| 1-3 failures | Flakiness confirmed. Proceed to hypothesis generation + isolation. | `flakiness_confirmed.status: confirmed` |
| 4-7 failures | Highly flaky. Proceed. | same |
| 8-10 failures | Likely a real bug. Inform user; still proceed to isolation — consistently broken can masquerade as flaky when ordering is involved. | `flakiness_confirmed.status: consistently_broken` |

### Evidence flow

The multi-run outcome feeds the **fail-rate baseline** that every later experiment compares against. Record it in every hypothesis's `Evidence For` section if the hypothesis predicts the observed fail-rate, and `Evidence Against` if the hypothesis predicts a different rate.

---

## Isolation Protocol

**Goal:** Determine if the test fails on its own or only when other tests run first. This cleanly separates ORDERING / SHARED_STATE hypotheses from TIMING / NON_DETERMINISM / EXTERNAL_DEPENDENCY hypotheses.

### Steps

1. **Isolated run:** Run the target test alone 5 times using the single-test command.
2. **In-suite run:** Run the full test suite (or the test file) 5 times; extract the target test's pass/fail from each run.
3. Write `experiments/isolation.md` with both result sequences.

### Decision tree

| Isolated | In-suite | Diagnosis | Hypotheses affected |
|---|---|---|---|
| Always passes | Sometimes fails | Ordering-dependent | Strengthens ORDERING, SHARED_STATE; weakens TIMING, NON_DETERMINISM |
| Sometimes fails | Sometimes fails | Not ordering-dependent | Weakens ORDERING; keeps TIMING, SHARED_STATE, NON_DETERMINISM, EXTERNAL_DEPENDENCY in play |
| Sometimes fails | Always passes | Unusual — possible RESOURCE_LEAK from the test itself in isolation, or in-suite parallelism changing timing | Strengthens RESOURCE_LEAK; flag for careful timing analysis |
| Always fails | Always fails | Not flaky — consistently broken | Exit with `flakiness_confirmed.status: consistently_broken`; report as a real bug |

Update every hypothesis file's `Evidence For` / `Evidence Against` section with the isolation outcome.

---

## Ordering Bisection Protocol

**Goal:** Find the specific test(s) that, when run before the target, cause it to fail.

**Prerequisites:** Isolation protocol showed target passes alone and fails in-suite.

### Steps

1. **List all tests** in the runner's natural order:
   - pytest: `pytest --co -q`
   - Jest: `npx jest --listTests`
   - go test: `go test -list ".*" ./PACKAGE/`
   - Gradle: `./gradlew test --tests "*" --dry-run`
2. **Find target position.** All tests before it are candidates.
3. **Binary bisection** — for each bisection step:
   - Split candidates into FIRST_HALF and SECOND_HALF.
   - Run `FIRST_HALF + TARGET` 3 times. If target fails >=2 times → interferer in FIRST_HALF.
   - Run `SECOND_HALF + TARGET` 3 times. If target fails >=2 times → interferer in SECOND_HALF.
   - Recurse on the failing half.
4. **Confirm** by running `INTERFERER + TARGET` 5 times. If target fails >=2 times → interferer confirmed.
5. Write each bisection step to `experiments/bisection-step-NN.md`. Final confirmation goes to `experiments/bisection-confirmed.md`.

### Flaky-test-inside-bisection guard

Bisection itself can flake. Require >=2 of 3 runs at each step to classify a half as "interfering." A single failure is not evidence — it is noise-compatible with the baseline fail rate. See anti-rationalization row #8 in [GOLDEN-RULES.md](GOLDEN-RULES.md).

### Bisection command construction

- **pytest:** `pytest TEST_A TEST_B ... TARGET -v --tb=short`
- **Jest:** `npx jest FILE_A FILE_B TARGET_FILE --verbose --runInBand`
- **go test:** `go test -run "TestA|TestB|Target" -v ./pkg/`
- **Gradle:** `./gradlew test --tests "A" --tests "B" --tests "TARGET" --info`

For runners that don't support arbitrary ordering, run the subset as the entire suite using test filters.

### Evidence flow

A confirmed interferer becomes `evidence_for` at strength `strong` in the ORDERING hypothesis, and `evidence_against` at strength `strong` in TIMING / NON_DETERMINISM (which would not be explained by ordering).

---

## Timing Analysis Protocol

**Goal:** Detect race conditions, timeout sensitivity, and slow operations that cause intermittent failures.

### Steps

1. **Timed runs:** Run the target test 5 times with verbose/duration output:
   - pytest: `--durations=0 --setup-show`
   - Jest: `--verbose`
   - go test: `-v`
2. **Record timing per phase** (setup / test body / teardown) in `experiments/timing.md`.
3. **Analyze variance** — flag:
   - Setup >10x median → slow resource initialization
   - Test body >5x median on failing runs → timeout or blocking call
   - Teardown spike → cleanup race
4. **Parallel vs serial** — run with parallelism disabled:
   - pytest: `-p no:xdist` or `--forked`
   - Jest: `--runInBand`
   - go test: `-parallel 1`
   - Vitest: `--pool=forks --poolOptions.forks.singleFork`
5. **Language-specific race detection**:
   - Go: `go test -race`
   - Python: check for shared mutable module-level variables
   - Java: check for `static` mutable fields in test classes; consider `@DirtiesContext` on Spring tests

### Evidence flow

- Fail rate drops to 0% under `--runInBand` or `-parallel 1` → strong evidence for SHARED_STATE.
- Fail rate unchanged under serial → weakens SHARED_STATE, keeps TIMING and NON_DETERMINISM in play.
- Race detector flags a data race → strong evidence for TIMING.
- Absence of race-detector output → NOT evidence against TIMING (see counter-table row #6).

---

## Environment Analysis Protocol

**Goal:** Surface environment factors that feed EXTERNAL_DEPENDENCY, RESOURCE_LEAK, and NON_DETERMINISM hypotheses.

### Steps

1. Walk the checklist in [ANALYSIS.md](ANALYSIS.md) "Environment Factor Checklist."
2. For each factor, record a finding in `experiments/environment.md`:
   - Parallelism config (workers, forks)
   - CI vs local divergence (ask the user)
   - Docker/container limits
   - Test database config and cleanup strategy
   - External URLs in test code
   - Filesystem operations
   - Time-dependent assertions (timezone, clock)
   - Memory limits
3. For each finding, update the relevant hypothesis file with evidence_for/against.

### Read the test code

1. Read the target test, its fixtures, and its setup/teardown methods. Track every shared resource, external call, time-dependent assertion, and non-deterministic input.
2. Read any interfering test identified by bisection.
3. Record findings as `Evidence For` entries in the relevant hypothesis files, citing `code:{file}:{line}` as the source.

The code-pattern signals in [ANALYSIS.md](ANALYSIS.md) are the grep shopping list.

---

## Distinguishing Experiment Protocol

**Goal:** Produce an experiment whose outcome uniquely separates the leading hypothesis from each surviving rival. This is the bar the falsifiability gate checks (see [GOLDEN-RULES.md](GOLDEN-RULES.md)).

### Steps

1. For the top 2-3 hypotheses after multi-run + isolation + ordering + timing, design a distinguisher following the template in [FORMAT.md](FORMAT.md).
2. The experiment must produce **mutually exclusive observable outcomes** for the hypotheses it tests. If the same outcome is compatible with multiple hypotheses, the experiment is NOT distinguishing — redesign it.
3. Write `experiments/exp-NNN.md` with the hypothesis-outcome table BEFORE running.
4. Run the experiment. Record the observed outcome.
5. For each hypothesis, record the verdict in that hypothesis's `Experiment Outcomes` table as `supports` / `contradicts` / `indeterminate`.
6. Update `STRUCTURED_OUTPUT` block: set `distinguishing_experiment_verdict` for the hypothesis; set `status: falsified` for any hypothesis whose outcome came back `contradicts`.

### Canonical distinguishers

| Rival pair | Distinguisher |
|---|---|
| ORDERING vs SHARED_STATE | Run target after a known-unrelated test (should still fail if SHARED_STATE via parallelism; should pass if purely ORDERING) |
| TIMING vs SHARED_STATE | Run with `--runInBand` / `-parallel 1`. TIMING persists; SHARED_STATE disappears. |
| TIMING vs NON_DETERMINISM | Run with artificially slow sleep injected between setup and test. TIMING fail rate drops or rises; NON_DETERMINISM unchanged. |
| SHARED_STATE vs RESOURCE_LEAK | Run N=30 single-test runs. SHARED_STATE fail rate stays constant; RESOURCE_LEAK rate increases over runs. |
| EXTERNAL_DEPENDENCY vs RESOURCE_LEAK | Disconnect network (or block the specific URL). EXTERNAL_DEPENDENCY fails every run; RESOURCE_LEAK unaffected. |
| ORDERING vs NON_DETERMINISM | Run the target alone N=20 times. ORDERING fail rate is 0%; NON_DETERMINISM rate matches in-suite rate. |

Add a distinguisher to `experiments/` before declaring a leader.

### If no distinguisher is available

Mark the hypothesis's `distinguishing_experiment_id` as `no_distinguisher_available` with a rationale. This blocks promotion to `root_cause_isolated_with_repro` and forces the label to `narrowed_to_N_hypotheses`.

---

## Red-Green-Red Demonstration Protocol

**Goal:** Prove a hypothesis is the root cause by producing a fix that deterministically eliminates the failure. Required for `root_cause_isolated_with_repro`.

### Prerequisites

- One hypothesis has `status: confirmed` with a distinguishing experiment recording `supports`.
- All surviving rivals have `contradicts` verdicts from the same distinguisher.
- The user has approved applying a fix (or has asked the skill to operate in an experimental branch).

### Phase 1 — RED (pre-fix baseline)

1. Record the branch/commit SHA.
2. Run N>=10 invocations of the single-test command.
3. Verify the fail rate is >= 10%. If lower, increase N until it is — the flakiness must be observable before attempting a fix.
4. Log to `runs/red-001` through `runs/red-NNN`.

### Phase 2 — GREEN (fix applied)

1. Apply the minimal fix implied by the confirmed hypothesis:
   - ORDERING → add teardown or reset fixture
   - TIMING → replace sleep with condition-based wait; add missing await; add synchronization primitive
   - SHARED_STATE → isolate per-test (unique port, per-test transaction, separate tmp dir)
   - EXTERNAL_DEPENDENCY → mock or inject
   - RESOURCE_LEAK → add cleanup in teardown/finally
   - NON_DETERMINISM → seed random generator, sort before compare, use ordered data structures
2. Record files changed and the diff in `experiments/rgr-{hypothesis_id}.md`.
3. Run N>=10 invocations of the single-test command.
4. Verify **all N runs pass** — fail rate MUST be exactly 0%. Any failure means the fix is incomplete; do NOT promote.
5. Log to `runs/green-001` through `runs/green-NNN`.

### Phase 3 — RED (revert and rerun)

1. Revert the diff from Phase 2 (apply its reverse).
2. Run N>=10 invocations of the single-test command.
3. Verify failure rate reproduces within 2x of Phase 1's rate. If Phase 3 does not reproduce, the original observation may have been transient — downgrade the label to `inconclusive_after_N_runs`.
4. Log to `runs/red2-001` through `runs/red2-NNN`.

### Record

Write the full `experiments/rgr-{hypothesis_id}.md` per the schema in [FORMAT.md](FORMAT.md). Set `hypotheses/{id}.md` `STRUCTURED_OUTPUT.red_green_red_completed: true` and `promoted_to_root_cause: true`.

### Reapply the fix

After Phase 3, reapply the fix (re-apply the Phase 2 diff) so the repository is left in the green state. Record this in `logs/coordinator.jsonl` as `event: "fix_reapplied"`.

---

## Resume-safe execution

Every protocol writes its incremental progress to `runs/` and `experiments/` before advancing. On resume (see [STATE.md](STATE.md)), completed runs are trusted — do not re-execute. Only replay from the last incomplete protocol step.
