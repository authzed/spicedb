# Golden Rules

Hard rules, the falsifiability gate, and the anti-rationalization counter-table. Consulted before finalizing any termination label.

## The 10 Golden Rules

1. **Never promote a hypothesis to root cause without a distinguishing experiment.** The experiment must be one whose outcome differs between the leading hypothesis and every surviving rival. An experiment that "supports everything" is not a distinguisher. Label it `narrowed_to_N_hypotheses` instead.

2. **"Diagnosed" requires red-green-red.** To claim `root_cause_isolated_with_repro` you MUST demonstrate: (a) RED baseline with failing runs, (b) GREEN post-fix with N>=10 deterministic passes, (c) RED revert reproducing the original failure. Two out of three is not enough. See [FORMAT.md](FORMAT.md) for the demo schema.

3. **Always run isolation before ordering.** If the test fails in isolation, ordering analysis is irrelevant. Skip to timing and environment analysis.

4. **Bisect, never brute-force.** Binary bisection of the test suite — cut the search space in half each iteration. Never linear elimination.

5. **Capture exact commands verbatim.** Every experiment logs its shell command verbatim so the user can reproduce it. Never paraphrase.

6. **Minimum 10 runs for any statistical claim.** Flaky tests can have fail rates under 10%. Never claim "passes reliably" with fewer than 10 runs.

7. **Never modify test code during diagnosis** beyond temporary instrumentation that is reverted before the report is delivered. Instrumentation must not change test semantics.

8. **Evidence FOR and AGAINST every hypothesis.** A hypothesis with only supporting evidence is unfalsifiable — the framework cannot rule it out or in. Flag it as `uncertainty_level: high` and design a distinguishing experiment that would contradict it if wrong.

9. **Coordinator does not self-approve.** The ranking and promotion decisions are computed from the `STRUCTURED_OUTPUT` blocks of every hypothesis file — not from the coordinator's narrative. The coordinator orchestrates and records; it never evaluates its own conclusions.

10. **Structured output is the contract.** Per-hypothesis verdicts live between `STRUCTURED_OUTPUT_START`/`STRUCTURED_OUTPUT_END` markers. Free-text is ignored when computing the final label. Unparseable → treat as `uncertainty_level: high, promoted_to_root_cause: false`.

---

## The Falsifiability Gate

A hypothesis passes the gate and may be promoted to `root_cause_isolated_with_repro` **only if all four conditions hold**:

1. It has at least one `evidence_for` entry at strength `strong` or `moderate` (not circumstantial-only).
2. It has a completed `distinguishing_experiment` whose observed outcome matches the "predicted if TRUE" branch.
3. Every surviving rival hypothesis has that same experiment's outcome recorded in its file as `contradicts` (the observed outcome matches the rival's "predicted if FALSE" branch).
4. A red-green-red demonstration has been completed with Phase 2 (green) showing 0% fail rate across N>=10 runs.

If any condition fails, the label cannot be `root_cause_isolated_with_repro`. Downgrade to `narrowed_to_N_hypotheses` with the surviving hypothesis list.

A hypothesis also passes the gate **negatively** (gets falsified) if its distinguishing experiment outcome matches the "predicted if FALSE" branch. Mark `status: falsified` — do not keep reasoning about it.

---

## Anti-Rationalization Counter-Table

Consult this table every time the coordinator is tempted to declare done, skip a stage, or soften a label. Each row is an excuse-pattern observed in prior flaky-test investigations that led to wrong diagnoses.

| Excuse the coordinator is tempted to make | Reality check | Required action |
|---|---|---|
| "Tests probably pass on retry — let's just rerun and move on." | Retry is not a fix. A test that fails 30% is broken; retry only masks it. | Run N>=10. Record the fail rate. Do not propose retry as a remedy. |
| "This is just a CI flake — the code is fine." | "CI flake" is a label for an unidentified bug. Flakiness originates from shared state, timing, or non-determinism regardless of host. | Apply the 6-category decision matrix. Do not exit with "CI flake" as a verdict. |
| "The timeout was too short — bump it and ship." | Longer timeout masks timing hypotheses; it does not falsify them. A flaky test at 10s timeout is still flaky at 60s timeout with lower frequency. | Run at the original timeout. Identify the operation whose variance exceeds the window. |
| "Let's just rerun it — probably transient." | Probably ≠ evidence. Transient is a pattern, not a cause. | Require distinguishing experiment. No shipping on "probably." |
| "Good enough for now — ordering dependency is obvious." | Obvious ≠ confirmed. Bisection often finds a different interferer than the one suspected. | Run the bisection; record the confirmed interferer name. |
| "Race detector didn't flag it, so no race." | Race detectors catch data races, not higher-level ordering races (transactions, file locks, shared caches). | Do not use absence of race-detector output as evidence against TIMING hypotheses. |
| "Passes locally 10/10 — must be CI-specific." | Local-CI divergence is a hypothesis (EXTERNAL_DEPENDENCY or environment), not a conclusion. | Log the environment diff (parallelism, workers, network, env vars) and make it a hypothesis with evidence. |
| "One bisection pass is enough." | Bisection of a flaky test can itself flake — a negative step at 3 runs is noise-compatible. | Run each bisection step N>=3 times; require >=2 confirmations before narrowing. |
| "Timing analysis showed variance, therefore TIMING is the cause." | Timing variance is consistent with TIMING, SHARED_STATE, and RESOURCE_LEAK. Variance alone doesn't discriminate. | Design a distinguisher (e.g. parallel-vs-serial) that separates these three. |
| "We found the first hypothesis that fits — done." | First-fit is confirmation bias. A distinguishing experiment often falsifies a plausible leader. | Run the distinguisher even when the hypothesis "feels right." |

Each row is a trapdoor. If the coordinator's narrative starts matching the "Excuse" column, stop, read the "Required action" column, and do that instead.

---

## Termination Label Rules

Pick exactly one — never invent a synonym, never mix labels.

### `root_cause_isolated_with_repro`
**Requires:**
- Exactly one hypothesis at `status: confirmed, promoted_to_root_cause: true`
- All other hypotheses at `status: falsified` with contradicting distinguishing-experiment outcomes
- Red-green-red demonstration complete (all 3 phases, Phase 2 at 0% fail rate across N>=10)
- `state.json.invariants.no_test_code_modified_except_reverted_instrumentation: true`

**Forbidden combinations:** multiple confirmed hypotheses; any `active` hypothesis; missing red-green-red.

### `narrowed_to_N_hypotheses`
**Requires:**
- N >= 1 hypotheses at `status: active`, each with evidence_for at strength moderate or better
- Insufficient distinguishing experiments to falsify all-but-one
- A list of recommended next probes included in the report

**Forbidden:** claiming this label when the user has not been given a list of concrete next experiments.

### `inconclusive_after_N_runs`
**Requires:**
- Total runs consumed >= 20
- No hypothesis reached `status: confirmed`
- Multi-run protocol did not establish a clear fail rate OR every generated hypothesis was falsified without a replacement

**Forbidden:** using this label when fewer than 20 runs were executed. Increase budget first.

### `blocked_by_environment`
**Requires:**
- Specific blocker identified: missing dependency, failing runner, missing secret, permission denied, network unreachable, etc.
- Exact error output captured in `logs/coordinator.jsonl`
- No remediation attempted beyond documenting the blocker

**Forbidden:** using this label when the blocker was caused by the diagnosis itself (e.g. our instrumentation broke the runner). In that case, revert instrumentation and retry.

---

## Invariants checked before emitting the report

Every one of these must pass. The coordinator refuses to emit a report until all pass.

1. `state.json.generation` increased monotonically throughout the run (no out-of-order writes).
2. Every hypothesis referenced in `state.json.hypotheses` has a file at `hypotheses/{id}.md` with a parseable `STRUCTURED_OUTPUT` block.
3. Every experiment referenced has a file at `experiments/{id}.md`.
4. The termination label matches the file invariants (see per-label requirements above).
5. No hypothesis is marked `promoted_to_root_cause: true` without a completed red-green-red demo.
6. No hypothesis is marked `status: confirmed` without its distinguishing experiment recording a `supports` verdict.
7. The anti-rationalization counter-table was consulted (recorded in `logs/coordinator.jsonl` with `event: "counter_table_consulted"`).
8. If `status: confirmed` was assigned: every other hypothesis either has `status: falsified` with a `contradicts` experiment verdict, or an explicit note explaining why it was deferred.

Failed invariant → coordinator emits `blocked_by_environment` with the invariant name, not the intended label.
