# Diagnosis Report Format

The exact output structure for the final diagnosis report. The report is synthesized from the `STRUCTURED_OUTPUT` blocks of every hypothesis file plus the honest termination label in `state.json`.

Written to `flaky-diag-{run_id}/report.md` and returned inline to the user.

## Report Template

```markdown
# Flaky Test Diagnosis Report

**Test:** [full test identifier — file::class::method or file > describe > it]
**Runner:** [detected test runner and version]
**Run ID:** `flaky-diag-{run_id}`
**Date:** [current date]

## Termination Label

`root_cause_isolated_with_repro | narrowed_to_N_hypotheses | inconclusive_after_N_runs | blocked_by_environment`

[One-sentence rationale consistent with the label.]

## Verdict

[For `root_cause_isolated_with_repro`:]

**Confirmed root cause:** [H-{id} — category — short name]
**Selected hypothesis:** [H-{id}]
**Red-green-red demonstrated:** yes
**Fail rate (baseline):** [X]% across [N] runs
**Fail rate (post-fix):** 0% across [N] runs

[For `narrowed_to_N_hypotheses`:]

**Surviving hypotheses:** [list of hypothesis IDs]
**Reason unable to discriminate:** [one of: distinguisher not available within budget | distinguisher outcome indeterminate | competing hypotheses predict same outcome]
**Recommended next probes:** [list of concrete next experiments]

[For `inconclusive_after_N_runs`:]

**Runs executed:** [N]
**Reason:** [flakiness not reproduced | every hypothesis falsified without replacement]
**Recommended next step:** [specific additional context needed — CI logs, environment details, larger run budget]

[For `blocked_by_environment`:]

**Blocker:** [specific blocker]
**Error output:** [captured error]
**Remediation suggested:** [what user must provide to unblock]

## Competing Hypotheses

| ID | Category | Status | Uncertainty | Distinguishing exp | Verdict |
|----|----------|--------|-------------|--------------------|---------|
| H-001 | ORDERING | confirmed | low | exp-007 | supports |
| H-002 | TIMING | falsified | low | exp-007 | contradicts |
| H-003 | SHARED_STATE | falsified | low | exp-008 | contradicts |

## Evidence Summary

### H-001: [short name] — [category] — status: [confirmed/active/falsified]

**Evidence For:**
- [evidence 1 with source | strength]
- [evidence 2 with source | strength]

**Evidence Against:**
- [evidence 1 with source | strength]

**Distinguishing experiment:** [exp_id]
- Procedure: [command]
- Predicted if true: [observable]
- Predicted if false: [observable]
- Observed: [observable]
- Verdict: supports | contradicts | indeterminate

[Repeat block for every hypothesis with `status` in {confirmed, active, falsified}. Omit `deferred` hypotheses unless they carry unique context.]

## Experiments Executed

### Multi-Run (baseline)

**Command:** `[exact command]`
**Runs:** [N]
**Results:** [P F P P F P P P P P]
**Fail rate:** [X]%

### Isolation

**Isolated (5 runs):** [P P P P P] — fail rate: [X]%
**In-suite (5 runs):** [P F P F P] — fail rate: [X]%
**Conclusion:** [ordering-dependent | not ordering-dependent | inconclusive]

### Ordering Bisection

[Include only if isolation showed ordering-dependent behavior.]

**Suite size:** [N] tests before target
**Bisection steps:** [N]
**Interfering test:** [full test identifier]
**Confirmation:** [INTERFERER + TARGET ran 5 times: F P F F P — confirmed]

### Timing Analysis

**Parallel mode (5 runs):** [results] — fail rate: [X]%
**Serial mode (5 runs):** [results] — fail rate: [X]%
**Timing variance:**
- Setup: median=[X]ms, max=[Y]ms ([Z]x variance)
- Test body: median=[X]ms, max=[Y]ms ([Z]x variance)
- Teardown: median=[X]ms, max=[Y]ms ([Z]x variance)

### Environment Analysis

| Factor | Finding |
|--------|---------|
| Parallelism config | [e.g., "4 workers via pytest-xdist"] |
| External calls in test | [e.g., "None detected" or "HTTP call to api.example.com"] |
| Shared state signals | [e.g., "Module-level `_cache` dict mutated by 3 tests"] |
| Resource cleanup | [e.g., "DB connection opened in setUp, not closed in tearDown"] |
| CI vs local divergence | [e.g., "User reports passes locally; fails ~30% in CI"] |

### Distinguishing Experiments

[Per-experiment block listing the exp_id, purpose, procedure, hypothesis-outcome table, observed outcome, and per-hypothesis verdicts. Link to `experiments/{exp_id}.md`.]

## Red-Green-Red Demonstration

[Include only if `termination.label == root_cause_isolated_with_repro`.]

**Hypothesis:** H-{id} — [category]

### Phase 1 — RED (baseline)
- Commit: [sha]
- N=[N], results: [P F sequence], fail rate: [X]%

### Phase 2 — GREEN (fix applied)
- Files changed: [list with line ranges]
- Diff summary: [2-5 line summary of fix]
- N=[N], results: [P sequence, all P], fail rate: 0%

### Phase 3 — RED (revert)
- N=[N], results: [P F sequence with failures reproduced], fail rate: [X]%

Fix reapplied after Phase 3. Repository left in green state.

## Code Analysis

**Flakiness signals found in test code (tier-4 evidence unless backed by experiment):**
- `[file:line]` — [signal description — e.g., "time.sleep(0.1) used to wait for async operation"]
- `[file:line]` — [signal description]

## Recommended Fix

[Populate fully if `termination.label == root_cause_isolated_with_repro` or `narrowed_to_N_hypotheses` with a leading hypothesis.]

### Root cause explanation

[1-2 paragraphs explaining the exact mechanism causing flakiness. Reference specific code locations and the confirmed hypothesis.]

### Fix

[Specific code change with file path and line numbers.]

**Before (the problematic pattern):**
```
[3-10 lines showing the current code]
```

**After (the fix):**
```
[3-10 lines showing the corrected code]
```

### Verification

After applying the fix, run:

```
[exact command to run the test N times]
```

Expected result: 0 failures in [N] runs.

## Anti-Rationalization Check

Before emitting this report, the coordinator confirmed:
- [ ] No "probably" or "likely" promoted to root cause — only `status: confirmed` with a distinguishing experiment
- [ ] No label upgraded without required evidence (e.g. `root_cause_isolated_with_repro` has a completed red-green-red)
- [ ] No hypothesis with only evidence_for (every hypothesis has both for AND against or is marked falsified)
- [ ] All hypotheses referenced in `state.json` have corresponding files in `hypotheses/`
- [ ] The counter-table in GOLDEN-RULES.md was consulted

## Artifacts

All experiment data is preserved at:
- `flaky-diag-{run_id}/state.json` — authoritative state
- `flaky-diag-{run_id}/hypotheses/` — per-hypothesis evaluations
- `flaky-diag-{run_id}/runs/` — per-run logs
- `flaky-diag-{run_id}/experiments/` — per-experiment records
- `flaky-diag-{run_id}/logs/coordinator.jsonl` — decision audit trail

Resume this run with `--resume {run_id}`. See [STATE.md](STATE.md).
```

---

## Section Rules

### Required sections (all labels)

- Termination Label
- Verdict
- Competing Hypotheses
- Evidence Summary
- Experiments Executed
- Anti-Rationalization Check
- Artifacts

### Conditional sections

- **Red-Green-Red Demonstration**: only for `root_cause_isolated_with_repro`
- **Recommended Fix**: populated fully only for `root_cause_isolated_with_repro` or `narrowed_to_N_hypotheses` with a clear leading hypothesis; for other labels, emit "deferred until additional evidence" with explicit next-probe list
- **Ordering Bisection**: only if isolation showed ordering-dependent behavior

### Forbidden phrases

Never emit these phrases in the report:
- "probably the root cause"
- "likely caused by"
- "appears to be"
- "seems to be"
- "diagnosed" (without red-green-red)
- "CI flake"
- "transient failure"

Instead, use:
- "confirmed by [experiment]" (when backed by a distinguisher with `supports`)
- "hypothesis H-{id} ranked first with [N] tier-{N} evidence items"
- "unable to discriminate between H-{a} and H-{b} within budget; recommend [specific probe]"

### Report length targets

- Termination Label + Verdict: 5-15 lines
- Competing Hypotheses table: 3-8 rows
- Per-hypothesis Evidence Summary: 8-15 lines each
- Experiments section: 10-40 lines
- Recommended Fix: 15-40 lines (when populated)
- Total report: 120-300 lines

---

## When experiments are inconclusive

If no single hypothesis reaches `status: confirmed` with a distinguishing experiment:

```markdown
## Termination Label

`narrowed_to_N_hypotheses`

The investigation narrowed the cause to {N} surviving hypotheses that could not be discriminated within the session budget.

## Verdict

**Surviving hypotheses:**
1. H-{id} — [category] — [why it survives] — [what would falsify it]
2. H-{id} — [category] — [why it survives] — [what would falsify it]

**Reason no leader emerged:** [specific reason — distinguisher not available | budget exhausted | indeterminate outcome]

**Recommended next probes:**
1. [specific experiment with exact command and prediction]
2. [specific experiment with exact command and prediction]
```

Never fabricate a root cause when evidence is insufficient. `narrowed_to_N_hypotheses` with a concrete probe list is a valid, honest termination.

---

## Report emission invariants

Before writing `report.md`, verify:

1. Every hypothesis referenced in the report has a `STRUCTURED_OUTPUT` block at `hypotheses/{id}.md`.
2. The termination label matches the state in `state.json`.
3. No forbidden phrase appears in the report (grep the output before emission).
4. If the label is `root_cause_isolated_with_repro`, the Red-Green-Red section is fully populated with N>=10 runs in each phase.
5. If the label is `narrowed_to_N_hypotheses`, the Recommended next probes list has >=1 concrete entry with an exact command.

Failed invariant → refuse to emit. Set `termination.label` to `blocked_by_environment` with the invariant name and retry after remediation.
