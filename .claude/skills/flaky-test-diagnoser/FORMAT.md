# Output Formats

Per-hypothesis evaluation schema, structured-output markers, distinguishing-experiment template, and the final report hand-off shape.

## Contents
- Per-hypothesis evaluation file
- Structured output block (required for every hypothesis)
- Distinguishing experiment template
- Run record format
- Red-green-red demonstration record

---

## Per-Hypothesis Evaluation File

Each hypothesis gets its own file at `flaky-diag-{run_id}/hypotheses/{hypothesis_id}.md`. `hypothesis_id` is `H-{NNN}` (three-digit, zero-padded, allocated in discovery order).

```markdown
# {hypothesis_id}: {short name}

**Category:** ORDERING | TIMING | SHARED_STATE | EXTERNAL_DEPENDENCY | RESOURCE_LEAK | NON_DETERMINISM
**Status:** active | falsified | confirmed | deferred
**Uncertainty level:** high | medium | low
**Created generation:** {integer, matches state.json.generation}
**Last updated generation:** {integer}

## Hypothesis statement

One to three sentences stating the proposed mechanism for the flakiness. Be specific: name the suspected shared resource, timing window, interfering test, or non-determinism source if possible. A vague hypothesis ("maybe a race") is rejected — sharpen it before recording.

## Evidence For

- `{evidence_id}` | `{source: runs/run-003.log OR code:{file}:{line} OR experiments/{exp_id}.md}` | {what this evidence shows} | strength: {strong | moderate | weak | circumstantial}

[At least one entry required. Pure speculation is not evidence. If a hypothesis has no evidence_for after the initial experiment pass, mark it `falsified` and move on.]

## Evidence Against

- `{evidence_id}` | `{source}` | {what contradicts the hypothesis} | strength: {strong | moderate | weak | circumstantial}

[At least one entry required. A hypothesis with zero evidence_against is unfalsifiable — flag as `uncertainty_level: high` and design a distinguishing experiment that would produce contradicting evidence if the hypothesis is wrong. A hypothesis that cannot be contradicted by any observable experiment is not a scientific hypothesis; mark `deferred` with rationale.]

## Distinguishing Experiment

**Experiment id:** `{exp_id}` or `no_distinguisher_available`
**Procedure:** {exact shell command(s) or procedure}
**Prediction if hypothesis is TRUE:** {specific observable outcome}
**Prediction if hypothesis is FALSE:** {specific observable outcome, must differ from TRUE outcome}
**Rivals this distinguishes from:** [H-002, H-003]
**Budget estimate:** {wall-clock seconds, runs needed}

The experiment MUST produce different observable outcomes for this hypothesis vs every listed rival. If the experiment's outcome is compatible with multiple hypotheses, it is not distinguishing — redesign it before running.

If no distinguisher is available within budget, write `no_distinguisher_available` and include a rationale in a `Notes` section. This forces the termination label toward `narrowed_to_N_hypotheses` — never `root_cause_isolated_with_repro`.

## Experiment Outcomes

| Run | Exp id | Command | Predicted (if true) | Observed | Verdict |
|-----|--------|---------|--------------------|---------|---------|
| 1 | exp-007 | `pytest test_foo.py -v --forked` | fail rate 0% | fail rate 0% (10 runs) | supports |
| 2 | exp-008 | `pytest test_foo.py -v` | fail rate >20% | fail rate 30% (10 runs) | supports |

Verdict values: `supports` | `contradicts` | `indeterminate`. Indeterminate outcomes are not evidence — do not count them toward confirmation or falsification.

## Rebuttal Round

If this hypothesis was the leader and a rival presented a rebuttal:

- **Rival:** {hypothesis_id}
- **Rebuttal:** {rival's best argument, one paragraph}
- **Leader response:** {evidence-backed reply, not assertion}
- **Outcome:** leader_held | leader_fell_to_rival | merged_with_rival

## Red-Green-Red Demo

[Only populated if this hypothesis was promoted to `root_cause_isolated_with_repro`.]

- **Fix description:** {what code changed, file:line}
- **Pre-fix multi-run:** N={n}, results: `{P F sequence}`, fail rate: {%}
- **Post-fix multi-run:** N={n}, results: `{P sequence, all P required}`, fail rate: 0%
- **Revert and rerun:** N={n}, results: `{P F sequence with fails reproduced}`, fail rate: {%}

All three phases required for the `root_cause_isolated_with_repro` label. Post-fix phase MUST be N>=10 runs, all passes. If post-fix shows any fail, the fix is incomplete — do NOT promote.

---

STRUCTURED_OUTPUT_START
hypothesis_id: {H-NNN}
category: {ORDERING | TIMING | SHARED_STATE | EXTERNAL_DEPENDENCY | RESOURCE_LEAK | NON_DETERMINISM}
status: {active | falsified | confirmed | deferred}
uncertainty_level: {high | medium | low}
evidence_for_count: {integer}
evidence_against_count: {integer}
distinguishing_experiment_id: {exp_id | no_distinguisher_available}
distinguishing_experiment_verdict: {supports | contradicts | indeterminate | not_run}
rebuttal_outcome: {leader_held | leader_fell_to_rival | merged_with_rival | no_rebuttal}
red_green_red_completed: {true | false}
promoted_to_root_cause: {true | false}
STRUCTURED_OUTPUT_END
```

The `STRUCTURED_OUTPUT_START`/`STRUCTURED_OUTPUT_END` block is the machine-readable contract. The coordinator synthesizes the final verdict by reading these blocks from every hypothesis file — free-text commentary is ignored. Unparseable blocks are treated as `status: active, uncertainty_level: high, promoted_to_root_cause: false`.

## Evidence strength hierarchy

Rank evidence by tier, not by rhetoric. From strongest to weakest:

1. **Controlled reproduction or distinguishing experiment** — an experiment whose outcome differs between this hypothesis and every rival
2. **Primary artifact with tight provenance** — run log, race-detector output, bisection isolation, timing percentile
3. **Multiple independent sources converging** — code pattern + experiment outcome + environment factor agree
4. **Single-source code-path inference** — greppable pattern without experimental confirmation
5. **Weak circumstantial** — naming, stack order, resemblance to prior bugs
6. **Intuition / analogy** — unranked; does not count as evidence

Down-rank hypotheses whose support is mostly tiers 5-6 when a rival has tiers 1-2 evidence.

---

## Distinguishing Experiment Template

Fill this before running. If you cannot fill every field, the experiment is not a distinguisher yet.

```markdown
# {exp_id}: {short name}

**Purpose:** Discriminate {H-001: TIMING} from {H-002: SHARED_STATE}
**Procedure:**
1. {exact step with shell command}
2. {exact step}
3. Record outcome in `experiments/{exp_id}.md` with per-run pass/fail

**Hypothesis-outcome table:**

| Hypothesis | Predicted outcome if this hypothesis is true |
|---|---|
| H-001 TIMING | Serial run (pool=1) still shows fail rate >10% |
| H-002 SHARED_STATE | Serial run shows fail rate 0% (shared state is absent with single worker) |

The outcomes MUST be mutually exclusive observable states. If they overlap, redesign.

**Runs executed:** {N}
**Raw result:** {exact pass/fail sequence}
**Verdict for H-001:** {supports | contradicts | indeterminate}
**Verdict for H-002:** {supports | contradicts | indeterminate}
**Wall clock:** {seconds}
```

---

## Run Record Format

Each individual test invocation during multi-run / isolation / bisection / timing is recorded at `flaky-diag-{run_id}/runs/run-{NNN}.log`:

```
COMMAND: pytest tests/test_foo.py::test_bar -v --tb=short
STARTED_AT: 2026-04-16T15:30:22Z
RUN_INDEX: 3
EXP_ID: multi-run-initial
EXIT: 1
DURATION_MS: 2034
STDOUT_TAIL_20: <last 20 lines>
STDERR_TAIL_20: <last 20 lines>
```

Aggregation scripts parse `EXIT: 0` (pass) vs `EXIT: [^0]` (fail). Do not paraphrase the command — store it verbatim so the user can reproduce.

---

## Red-Green-Red Demonstration Record

Stored at `flaky-diag-{run_id}/experiments/rgr-{hypothesis_id}.md`:

```markdown
# Red-Green-Red: {hypothesis_id}

## Phase 1: RED (pre-fix baseline)
- Branch / commit: {sha or "main"}
- Multi-run: N=10, results: `F P F P P F P F P P`, fail rate: 40%
- Run log dir: `runs/red-001` through `runs/red-010`

## Phase 2: GREEN (fix applied)
- Fix: {short description}
- Files changed: `{file}:{line range}`
- Diff: {unified diff or path to patch file}
- Multi-run: N=10, results: `P P P P P P P P P P`, fail rate: 0%
- Run log dir: `runs/green-001` through `runs/green-010`

## Phase 3: RED (revert and rerun)
- Revert: applied reverse of Phase 2 diff
- Multi-run: N=10, results: `F P P F P F P P F P`, fail rate: 40%
- Run log dir: `runs/red2-001` through `runs/red2-010`

## Verdict

- Phase 1 fail rate: {x}%  (>= 10% required)
- Phase 2 fail rate: 0%    (MUST be exactly 0%)
- Phase 3 fail rate: {y}%  (must reproduce within 2x of Phase 1)

If all three conditions hold, the fix is confirmed. Promote the hypothesis to `confirmed`; set `promoted_to_root_cause: true` in its structured output.

If Phase 2 shows any failure, the fix is incomplete — do NOT promote. Return to hypothesis generation.
If Phase 3 fails to reproduce the original flakiness, the original observation may itself have been transient — downgrade the label to `inconclusive_after_N_runs`.
```

---

## Final Report Output

The top-level report emitted at the end of the run follows [REPORT.md](REPORT.md). It is generated by synthesizing the `STRUCTURED_OUTPUT` blocks from every hypothesis file plus the honest termination label from `state.json`.
