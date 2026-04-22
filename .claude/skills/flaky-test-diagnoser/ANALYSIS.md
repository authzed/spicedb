# Root Cause Analysis

How to classify hypotheses into the 6 categories, what code patterns indicate each category, how to check environment factors, and the decision matrix for choosing an initial hypothesis set.

## Contents
- Root cause categories (the 6 hypothesis buckets)
- Code pattern signals (grep shopping list per category)
- Environment factor checklist
- Decision matrix for initial hypothesis generation

---

## Root Cause Categories

Every hypothesis is classified into exactly one of these 6 categories. The coordinator generates >=3 hypotheses spanning different categories during step 4 of the workflow.

### 1. ORDERING — Test depends on execution order

**Signature:** Passes in isolation, fails in-suite. Bisection identifies a specific interfering test.
**Mechanism:** A prior test modifies shared state (database rows, module-level variables, singleton instances, environment variables, filesystem) that the target test assumes is clean.
**Common evidence:**
- Interfering test writes to a database/cache without cleanup
- Module-level variable mutated by interfering test
- Environment variable set by interfering test and not restored
- Temp files created by interfering test not cleaned up

**Canonical distinguisher:** Bisection finding a confirmed interferer (>=2/3 runs fail with INTERFERER + TARGET).
**Typical fix:** Add setup/teardown that resets shared state. Make the target independent of assumed initial state.

### 2. TIMING — Race condition or timeout sensitivity

**Signature:** Fails in both isolation and in-suite; fail rate changes with parallelism or system load; failing runs show significantly longer execution times.
**Mechanism:** A race condition (concurrent operations observed in wrong order) or a time-window assumption violated under load.
**Common evidence:**
- `sleep()` or `setTimeout()` used to wait for async operations
- Assertions on time-sensitive values (timestamps, durations)
- Missing `await` on async operations
- Polling with fixed timeout instead of condition-based waiting
- Go race detector reports data races

**Canonical distinguisher:** Fails under `--runInBand` / `-parallel 1` (TIMING persists); fail rate changes when injecting a sleep between setup and test body.
**Typical fix:** Replace sleeps with condition-based waits. Add missing `await`. Use deterministic synchronization (mutexes, channels, promises).

### 3. SHARED_STATE — Shared mutable state without isolation

**Signature:** Fails in both isolation and in-suite, but serial execution (no parallelism) makes it pass reliably.
**Mechanism:** Multiple test threads/processes access the same mutable resource (singleton, shared database, shared port, shared file) without synchronization.
**Common evidence:**
- Static/global mutable variables in test setup
- Hard-coded port numbers in tests
- Shared database without per-test transactions or cleanup
- Shared temp directory without per-test namespacing

**Canonical distinguisher:** Fail rate drops to 0% under `--runInBand` / `-parallel 1`.
**Typical fix:** Isolate shared resources per test (unique ports, per-test transactions, per-test tmp dirs). Fallback: force serial execution for the affected suite.

### 4. EXTERNAL_DEPENDENCY — Test relies on an external service

**Signature:** Fail rate varies across environments (local vs CI). Failures correlate with network availability or external service status.
**Mechanism:** The test calls a real external service (API, DNS, NTP, database) instead of a mock/stub.
**Common evidence:**
- HTTP calls to external URLs in test code (not mocked)
- DNS resolution in tests
- `time.Now()` / `Date.now()` with assertions on specific values
- Dependency on system locale or timezone

**Canonical distinguisher:** Block the specific URL (iptables / hosts file) — EXTERNAL_DEPENDENCY fails 100%; other hypotheses unaffected.
**Typical fix:** Mock external calls. Use fixed/injected time sources. Use test containers for database dependencies.

### 5. RESOURCE_LEAK — Test leaks resources across runs

**Signature:** First N runs pass, then failures start. Fail rate increases over time within a batch.
**Mechanism:** Test allocates resources (file handles, connections, threads, memory) not released, eventually hitting system limits.
**Common evidence:**
- File handles opened but not closed
- Database connections acquired but not released
- Goroutines/threads started but not joined/stopped
- Increasing memory usage across runs
- "too many open files" or "connection refused" errors in later runs

**Canonical distinguisher:** N=30 sequential single-test runs — RESOURCE_LEAK fail rate monotonically increases; SHARED_STATE rate is flat.
**Typical fix:** Add resource cleanup in teardown/finally. Use context managers (Python), try-with-resources (Java), `defer` (Go). Verify cleanup runs on test failure.

### 6. NON_DETERMINISM — Test output depends on non-deterministic input

**Signature:** Fails in isolation at a consistent rate regardless of ordering or parallelism. No timing variance between pass/fail runs.
**Mechanism:** Test depends on random values, hash map iteration order, filesystem readdir order, or other non-determinism sources.
**Common evidence:**
- `Math.random()`, `random.random()`, `rand.Int()` without seeding
- HashMap/dict iteration used to assert order
- Filesystem listing (`readdir`, `os.listdir`, `glob`) used to assert order
- UUID generation used in assertions
- Floating point comparison without epsilon

**Canonical distinguisher:** Same fail rate in isolation as in-suite, same fail rate parallel vs serial, no timing variance between pass/fail runs.
**Typical fix:** Seed random generators. Sort before comparing collections. Use ordered data structures. Use approximate float comparison.

---

## Code Pattern Signals

The grep shopping list when reading the test code (workflow step 7). Matches at the listed paths become `evidence_for` entries in the relevant hypothesis files at strength `single-source code-path inference` (tier 4) unless backed by an experiment.

### ORDERING signals
- Grep: `setUp`, `tearDown`, `beforeAll`, `afterAll`, `beforeEach`, `afterEach`, `@Before`, `@After`, `fixture`
- Check: does teardown reset ALL state that setup creates?
- Check: module-level variables mutated by tests?

### TIMING signals
- Grep: `sleep`, `setTimeout`, `time.Sleep`, `Thread.sleep`, `wait_for`, `asyncio.sleep`
- Grep: `await` missing before async calls (audit all `async` function calls)
- Grep: `timeout`, `deadline`, `Duration`
- Check: polling loop with fixed timeout?

### SHARED_STATE signals
- Grep: `static`, `global`, `module-level`, `singleton`
- Grep: hard-coded ports (`8080`, `3000`, `5432`, `27017`)
- Grep: shared file paths (`/tmp/test`, fixed filenames)
- Check: shared database without rollback?

### EXTERNAL_DEPENDENCY signals
- Grep: `http://`, `https://` in test files (real URLs, not mocks)
- Grep: `requests.get`, `fetch(`, `http.Get` in tests
- Grep: `Date.now`, `time.time`, `Instant.now`, `System.currentTimeMillis` in assertions
- Check: test doubles/mocks for all external calls?

### RESOURCE_LEAK signals
- Grep: `open(` without context manager (Python); `new FileInputStream` without try-with-resources (Java)
- Grep: connections created in setup; check closed in teardown
- Grep: goroutines started (`go func`) without shutdown
- Check: error path cleans up resources?

### NON_DETERMINISM signals
- Grep: `random`, `rand`, `Math.random`, `uuid`, `UUID`
- Grep: `HashMap`, `dict`, `map` used in order-sensitive assertions
- Grep: `os.listdir`, `readdir`, `glob` used in assertions
- Check: collection comparisons order-dependent?

---

## Environment Factor Checklist

Run through this list during the Environment Analysis Protocol ([EXPERIMENTS.md](EXPERIMENTS.md)). Each factor either feeds one hypothesis category or falsifies another.

| Factor | How to check | Feeds hypothesis |
|---|---|---|
| Parallelism | Read runner config for `workers`, `forks`, `parallel`, `--jobs` | SHARED_STATE, TIMING |
| CI vs local | Ask user if flakiness differs across environments | EXTERNAL_DEPENDENCY, environment delta |
| Docker/container | `Dockerfile`, `docker-compose.test.yml` | Resource limits, networking |
| Database | Test DB config, migrations, seeding | SHARED_STATE, ORDERING |
| Network | Grep test code for external URLs | EXTERNAL_DEPENDENCY |
| Filesystem | Grep for file operations in tests | ORDERING, RESOURCE_LEAK |
| Time | Grep for time-dependent assertions | TIMING, EXTERNAL_DEPENDENCY |
| Memory | Runner memory limits | RESOURCE_LEAK |
| Test framework upgrades | `git log` on test config | Environment delta — can invalidate prior baseline |
| Retry policy | Grep for `@Retry`, `rerunFailingTestsCount`, `--retries` | Masking prior flakiness |

---

## Decision Matrix for Initial Hypothesis Generation

When generating the initial >=3 competing hypotheses (step 4 of the workflow), use this matrix to seed at least one hypothesis per signal tier. The goal is to **deliberately generate different hypotheses**, not the same explanation repeated.

| Experiment signal | Initial hypothesis to seed |
|---|---|
| Passes alone, fails in-suite, bisection narrows | ORDERING |
| Passes alone, fails in-suite, no bisection leader | SHARED_STATE (indirect via parallelism) |
| Fails alone AND in-suite, serial fixes it | SHARED_STATE (direct) |
| Fails alone AND in-suite, serial does not fix, high timing variance | TIMING |
| Fails alone AND in-suite, low timing variance, consistent fail rate | NON_DETERMINISM |
| Fail rate increases with run index | RESOURCE_LEAK |
| Fail rate differs across environments | EXTERNAL_DEPENDENCY |

### Required spread

The initial hypothesis set MUST include at least one hypothesis from each of the following super-groups:

1. **State-based**: ORDERING or SHARED_STATE
2. **Time-based**: TIMING or RESOURCE_LEAK
3. **Input-based**: NON_DETERMINISM or EXTERNAL_DEPENDENCY

This prevents the coordinator from anchoring on a single category when evidence is still thin. If the signals strongly point to only one super-group, still generate a strawman hypothesis from another super-group so the distinguishing experiment has something to falsify.

### Down-ranking and merging

After experiments run, down-rank a hypothesis when:
- Direct evidence contradicts it (distinguisher says `contradicts`)
- It survives only by adding unverified assumptions
- It makes no distinctive prediction vs rivals
- A rival explains the same facts with fewer assumptions
- Its support is mostly tiers 5-6 (weak circumstantial / intuition) while a rival has tiers 1-3

Merge two hypotheses if they reduce to the same underlying mechanism AND predict the same outcomes for every planned distinguisher. Record the merge in the surviving hypothesis file's `Rebuttal Round` section with `outcome: merged_with_rival`.

Keep two hypotheses separate if they imply different next probes, even when they sound similar — different probes = different discriminators.

---

## When multiple categories match

If a hypothesis fits two categories (e.g. SHARED_STATE + TIMING), prefer the one with the strongest experimental evidence. Generate both as separate hypotheses initially — the distinguishing experiment will separate them. Only merge after the experiment confirms they predict the same outcomes.

## When no category fits

If no hypothesis can be seeded from the signals, mark the run `blocked_by_environment` and ask the user for: (a) the CI log of a failing run, (b) the full test file, (c) the test fixtures. Do not fabricate a category to fill the slot.
