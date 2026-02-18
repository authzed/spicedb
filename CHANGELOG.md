# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]
### Added
- feat(query planner): add recursive direction strategies, and fix IS BFS (https://github.com/authzed/spicedb/pull/2891)
- feat(query planner): introduce query plan outlines and canonicalization (https://github.com/authzed/spicedb/pull/2901)
- Schema v2: introduces support for PostOrder traversal in walk.go (https://github.com/authzed/spicedb/pull/2761) and improve PostOrder walker cycle detection (https://github.com/authzed/spicedb/pull/2902)

### Changed
- Begin deprecation of library "github.com/dlmiddlecote/sqlstats" (https://github.com/authzed/spicedb/pull/2904).
  NOTE: in a future release, MySQL metrics will change.
- Add support for imports and partials to the schemadsl package that drives the LSP and development server (https://github.com/authzed/spicedb/pull/2919).
- `DatastoreTester.New` now takes a `testing.TB` as its first argument, allowing per-test cleanup in datastore test suites (https://github.com/authzed/spicedb/pull/2925).
- Added support for CRDB 26.1 by fixing how version information is read from the cluster

### Fixed
- enforce graceful shutdown on serve and serve-testing (https://github.com/authzed/spicedb/pull/2888)
- Spanner metrics regression (https://github.com/authzed/spicedb/pull/2329)
- improve streaming dispatch logging and observability (https://github.com/authzed/spicedb/pull/2915)

## [1.49.1] - 2026-02-06
### Added
- chore: add metrics and tests to all cache implementations by @miparnisari in https://github.com/authzed/spicedb/pull/2874
- feat(query planner): finish LR consistency tests with the fix to the recursive iterator by @barakmich in https://github.com/authzed/spicedb/pull/2881

### Changed
- chore: implement self in schemav2 by @tstirrat15 in https://github.com/authzed/spicedb/pull/2887

### Fixed
- fix: prevent panic on malformed cursor by @tstirrat15 in https://github.com/authzed/spicedb/pull/2878
- fix(query planner): update IterSubjects for wildcards and Alias iterators for confomance by @barakmich in https://github.com/authzed/spicedb/pull/2864
- fix(query planner): improve LR consistency and support multiple resourcetypes by @barakmich in https://github.com/authzed/spicedb/pull/2875
- fix(query planner): query both subrelation and ellipses on arrows for IterResources by @barakmich in https://github.com/authzed/spicedb/pull/2879
- fix: handle `self` keyword in warnings checks, and check these warnings are error-free in consistency by @barakmich in https://github.com/authzed/spicedb/pull/2884
- fix: make sure that use self comes out of formatter when self is used by @tstirrat15 in https://github.com/authzed/spicedb/pull/2885

### Security
- A fix for a [low-severity GHSA](https://github.com/authzed/spicedb/security/advisories/GHSA-vhvq-fv9f-wh4q) in [#2878](https://github.com/authzed/spicedb/issues/2878)

## [1.49.0] - 2026-02-03
### Added
- Support for `self` keyword added to permissions.
  Previously, if you wanted to represent something like "a user should be able to view themselves," this required adding a relation to the schema and then writing a relation from the user to itself. We've added support for a `self` keyword in permissions that represents this directly, which reduces storage requirements, removes the need for a trip to the database, and removes a relationship that needs to be synced. For more information, see [the Docs](https://authzed.com/docs/spicedb/concepts/schema#the-self-keyword) and the [PR](https://github.com/authzed/spicedb/pull/2785)
- Experimental Postgres Foreign Data Wrapper.

  In [#2806](https://github.com/authzed/spicedb/issues/2806), we added a new experimental command to SpiceDB that serves a [Postgres Foreign Data Wrapper](https://wiki.postgresql.org/wiki/Foreign_data_wrappers): `spicedb postgres-fdw [flags]`.
  If you configure your Postgres instance accordingly, it can speak to SpiceDB through the FDW as a proxy, allowing you to write queries like:

  ```sql
  -- Check if user:alice has permission to view document:readme
  SELECT has_permission
  FROM permissions
  WHERE resource_type = 'document'
    AND resource_id = 'readme'
    AND permission = 'view'
    AND subject_type = 'user'
    AND subject_id = 'alice';
  ```

  You can now express checks and lookups as `SELECT`s and `JOIN`s in your main application code, and you can read, write, and delete relationships using Postgres as the client.
  For more information, see the [documentation](https://github.com/authzed/spicedb/tree/main/internal/fdw) in the repo.

  NOTE:
  * This feature is experimental. We'd welcome you trying it out and providing feedback, but it will likely change before its final GA'd form.
  * This feature DOES NOT solve the [Dual-Write Problem](https://authzed.com/blog/the-dual-write-problem). You *can* make updates in the context of a Postgres transaction, but Postgres's FDW protocol doesn't support a two-phase commit semantic, which means there are still failure modes where a transactional write will land in SpiceDB but not Postgres or vice-versa.
- Query Planner

  This release includes the first experimental handle on our new [Query Planner](https://authzed.com/blog/introducing-spicedb-query-planner). If you run SpiceDB with the new `--experimental-query-plan` flag, SpiceDB will use the query planner to resolve queries.
  This is mostly provided for the curious; there's still work to do on statistics sources and optimizations before we expect that it will provide performance benefits across most workloads.
  We don't yet recommend turning on this flag in your system outside of experiments in your local or development environments. We'll continue work and let you know when it's ready for production.

  #### Changed
- A fix for cockroach's connection pooler where the pooler won't report itself as ready until all connections are ready to be used: [#2766](https://github.com/authzed/spicedb/issues/2766)
- A fix for a segfault when providing datastore bootstrap files with caveats in them: [#2784](https://github.com/authzed/spicedb/issues/2784)
- Touching an existing relationship and providing an empty expiration field will now clear an existing expiration value in CRDB and MySQL: [#2796](https://github.com/authzed/spicedb/issues/2796)
- A fix for lexing Unicode characters in string literals in schemas: [#2836](https://github.com/authzed/spicedb/issues/2836)
- We've deprecated datastore hedging, as it didn't provide performance gains and led to a less stable system: [#2819](https://github.com/authzed/spicedb/issues/2819)
- There's a new `--datastore-watch-change-buffer-maximum-size` flag for the Watch API that determines how many changes SpiceDB will buffer in memory before it emits an error. This protects against OOMkills when the backing datastore fails to produce a checkpoint: [#2859](https://github.com/authzed/spicedb/issues/2859)

## [1.48.0] - 2025-12-09
### Added
- feat: add Memory Protection Middleware (enabled by default, use `--enable-memory-protection-middleware=false` to disable) by @miparnisari in https://github.com/authzed/spicedb/pull/2691.
  ⚠️ Now, if your server's memory usage is too high, incoming requests may be rejected with code "ResourceExhausted" (HTTP 429).

### Changed
- use FAILED_PRECONDITION for recursion depth errors by @tstirrat15 in https://github.com/authzed/spicedb/pull/2729
- docs: improve description of some flags by @miparnisari in https://github.com/authzed/spicedb/pull/2692
- Updated Go to 1.25.5 by @tstirrat15 in https://github.com/authzed/spicedb/pull/2740

### Fixed
- expose x-request-id header in HTTP Gateway responses by @Verolop in https://github.com/authzed/spicedb/pull/2712
- error message when cannot run 'datastore gc' or 'datastore repair' by @miparnisari in https://github.com/authzed/spicedb/pull/2609
- Postgres:
    * wire up missing revision timestamp on PG ReadWriteTx by [@vroldanbet](https://authzed.slack.com/team/U03HU4QUZU3) in https://github.com/authzed/spicedb/pull/2725
- Spanner:
    * Watch API by @miparnisari in https://github.com/authzed/spicedb/pull/2560
    * statistics by @miparnisari in https://github.com/authzed/spicedb/pull/2745

## [1.47.1] - 2025-11-20
### Changed
- Performance improvements for `WriteSchema` in [#2697](https://github.com/authzed/spicedb/issues/2697) and for `ReadRelationships` in [#2632](https://github.com/authzed/spicedb/issues/2632)
- disable tracing of health check requests by @ivanauth in https://github.com/authzed/spicedb/pull/2614

### Fixed
- do not warn if requestid middleware errors due to `ErrIllegalHeaderWrite` by @miparnisari in https://github.com/authzed/spicedb/pull/2654
- Spanner: "concurrent write to map error" in Watch API by @miparnisari in https://github.com/authzed/spicedb/pull/2694
- Postgres: set missing fields in postgresRevision.MarshalBinary by @ostafen in https://github.com/authzed/spicedb/pull/2708
- Postgres & MySQL: duplicate metrics error with read replicas (#2518) by @miparnisari in https://github.com/authzed/spicedb/pull/2707

### Security
- [CVE Fix](https://github.com/authzed/spicedb/security/advisories/GHSA-9m7r-g8hg-x3vr): Fixed a bug that would result in missing resources in `LookupResources` when certain permission structures are present (`Check`s were unaffected)
- Upgrade Go to latest version to fix CVE by @tstirrat15 in https://github.com/authzed/spicedb/pull/2671

## [1.46.2] - 2025-10-24
### Added
- relationship expiration is now on by default by @miparnisari in https://github.com/authzed/spicedb/pull/2605
- add man page generation support by @ivanauth in https://github.com/authzed/spicedb/pull/2595
- add fgprof wall-clock profiler by @vroldanbet in https://github.com/authzed/spicedb/pull/2618
- CRDB: add write backpressure when write pool is overloaded  by @ecordell in https://github.com/authzed/spicedb/pull/2642
    * ⚠️ With this change, Write APIs now return ResourceExhausted errors if there are no available connections in the pool

### Changed
- perf: significant improvements around LR3 dispatching by @josephschorr in https://github.com/authzed/spicedb/pull/2587
- CRDB: move off experimental changefeed query by @miparnisari in https://github.com/authzed/spicedb/pull/2617

### Fixed
- properly rewrite errors for watch api by @miparnisari in https://github.com/authzed/spicedb/pull/2640

## [1.46.0] - 2025-10-06
### Changed
- update telemetry guide by @emmanuel-ferdman in https://github.com/authzed/spicedb/pull/2567
- perf: add trait filtering support to read relationships by @josephschorr in https://github.com/authzed/spicedb/pull/2572
- metrics: register logical checks metric by default by @jzelinskie in https://github.com/authzed/spicedb/pull/2575
- docs: Update README.md by @sohanmaheshwar in https://github.com/authzed/spicedb/pull/2586

### Fixed
- LR3 Fixes and Improvements by @josephschorr in https://github.com/authzed/spicedb/pull/2570 and https://github.com/authzed/spicedb/pull/2574
- propagate cancellation errors in consistency middleware by @tstirrat15 in https://github.com/authzed/spicedb/pull/2581
- breakage of gRPC retries by @vroldanbet in https://github.com/authzed/spicedb/pull/2577
    *  ⚠️ With this change, if you use the `zed` CLI, you must update to the latest version ([v0.33.0](https://github.com/authzed/zed/releases/tag/v0.33.0))
- fix: add flags to configure how to handle zedtokens meant for a different datastore by @josephschorr in https://github.com/authzed/spicedb/pull/1723

## [1.45.4] - 2025-09-12
### Added
- LookupResources v3, based on a new cursored iterator library by @josephschorr in https://github.com/authzed/spicedb/pull/2451 and https://github.com/authzed/spicedb/pull/2540

### Changed
- update Go from 1.24.0 to latest 1.25.0 by @kartikaysaxena in https://github.com/authzed/spicedb/pull/2539
- docs: revamp README.md by @miparnisari in https://github.com/authzed/spicedb/pull/2474
- remove deprecated OTEL interceptors by @vroldanbet in https://github.com/authzed/spicedb/pull/2561
- perf: Add support in LookupSubjects for skipping caveats/expiration by @josephschorr in https://github.com/authzed/spicedb/pull/2564

### Fixed
- fix: improve the cluster error message when errors from all dispatchers by @josephschorr in https://github.com/authzed/spicedb/pull/2543
- fix: handling of multiple metadata for a single revision in Watch by @josephschorr in https://github.com/authzed/spicedb/pull/2563

## [1.45.3] - 2025-08-15
- Move Dockerfile builds to Go 1.24.6 by @josephschorr in https://github.com/authzed/spicedb/pull/2533
- Improve the errors returned from schema changes by @josephschorr in https://github.com/authzed/spicedb/pull/2532
- Fix issue with schema write validation by @tstirrat15 in https://github.com/authzed/spicedb/pull/2534

## [1.45.2] - 2025-08-08
### Changed
- improve docs of flags by @miparnisari in https://github.com/authzed/spicedb/pull/2490 and https://github.com/authzed/spicedb/pull/2519
- Ensure that the proper indexes are used for schema diff operations by @josephschorr in https://github.com/authzed/spicedb/pull/2520
- Have check debug trace overall result have a finer-grain matching to the results by @josephschorr in https://github.com/authzed/spicedb/pull/2511
- Parse schema from schemafile property by @noseworthy in https://github.com/authzed/spicedb/pull/2499

### Fixed
- Fix missing `rows.Err` check that could cause too-large writes to silently fail in PG by @josephschorr in https://github.com/authzed/spicedb/pull/2526
- Populate empty caveat_name and caveat_context in MySQL with default values by @mazdakb in https://github.com/authzed/spicedb/pull/2506

## [1.45.1] - 2025-07-10
### Fixed
- fix [#2496](https://github.com/authzed/spicedb/issues/2496) by @miparnisari in https://github.com/authzed/spicedb/pull/2497

## [1.45.0] - 2025-07-08 [YANKED]
⚠️ **Warning**: Due to [#2496](https://github.com/authzed/spicedb/issues/2496), please use v1.45.1.

### Changed
- Postgres: configure migration driver to support PgBouncer by @vroldanbet in https://github.com/authzed/spicedb/pull/2462
- trim binary by removing dependency to github.com/google/go-github/v43 by @miparnisari in https://github.com/authzed/spicedb/pull/2449
- default cockroachdb connection jitter to 30m by @ecordell in https://github.com/authzed/spicedb/pull/2467
- Improve how we select index forcing for CRDB by @josephschorr in https://github.com/authzed/spicedb/pull/2458

### Fixed
- Fix prefix on metric in the TELEMETRY doc by @josephschorr in https://github.com/authzed/spicedb/pull/2473
- pin fsnotify to prevent panics by @miparnisari in https://github.com/authzed/spicedb/pull/2485
- reference multi-platform sha256 in Dockerfile and bring back BASE argument in Dockerfile.release by @miparnisari in https://github.com/authzed/spicedb/pull/2471

## [1.44.4] - 2025-06-16
### Changed
- Add further filtering of forced indexes on CRDB to discount other shapes by @josephschorr in https://github.com/authzed/spicedb/pull/2437
- Skip selecting caveats and/or expiration in LR2 where applicable by @josephschorr in https://github.com/authzed/spicedb/pull/2441
- Allow for setting of the same metadata on a CRDB watch transaction by @josephschorr in https://github.com/authzed/spicedb/pull/2445
- Change the schema for CRDB to have the subjects sort match the index field order by @josephschorr in https://github.com/authzed/spicedb/pull/2440

## [1.44.3] - 2025-06-09
### Added
- Add development containers support by @katallaxie in https://github.com/authzed/spicedb/pull/2379
- Fix the kind of the logical checks telemetry by @josephschorr in https://github.com/authzed/spicedb/pull/2416

### Changed
- improvements to secondary dispatch by @josephschorr in https://github.com/authzed/spicedb/pull/2389
- move default GOMEMLIMIT to 90% of available memory by @vroldanbet in https://github.com/authzed/spicedb/pull/2427

### Removed
- remove serve-devtools command by @miparnisari in https://github.com/authzed/spicedb/pull/2424

### Fixed
- Remove subject check for deleted definitions in schema by @josephschorr in https://github.com/authzed/spicedb/pull/2395

## [1.44.2] - 2025-06-05
### Security
- CVE-2025-49011 fix: Checks involving relations with caveats can result in no permission when permission is expected by @miparnisari

## [1.44.1] - 2025-06-05
- Merge commit from fork
- Fix flaky reporter test
- Fix the flaky cluster test
- Fix flaky cluster test
- Have primary sleep cancel early if any result comes back from the secondary
- move default GOMEMLIMIT to 90% of available memory
- Add configurable primary hedging delay
- Have streaming dispatches also report to the metric whether they use the primary or secondary dispatcher
- Add maximum secondary dispatch wait and fix name of wait metric
- Quick test fixes for typechecking
- address comments
- revert helpful change to get coverage up
- improve coverage to past cover tests
- include alternative check that includes subrelations
- rename and fix comment
- lints
- finish tests
- attach the typechecking to typesystem instead of graph -- it didn't require graph and there's an unrelated bug discovered
- [schema] add type-checking test
- remove serve-devtools command
- Bump the github-actions group with 4 updates
- Bump golang in the docker group
- Add an additional unit test for the main function
- Rename the telemetry metric back, as fixing its type didn't break anything
- Fix the kind of the logical checks telemetry
- Add additional testing for telemetry package
- Add additional testing for releases package
- fix: Check not returning correct response when caveats are used
- Add tests for the pertoken middleware
- Add additional testing for perfinsights middleware
- Add additional unit tests for the caveats package
- Add additional unit tests for the caveat replacer package
- migrate Separating context proxy to stdlib context.WithoutCancel()
- do context severing with stdlib context.WithoutCancel()
- add development container support
- Add unit test for running main SpiceDB binary
- Add additional unit tests to preshared key package
- update codecov.yaml
- Add additional unit tests for the development package
- add codecov badge
- include integration tests in code coverage
- collect unit test coverage for codecov to work
- Handle both kinds of cancelation errors in test, part 2
- revert PR #2382
- Remove subject check for deleted definitions in schema

## [1.44.0] - 2025-05-19
### Added
- Add new flag that toggles a new performance insights prometheus metric: `--enable-performance-insight-metrics` (#2383)
- Add additional forced CRDB index in special case of no object IDs by @josephschorr in https://github.com/authzed/spicedb/pull/2385

### Changed
- Cache Features call in CRDB and skip outputting the check error by @josephschorr in https://github.com/authzed/spicedb/pull/2380

### Fixed
- Fix issue with underscore handling when deleting by object prefix (#2393)

## [1.43.0] - 2025-05-01
### Changed
- Improve CRDB index forcing logic to be more fine-grain
- Set pgx CancelRequestContextWatcherHandler.CancelRequestDelay to fix cancelation delay
- postgres datastore: do not warn on context cancelation
- add query shape and fix observe closer in ReverseQueryRelationships
- make other fmt.Errorf errors return as gRPC internal errors
- return grpc code IllegalArgument on malformed revision
- Allow schema watch in CRDB driver when watch is otherwise disabled
- wires query shape into the datastore QueryRels latency metric
- fix panic when running mage test:all
- fix pgx logging SQL cancelation as errors
- Only log errors if they are not cancel errors
- Make sure to close query rel latency metric on error and add query shape to metric
- fix typo in goreleaser.windows.yml
- fix custom analyzers skipping all files
- add ssf badge
- pin dockerfile images
- pin github actions
- add GCI linter for better organization of imports
- fix regression in [#2353] - add PR write permissions to labeler action
- tighten github workflow scopes
- add security policy
- Use golangci-lint v2
- Disable test of readonly serve-test, as it is making the test server itself flaky
- Switch experimental column optimizations to be enabled by default
- Fix labels on telemetry metric
- Add missing type set pass, add test and rename all defaults
- Switch the loader to use the passed-in typeset
- remove dependency on github.com/hashicorp/go-multierror
- Minimal decoupling of validation file parsing and schema compilation
- Change DB creation to not occur in a transaction
- Add retries to MySQL test DB constructor
- amend mutex analyzer
- Change GC to use a single, shared connection
- amend mutex analyzer
- Fix logger for certmanager
- Return an error if a lock could not be released
- Change PG and CRDB datastore drivers to use PGX cancelation
- Bump the go_modules group across 2 directories with 1 update
- add mutex analyzer
- add GUARDED_BY comments
- Tidy magefiles
- Tidy e2e and analyzers
- Bump golang.org/x/net from 0.37.0 to 0.38.0 in the go_modules group
- Change all call sites to pass the typeset to deserialization
- Add accessors for deserialization of a caveat using a custom typeset
- Simplify the registration in the caveats typeset
- Switch to larger depot instances for datastore consistency tests
- Add a concept of a caveat TypeSet to allow overriding the types available to caveat processing
- Add logical checks telemetry
- Further optimize the cases when transaction metadata is written in CRDB
- document packages
- postgres: optimize CheckRevision query
- Disable preconditions when isolation level is relaxed
- introduces a flag to relax postgres isolation level
- Force indexes on Postgres and change subject sort order to match index
- Force indexes on Spanner and change subject sort order to match index
- internal/datastore/pg: template repair batch size
- Add option to disable watch
- Address review feedback
- Add support for index hinting and enable forced indexes on CRDB
- Fill out the remainder of the index tracking and query shapes
- Start on queryshape validation during index checking
- internal/datastore/pg: server-side loop for repair
- Ensure the Postgres read-write tx uses the TX for the reader
- put error checks before accessing return values
- Update Go to 1.23.8 to fix a reported vuln in Go
- Increase datastore test timeout
- Fix handling of no columns in Spanner
- [typesystem] remove typesystem package
- Remove panic from devcontext serve in favor of a warning
- add test-case for the migration
- fix(memdb): check if closed
- Add improved caveat filtering to the datastore relationship filter
- enable tparallel to keep tests fast
- make trivy build faster with more runner compute
- fix linter errors and missing go mod tidy
- fixes incorrect handling of definition deltas via Postgres Watch API
- Bump the go-mod group with 26 updates
- rm ref from other workflows
- feat: clickable build notification links
- introduces depot runners
- Bump golang in the docker group

## [1.42.1] - 2025-04-02
### Changed
- support both old and new read replica flag prefixes
- remove limit in AtRevision in dispatch resolvermeta

## [1.42.0] - 2025-03-31
### Changed
- add better LR otel instrumentation
- fix leader election retry interval for revision heartbeat
- update mysql docs
- Have dispatch skip hedging delays when dispatching to unsupported relations
- move convertWatchKindToContent to separate file
- amend [#2286](https://github.com/authzed/spicedb/issues/2286)
- Add test that will panic if the primary is not canceled
- Switch tdigest lib due to occasional panics in the other library on ARM
- Clean up the logs a bit in secondary dispatching
- Add guard for nil response
- Add secondary dispatcher benchmark
- Improve performance of CEL handling in secondary dispatch
- incorrect labelling of primary dispatch cancelation
- measure duration of the CEL expression used for the secondary
- Try with p95 to see if it gives us the sweet spot for cancelation ratio
- add metrics to see if primary dispatch executed or cancelled
- add more resolution to hedger wait histogram
- Add additional trace logs
- do not use hardcoded delay offset and switch to p90
- log hedger wait
- Make delays configurable for testing
- Restructure context cancelation in secondary dispatch hedging to be more fine-grained and cancel sooner
- add metric for the secondary dispatch computed t-digest wait duration
- fix panic with a fork with the fix
- add buf format to lint:extra
- remove deprecated tenv linter
- add unit tests for ConvertWatchKindToContent
- upload coverage to codecov
- change API shape and tests
- Fix MySQL test
- Reduce flakiness of the stats test
- Schema Change Events via Watch API
- Address review feedback on index checking
- Fill out the remaining Postgres indexes
- Add CRDB indexes
- Add MySQL indexes
- Add explain parsing support for MySQL
- Get end-to-end checking working for Postgres
- Add first query shape to Check
- Add a proxy to check that indexes referenced are used
- Wire the new query options through the datastores
- Add index definitions and query shape support to schema and query options
- Add a metric tracking the selected replica
- [schema] fix concurrency
- [schema] add failing concurrency test
- [schema] fix automatic validation if the source is pre-validated
- Have revision errors only log at debug level
- fix deprecated option and update copyright year
- Add additional logging to flakey PG test
- do not log at warn level when setting datastore in read-only
- Reattach child contexts to the parent for streaming dispatch
- avoid contention when gathering hedger statistics
- Add statistics-based auto-hedging support to the remote dispatcher
- Add secondary dispatch support for LS
- [*] finish converting to new package
- [*] convert packages to use the new schema package
- Merge branch 'main' into flags/fix-datastore-read-replica-conn-pool-read-flags
- fix typo in flag prefix
- Bump golang.org/x/net in /e2e in the go_modules group across 1 directory
- Go mod tidy
- Bump golang.org/x/net from 0.35.0 to 0.36.0 in the go_modules group
- Update DeleteRelationships API to return the number of relationships deleted
- Fix LR2 secondary dispatch sometimes returning only one result
- Fix default concurrency limit for bulk check API
- Update the datastore interface to return the number of deleted relationships
- remove pointerization of the compiledPartials
- Require definitions and partials to have distinct names
- [schema] address @josephschorr's comments
- [schema] address @tstirrat15's comments
- [schema] Fix up schema package to be more useful in the followup
- [schema] fix tests
- [schema] port over tigerreachability (tests still to be cleaned up)
- [schema] port over reachabilitygraph.go and tests
- [schema] create parallel schema package and port typesystem/typesystem.go over to it
- Add CRDB matrix tests
- Bump golang in the docker group
- Update e2e go.mod
- Bump the go-mod group with 14 updates
- s/goreleaser/release
- replace 8398a7/action-slack with slack-github-action
- feat: add release job Slack notification
- global ctx
- Merge branch 'mage-test-coverage' of https://github.com/kartikaysaxena/spicedb into mage-test-coverage
- refactor ctx
- context coverage bool
- separate targets for cover
- use path hash instead
- different profiles for tests
- added test coverage
- refactor ctx
- Merge branch 'main' into mage-test-coverage
- Merge branch 'main' into mage-test-coverage
- context coverage bool
- separate targets for cover
- use path hash instead
- Merge branch 'main' into mage-test-coverage
- different profiles for tests
- added test coveragd

## [1.41.0] - 2025-02-28
### Changed
- follow ups to [#2252](https://github.com/authzed/spicedb/issues/2252)(https://github.com/authzed/spicedb/issues/2252)
- Move nodeid default calculation to init to avoid race
- add heartbeat leader election
- make pg datastore continuously checkpoint using a revision heartbeat
- expand pgSnapshot tests
- Update go.mod for vulns in Go libs
- Promote the reflection APIs into the schema service
- Rename experimental reflection methods in prep for promoting the APIs
- err with ref
- improve err msg
- Change Spanner default metrics to go to OTEL
- support follower read delay on pg and mysql datastores
- Improve handling of watch errors
- Fix for https://github.com/envoyproxy/go-control-plane/issues/1074 and tidy go mod
- Bump the go-mod group across 1 directory with 34 updates
- Port expiration compiler changes into composableschemadsl

## [1.40.1] - 2025-02-08
### Changed
- Port in expiration parsing
- crdb pool: handle cases with nil errors
- Have strictreplicated proxy buffer relationships
- Move to Go 1.23.6 for a reported Go vuln
- fixes pg replica error: "canceling statement due to conflict with recovery"
- Increase watch test timeout
- Remove the reference for changes being emitted by Watch
- Update tracing logic in authn code
- Add metrics for usage of the replication proxies in the datastore
- Change default version for CRDB testing to latest
- Implement imports on AST
- Remove parallel test to reduce flakiness
- Change strict reading PG to only return rows when valid
- [mysql] pipe logger into mysql for debug printing (#1055)
- Have LookupSubjects return Unimplemented if a cursor is used
- Switch namespace read call in Check to simplify
- Reorganize the read replica proxies
- Implement parsing of partials syntax
- Ensure that deferred replication fallback errors are handled properly on QueryRelationships and ReverseQueryRelationships iterators in the strict read mode proxy
- upgrade go to fix CVE
- implement flag for spanner

## [1.40.0] - 2025-01-16
### Changed
- Simplify import syntax
- add schemaFile to ValidationFile
- Change the postgres migrations in two ways:
- Change feature detection for CRDB watch to not require waiting
- Add postgres to the consistency tests to generate consistent test names
- Increase max number of retries on flaky test
- Delete LookupResources v1, ReachableResources and all helper code
- Move trace ID generation into a single function
- Change node ID to use a hash of the hostname
- Re-enable parallel testing of consistency datastore tests
- Don't hold a connection to the postgres instance after creating the DB
- Switch postgres tests to run in a matrix of versions
- Remove sleep in stats test unless needed
- Disable retries on the serialization test for PG
- Switch postgres to use a set and return an error if a duplicate caveat name is given
- Change unlock call to a unshared context in GC
- Ensure source is returned for all check debug traces
- Address further feedback:
- Address review feedback on SQL generator
- Move column count logic into helpers
- Address review feedback
- Add additional tracing to relationship querying
- Add testing for expanded comparison logic in SQL builder
- Elide the expiration column and checking when expiration is either disabled or the relationship cannot be marked as expiring
- Implement a combined builder pattern for relationship SQL construction
- Update tests for expiration filtering
- Move column elision behind an experimental flag
- Add validation test for elision of columns
- Change tests to use a new entrypoint for creating memdb for testing
- Add additional testing for column elision
- Skip loading of caveats in SQL when unnecessary
- Relationships selected in SQL-based datastores should elide columns that have static values
- Add a node ID to debug tracing and spans
- Add support for debug tracing to CheckBulkPermission API
- Fix the strict read proxy
- Remove parallel running on datastore consistency tests to reduce flakiness
- Ensure datastore containers do not auto-restart
- Tidy e2e
- Fix casting of type in PG datastore repair query
- Bump the go-mod group with 23 updates
- Bump golang in the docker group
- Add slightly more information to the LR2 dispatch traces
- Switch datastore tests to use a larger runner
- Move caveat loading into a shared runner to reduce overhead in dispatch
- Wire Spanner's logging up to zerolog
- Ensure spanner raises an error when trying to delete an unknown namespace
- Add watch streaming test to CRDB tests
- Add test for invalid namespace delete to increase datastore coverage
- Fix typo in name of errors file
- Add option to enable query parameters to appear in traces
- Add caveated bulk load test to datastore tests
- Add tracing to the LR2 implementation
- Add some additional datastore tests to improve coverage
- Update net lib for reported Go library vulnerability
- Improve test coverage of memdb datastore with some new rel tests
- Add support for bulk check in steelthread test
- Deparallelize the steelthread tests to hopefully remove the flakiness
- Fix name of test suite for DS consistency tests
- Add additional tests to the datastore consistency test suite
- Add steelthread tests to CI and to `mage test:all`
- Add basic steelthread tests for bulk import and export of relationships
- Add GC run test
- Change GC test to always call GC directly
- Bump the go_modules group across 2 directories with 1 update
- Update Go crypto to v0.31.0 due to a reported vuln in that lib
- Update grpc health probe for crypto lib fix
- Backport changes from [#2163](https://github.com/authzed/spicedb/issues/2163)(https://github.com/authzed/spicedb/issues/2163) into 1.39.0
- Fix bulk export of relationships with caveats
- Update grpc health probe for crypto lib fix
- Update Go crypto to v0.31.0 due to a reported vuln in that lib
- Bump the go_modules group across 2 directories with 1 update
- Increase timeout on GC check in GC test
- Move GC tests in PG out of parallel running
- Garbage Collection improvements in MySQL and Postgres
- Remove now-unused windows workflow
- Switch number of rels returned on error
- Switch rel expiration to use the datastore's clock
- Remove RelationDoesNotAllowCaveatsForSubject as it is unused now
- Remove new indexes, as per internal discussion to revisit later
- Add arrow expiration consistency test
- Move constant into const
- Make sure *all* queries used in the query builder are filtered by rel expiration
- Change to use a single `now` for the entire reader in memdb
- Add doc comments on expiration filter options
- Add counter test over expiring and expired relationships
- Add expiration to memdb relationship debug string
- Remove unused method in memdb
- Add additional rel conversion tests
- Lower watch buffer size to ensure timeout is hit
- Add expiration into CRDB watch
- Fix typo
- Fix metadata map reference in CRDB
- Truncate times to seconds when generating hashes
- Add indexes for relationship expiration GC
- Ensure the canonical representation of expiring rels always uses UTC
- Add experimental flag for enabling rel expiration
- Add expired relationship GC to PG and MySQL drivers
- Add filtering for TTL deletes in CRDB
- Add watch filtering for TTL deletes in Spanner
- Add bulk upload with expiration test
- Add watch test for relationship with expiration
- Add support in Spanner driver for expiration
- Add support in CRDB driver for expiration
- Add support in MySQL driver for expiration
- Add support in Postgres driver for expiration
- Enable expiration on bulk import of relationships
- Further improvement to caveat validation errors for relationship expiration
- Add support in schema change checking for expiration
- Add write rels test for expiring relationship
- Get relationship expiration working in consistency tests end-to-end
- Add support for expiration on rels in validation
- Flip flag on the schema compiler to allow `use expiration` by default
- Add support for rel expiration in memdb and add basic datastore tests
- Add missing translation in struct for rel expiration

## [1.39.1] - 2024-12-12
### Changed
- Update Go crypto to v0.31.0 due to a reported vuln in that lib
- Update grpc health probe for crypto lib fix
- Backport changes from [#2163](https://github.com/authzed/spicedb/issues/2163) into 1.39.0

## [1.39.0] - 2024-12-05
### Changed
- Add missing wiring of service label through With* methods in options
- Add missing service label check in metrics for consistency
- Remove t.Parallel from pgbouncer tests to fix flakes
- Remove parallel from MySQL tests as well, to hopefully fix flakes
- Changes to address flaky DB tests
- Remove duplicate clause from namespace deletion in PG driver
- Tidy
- Bump golang in the docker group
- Bump the go-mod group with 18 updates
- fix: namespace/caveat changes cause an incorrect delete event
- repro: namespace/caveat changes cause an incorrect delete event
- Add one more test for mixed valid/invalid
- Update comment
- Capture and fix cache bug
- Ensure caveats are read in bulk import
- Update github.com/opencontainers/runc for reported vuln in that lib
- Adjust names of errors to match new lint rule
- Add support to the core and tuple packages for relationship expiration
- Type system changes for first-class expiration support
- Add explicit option to enable expiration in schema
- adds index to support efficient querying of Watch API
- Add support to the schema generator for the new expiration trait
- Add support to the schema compiler for the new expiration trait
- Add basic parser support for `with expiration` on subject types
- Add lexer changes for relationship expiration support
- postgres watch: checkpoints should move the high watermark revision
- issue a checkpoint when head revision moved outside an application transaction
- bump max connections for PG tests
- add a flag to allow spicedb to run against non-head migrations
- Fix MySQL test breakage caused by daylight savings change
- Add subject filters in schema relation delete to force use of the index
- Add subject filters in schema relation delete to force use of the index
- introduces ByteSortable method in Revision
- Add missing limit in schema delta checking
- Improve PG serialization error on writes
- Implement circular import detection + import dedup
- introduces `WatchOptions.EmissionStrategy` to handle limitations of CockroachDB changefeeds.
- fix trivy ratelimit errors in CI
- adds code and test to detect logical clock overflow
- fix bug in HCLRevisionFromString when parsing zero logical clock
- reproduce bug in HCLRevisionFromString when parsing zero logical clock
- Implement naive resolution of local imports
- Tidy e2e
- Bump the go-mod group across 1 directory with 23 updates
- Bump golang in the docker group
- Update operations_test.go
- Use complete sentence in error
- Fix caveat string encode to not emit spaces
- Add additional check to elide writing empty transaction metadata
- Fix MySQL test breakage caused by daylight savings change
- pkg/proto: adopt CodecV2 and gRPC buffer pooling
- Update doc comments
- Add relationtuple stringifying function
- Remove internal label from consistency middleware to allow it to be replaced
- Move consistency middleware into pkg so embedded uses can override
- Fix typo
- Fix formatting
- Add a new consistency middleware for full-consistency-only callers
- Fix signature of MustParseV1Rel
- Update usage of renamed fn
- Add MustParseV1 for test purposes
- Rename function to clarify lack of caveat
- Make lexer tests exhaustive, update
- Update parser behavior to reflect new keywords
- Add consumeKeywords function
- Update test to reflect new behavior
- Add any and all to keywords
- Add implementation of local import syntax
- Revert changes to generated options
- Re-add flag and deprecate
- Re-add cursor logic
- Gofumpt
- Fix lint issue
- Remove LookupResources v1 implementation
- Remove dead code"
- Enable native histograms in server latency metrics
- Change the trivy database to work around rate limits
- Gofumpt
- Update import references
- Add composable schema DSL package
- Add currently-enabled workflow to release windows
- Adjustments after running `fieldalignment -fix github.com/authzed/spicedb/pkg/tuple`
- Address PR feedback
- The instantiation ctx timeout for postgres datastore is aggressive at 5 seconds. This causes intermittent initialisation failures in environments where the postgres is slow / remote etc. Since this is an instantiation flow, its ok to increase the timeout to a higher number like 30 seconds. This can avoid such failures.
- Address PR feedback
- Address review feedback
- Add DebugAssertNotNil
- Fixes for new relationship structs after rebase
- Fix dev test error
- Update read-only mode test for PG
- Disable close-after-usage check for rel iterator, as it handles it internally now
- Integration test fixes for changed relationships
- Integration test fixes for changed relationships
- Fix WASM test for relationships changes
- Fix benchmark test
- Fix Spanner tests for relationship changes
- Fix MySQL tests for relationship changes
- Fix Postgres tests for relationship changes
- Fix CRDB tests for relationship changes
- Move the remaining go files to 1.23.1
- Address lint errors
- Update CRDB and Spanner and a few other locations for relationship changes
- Update MySQL driver for relationship changes
- Update postgres driver for relationship changes
- Start addressing RelationTuple in all other packages
- Tuple -> Relationship renames
- Small tuple package improvements
- Update tests and get memdb passing for struct and iterator changes
- Start on datastore conversion with MemDB
- Reorganize tuple package and add relationship structs

## [1.38.1] - 2024-11-18
### Changed
- Fix MySQL test breakage caused by daylight savings change
- Add subject filters in schema relation delete to force use of the index
- Add subject filters in schema relation delete to force use of the index
- Add missing limit in schema delta checking
- pkg/server: pass ctx to info collector
- metrics: spicedb_environment_info from telemetry
- fix CI errors on recent merge
- fix CI errors on recent merge
- Ensure caveat context is sent to all LR2 dispatches
- Merge commit from fork
- Add API support for transaction metadata on WriteRels and DeleteRels
- Tidy e2e
- Ensure caveat context is sent to all LR2 dispatches
- make unit tests faster
- Bump the go-mod group with 15 updates
- add longer backoff to postgres tests
- emit memdb checkpoints after changes
- Implement support for metadata associated with read-write transactions

## [1.38.0] - 2024-10-18
### Changed
- pkg/server: pass ctx to info collector
- metrics: spicedb_environment_info from telemetry
- fix CI errors on recent merge
- fix CI errors on recent merge
- Ensure caveat context is sent to all LR2 dispatches
- Merge commit from fork
- Add API support for transaction metadata on WriteRels and DeleteRels
- Tidy e2e
- Ensure caveat context is sent to all LR2 dispatches
- make unit tests faster
- Bump the go-mod group with 15 updates
- add longer backoff to postgres tests
- emit memdb checkpoints after changes
- Implement support for metadata associated with read-write transactions

## [1.37.2] - 2024-11-13
### Changed
- Add subject filters in schema relation delete to force use of the index
- Add missing limit in schema delta checking
- pkg/server: pass ctx to info collector
- metrics: spicedb_environment_info from telemetry
- fix CI errors on recent merge
- fix CI errors on recent merge
- Ensure caveat context is sent to all LR2 dispatches
- Merge commit from fork
- Add API support for transaction metadata on WriteRels and DeleteRels
- Tidy e2e
- Ensure caveat context is sent to all LR2 dispatches
- make unit tests faster
- Bump the go-mod group with 15 updates
- add longer backoff to postgres tests
- emit memdb checkpoints after changes
- Implement support for metadata associated with read-write transactions

## [1.37.1] - 2024-10-14
### Changed
- Ensure caveat context is sent to all LR2 dispatches

## [1.37.0] - 2024-09-26
### Changed
- Remove duplicate and redundant code
- Enable LRv2 by default and update the steelthread tests
- fixes memory leak via HTTP Gateway
- Use new registration function
- Add utility to register common flags
- Stop using yaml anchors
- Fix build folder
- Bump to most recent version of goreleaser
- Revert "Merge branch 'use-most-recent-goreleaser' into release-1.36"
- Merge branch 'use-most-recent-goreleaser' into release-1.36
- Fix serve-devtools flags
- make bulk export service functions use read-only datastore
- Stop using yaml anchors
- README: refresh logo with brighter colors
- README: rework sections: zanzibar, contrib, users
- Add support for secondary dispatching on LR2

## [1.36.1] - 2024-09-19
### Changed
- Stop using yaml anchors
- Fix build folder
- Bump to most recent version of goreleaser
- Revert "Merge branch 'use-most-recent-goreleaser' into release-1.36"
- Merge branch 'use-most-recent-goreleaser' into release-1.36
- Fix build folder
- Bump to most recent version of goreleaser
- Gofumpt
- Add usage metrics for export
- Add metadata tests for import and export
- Run go mod tidy on the subdirs
- Ran mage lint:go
- Copy-paste to simplify
- Remove dedupe attempt
- Beginning of implementation
- Bump to most recent authzed-go
- add continuous checkpointing to Datastore Features
- Remove warning for an arrow referencing a relation in its own namespace
- Fix log.Err usage
- Wrap errors where applicable
- Log errors when encountered
- Make files pass goreleaser-pro check
- Pin goreleaser version for now
- Reinstate erroneously-removed bits
- Try using goamd64 flag to point at correct file
- Fix releaser issues
- Add safecast to e2e
- Get types lining up
- Fix a typo that led to nil dereference
- Run formatting
- Lots of lint fixes
- Attempt at test fix
- Fix limit max
- Simplify casts in development package
- Remove lossy cast
- Fix unsafe conversion errors
- Fix lint errors
- Fix missing annotation
- Bump in dockerfile.release as well
- Bump go version in container
- Bump version for analyzers
- Add e2e mod file as well
- Remove erroneously-committed file
- Bump go version to take go patch
- Bump github.com/opencontainers/runc in the go_modules group
- Break out windows goreleaser config into its own file and build step, as it needs to run on a windows machine
- Add goreleaser configuration to push Windows package to Chocolatey
- Implement support for relationship integrity
- Remove the ONRTypeSet and create a new combined CheckDispatchSet
- Move ONRSet into the one internal package in which its used and simplify
- Fix error formatting
- Tidy
- Bump the go-mod group with 32 updates
- Remove duplicate update test
- Bump golang in the docker group
- Make the max size exceeded error public
- Add linter to enforce (Un)MarshalVT usage
- Fix data type for pg_class relcount
- Add configurable max buffer size for watch change tracker
- Add a default connect timeout for watch in CRDB driver
- Ensure the validationfile loader passes the full caveats to the typesystem
- Lint fix
- Ensure cursored LRv2 calls are dispatched to LRv2
- Fix up flags
- Update pkg/diff/namespace/diffexpr_test.go
- Have diffexpr handle the case of adding to a single child expression
- Add flag registration to the testing command
- Add otel flags back to migrate command
- Update pkg/cmd/serve.go
- Reorganize some flags and hide some things
- Add pprof and termination flags for migrate
- Fix flag registration
- Add bold and blue to headers
- Remove command passthrough
- Linting
- Reorganize flags into flagsets
- Pass flagset into helper
- Move serve-related flags up into serve
- Only add the finalizer on iterators when CI testing
- Configure memlimit and add some notes
- Bump to most recent version
- Remove unnecessary branch from limit logic
- Cleanup handling of internal errors in Check dispatch

## [1.35.3] - 2024-08-16
### Changed
- bump cobrautil for automaxprocs fix
- Ensure all resources are returned for relation check when caveats are specified
- Add nicer error if the Postgres primary node has gone readonly
- Push up usage
- Use cobrautil for proclimits
- Move import to break cycle
- Move to break cycle:
- Add limit setting to default prerun
- Get rid of debug
- Move limit setup out of main
- Add limits RunE function
- Change the filter count check to a debug assertions
- Ensure debug information is returned for recursive checks that dispatch
- Add expression diffing library for schema

## [1.35.2] - 2024-08-05
### Changed
- Fix experimental LookupResources2 to shear the tree earlier on indirect permissions
- Tidy for changes
- Bump the go-mod group with 21 updates
- Add server version middleware to serve-testing
- Handle functioned arrows in warnings system
- Add ability to get warnings from the WASM dev interface
- Add an extra `source_code` field to developer warnings

## [1.35.1] - 2024-07-30
### Changed
- bump Docker to address security scanners surfacing CVE
- Fix conversion of caveat debug context
- Switch caching package's interface to be generic and add experimental flag to try different caches

## [1.35.0] - 2024-07-25
### Changed
- Fix debug traces when caveats use the same param name
- additional dispatch chunk safeguards
- add support for configurable dispatch chunk size
- Disable prepared statements via pgx config
- optimization: avoid proto allocation
- optimization: reduce allocations in syncONRSet
- optimize FilterToResourceIDs and FilterWithSubjectsSelectors
- Review feedback and additional tests
- Remove apparently unneeded COALESCE call
- Re-engineer how check hints are handled to use protos for performance
- Switch experimental LookupResources2 to request additional chunks of dispatched resources when checking those already received from another dispatch
- Add additional steelthread tests for LookupResources and fix issue in LR2
- Implement a new, experimental variant of LookupResources as LookupResources2
- Add ability to toggle off specific warnings via magic comments
- Ensure that the bootstrap overwrite flag actually fully overwrites
- Update gRPC to v1.65.0 to fix reported gRPC vuln
- added tests for handling malformed contents on hovering and formatting without panicing
- fixed lsp panicing on formatting malformed content
- Bump github.com/rs/cors
- adjust pg revision timestamps
- Move to go 1.22.5 for a reporting go lang vuln
- Tidy go.work.sum
- Tidy e2e
- Bump the go-mod group with 22 updates
- Bump goreleaser/goreleaser-action in the github-actions group
- Add an additional mode to replica support that uses a strict read mode
- Add caching on read replica CheckRevision calls to reduce the overhead of using read replicas
- Add support for read replicas to Postgres and MySQL data stores
- Move integration test file into the correct directory
- Add a steelthread test for an indirect permission for LR
- Add a steelthread test for intersection arrows
- Workaround to snapcraft regression
- Add dispatching support for intersection arrows
- Add translator and generator support for new functioned arrows
- Add support for new syntax to schema parser
- Have steelthread tests run in parallel and against all datastores
- Add basic LR steelthread tests
- Fix resuming reverse queries by subject in memdb and add a datastore test
- PR feedback
- enriches postgres revisions with txID and timestamp
- Add additional LS steelthread test with intersection
- Start on a steel thread test framework

## [1.34.0] - 2024-06-20
### Changed
- .github: bump to snapcraft 8.x
- Fix exclusion operation in Check dispatch to *always* request the full set of results for the first branch
- Fix empty value on optional credentialsJSON for Spanner
- Move to go 1.22.4 for a reported go vuln
- Merge branch 'main' into fix/bulk-loader-nullstring
- Use normal string instead of sql.NullString for caveatname when bulk importing tuples. Normal Writes also use an empty string. Touch operations with caveat updates wont work when the values are null instead of "".
- Small improvements to locking in optimized revision handler
- Remove OptimizedRevision from singleflight since the handler itself does a singleflight
- Export Spanner credential JSON for datastore
- Add a custom linter to find any recursive error marshaling for zerolog
- Add better subject error messages on write/delete validation
- Support credential JSON for Spanner
- Ensure the object type prefix is used for caveat refs as well
- Add the debug trace to the details of the recursion error
- README: fix discord badge
- spanner: use stale reads for current_timestamp for optimized revision
- Bump github.com/mostynb/go-grpc-compression in the go_modules group
- Return a proper error code if a wildcard subject is specified
- Fix exclusion operation in Check dispatch to *always* request the full set of results for the first branch
- adds automaxprocs and automemlimit
- Tidy e2e
- Bump the go-mod group with 21 updates
- Update CLA link in `CONTRIBUTING.md` to point to v2
- Ensure stability of exclusions in validation package
- goreleaser: use build.head? in install
- goreleaser: refactor brew formula
- Make sure to escape underscores in resource ID prefix matches in filters
- Fix deprecation warnings for grpc Dial by centralizing most call sites and adding TODOs to the rest
- Changes to the relationships count API implementation after design discussion
- Tidy e2e
- Add basic relationships count API
- Add test for UpdateCounter and fix various issues encountered
- Add stable filter name tests and have it return an error as an extra check
- Add CRDB support for counters
- Add Spanner support for counters
- Add MySQL support for counters
- Add postgres support for counters
- Add memdb support for the new counters functions
- Update datastore proxies with new counting methods
- Add counting interfaces to datastore
- makes it possible to compare datastore-specific revisions with NoRevision

## [1.33.1] - 2024-06-06
### Changed
- Fix exclusion operation in Check dispatch to *always* request the full set of results for the first branch

## [1.33.0] - 2024-05-17
### Changed
- Update ROADMAP.md
- ROADMAP: init
- Remove unused datastore config
- Switch spanner datastore to use the built-in stats table for estimating rel count
- Add ExperimentalComputablePermissions API
- Add ExperimentalDependentRelations reflection API
- Update go version
- Update grpc health probe for reported vuln in Go
- pkg/cmd: auto complete otel, log flags
- .github: pass snap store creds to goreleaser
- Add filtering to the experimental schema reflection API
- Add experimental schema reflection API
- Add support for ExperimentalSchemaDiff
- Add schema-wide diffing library
- expose BulkExportRelationships service controller logic
- fetch git tags so that goreleaser generates the right binary version
- pin to a trivy version that does not detect the built image as spicedb 0.0.1
- refactor bulk export relationships logic
- Update CEL version used to latest
- Tidy go.work.sum
- Tidy e2e
- Bump the go-mod group with 21 updates

## [1.32.0] - 2024-04-30
### Changed
- cmd: add man generation command
- Add warnings support to the LSP
- Fix positioning on the warnings
- Begin support for warnings and linting in schema
- .goreleaser: rename nightly config
- .goreleaser: fixup deprecated fields
- github: add snap releases
- goreleaser: publish snaps
- goreleaser: add snap completion
- .github: install snap in workflows
- goreleaser: add completions to release
- magefiles: add gen:completions
- goreleaser: init snap
- add aws iam authentication for mysql
- Update http2 package for Go vuln
- Add configurable limits for all APIs
- Include doc comments in resolver generated source
- Ignore AST nodes without rune positioning information (such as comments)
- internal/lsp: fix nil pointer during logging
- internal/lsp: add textDocDidChange support
- don't treat "" as a special case, avoid calling instead
- return an error for unknown credential providers return the NoCredentialsProvider (aka nil) when given a empty string
- return -> returns
- review feedback & more unit testing
- add support for AWS IAM authentication in the postgres datastore

## [1.31.0] - 2024-04-11
### Changed
- internal: use GCR mirror for Docker images
- cmd/server: log dispatching at debug level
- Fix SubjectSetByType to properly *merge* mapped relations
- Fix SubjectSetByType to properly *merge* mapped relations
- introduces a faster query to tuple GC
- Fix logging in LSP
- Add hover support to the LSP implementation
- Add additional resolver test
- Fix rune start positions for left recursive expressions
- Disable the repair tests on PG versions that do not support it
- bump analyzers go.work to 1.22.2
- bump to go 1.22.2
- Add support for older LSP implementations that don't request diagnostics
- Add missing setup-go
- Initial implementation of a Language Server for SpiceDB schema
- Update golang.org/x/net@v0.23.0 to fix reported vuln in the go lib
- Update golvulncheck to fix issue in their latest release
- Add support for caveat params to the resolver
- Fix typo in NodeTypeCaveatExpression
- Add basic schema resolver to the development package
- LookupResources Postgres query optimization
- Update README with playground repo link
- Use a specific relation for arrow lookups in LR when applicable
- Fix source range on caveat type references
- Start on additional development capabilities around schema by adding method to map positions back to the AST node path in a source file
- Correct version requirement for datastore repair
- Add license checking lint step to CI
- mod tidy e2e
- Bump the go-mod group with 6 updates
- Bump the github-actions group with 1 update
- PR review: assignment style does not align with the codebase
- propagate request-id also through reachable resources dispatch
- add propagation of requestid cross dispatch
- propagate logger context into pgx logs
- fixes critical issue with OpenMetrics suffixes
- Fix re-creating deleted relationships
- Add some additional deletion tests for relationships
- Early terminate in LookupResources when no limit was specified
- Update OpenTelemetry middlewares
- Add additional unit tests for expected behavior and fix errors for BulkLoad
- Small type system memory improvements
- Small mem improvements on BulkImport
- go mod tidy
- Bump the go-mod group with 2 updates
- Use type information to optimize TOUCH operations in the PG datastore
- Update labeler config for labeler action v5
- Import request ID metadata key from authzed-go
- report GC stats even in the event of a GC worker error
- Bump the go_modules group group with 1 update
- Bump github.com/docker/docker in /magefiles
- rename update groups
- Bump the gomod-version group with 8 updates
- Bump the gomod-version group with 1 update
- go mod tidy e2e
- fix linter warning
- go mod tidy analyzers
- Bump the gomod-version group with 24 updates
- add github action grouping
- Bump actions/labeler from 3 to 5
- Bump docker/setup-qemu-action from 1 to 3
- fix rate limiting in security scanning
- adds dependabot configuration to update GitHub Actions

## [1.30.1] - 2024-04-01
### Changed
- Fix SubjectSetByType to properly *merge* mapped relations

## [1.30.0] - 2024-03-18
### Changed
- do return backward incompatible `--explain` debug info in trailer
- turns gRPC latency histogram into a toggleable option
- make registration of gRPC prom metrics not fail if already registered
- fix race on error member of TaskRunner
- Move debug traces for CheckPermission into the response
- fixes merge queue not supporting CodeQL
- use the most recent Go version with CodeQL
- regenerate CodeQL configuration using GitHub
- dependency updates
- move health check logs to debug level
- CheckBulkPermissions
- Add mage test:unitcover to generate coverage reports over all unit tests
- Small increase in test coverage for subjects testutil
- Add extended relationships filtering to watch API
- Add extended relationships filtering to read relationships and delete relationships APIs
- Add extended relationships filtering to bulk export API
- Add support for extended relationship filtering in all datastores
- Update buf and API version for new relationships filters
- Have caveat diffs properly check if an expression has changed
- Update gRPC health probe version for recent Go vulns
- Debug migrate command in VSCode
- fix: reference to changed variable
- chore: add tests for observable proxy
- fix: delete options not being passed
- Set maximum bulk check size
- Add linter
- Fix and remove downcasts
- Ensure SpiceDB release versions are semver
- Update protobuf version for reported CVE in protobuf lib
- Ensure that invalid versions do not cause a nil panic
- Update protobuf version for reported CVE in protobuf lib
- Ensure that invalid versions do not cause a nil panic
- spanner: allow spicedb to run with head or head-1 migration
- Tidy e2e
- Add  // nolint: staticcheck to Spanner deprecated calls
- Tidy e2e
- Bump github.com/planetscale/vtprotobuf
- Bump cloud.google.com/go/spanner from 1.54.0 to 1.57.0
- Tidy e2e
- Tidy e2e
- address PR feedback
- fixes integer overflow in ForEachChunk
- chunking correctness: add repro for DispatchCheck
- reproduce integer overflow in ForEachChunk
- address PR feedback
- fixes integer overflow in ForEachChunk
- chunking correctness: add repro for DispatchCheck
- reproduce integer overflow in ForEachChunk
- Bump github.com/prometheus/client_golang from 1.18.0 to 1.19.0
- Bump golang.org/x/vuln from 1.0.1 to 1.0.4
- Bump go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp
- adds OpenTelemetry TraceID to logs
- Switch delete relationships API to use the new limit-ed DeleteRelationships call
- Add optional limit on DeleteRelationships calls in the datastore
- Change CRDB datastore to use a combined deletion query, rather than a query per relationship
- Change CRDB driver to use new method for getting transaction timestamp
- Fix flake on transaction retry test by specifying a longer timeout
- enables exemplars prometheus support
- Fix flake in singleflight test by increasing the run time slightly
- Remove stale TODOs
- disables Snyk checks
- reduces chunking allocations for wide relations
- Close the parent context in serve_test when complete
- VSCode launch config
- Merge branch 'main' into enable-snyk
- centralize Go version used
- enable snyk security scanning
- Remove duplicate testing code
- Hide a previously deprecated flag
- skip all steps for matrix jobs when the whole job should be skipped
- replace so we don't get advisory failures in the e2e package
- enable gosec G404 and document when why it's ok to use math/rand
- disable revive unused-parameter
- replace math/rand usage in requestid middleware
- Small improvement in tuple package to remove TODO
- Fix small TODO in type system with a small code move
- spanner: allow spicedb to run with head or head-1 migration
- README: add logos for acknowledgements
- mage: bump markdown lint version
- README: htmlify, update links
- Further fixes to flaky Postgres tests
- Use the same default port for the HTTP API across serve and serve-testing
- cmd: deprecate root-level head and migrate

## [1.29.5] - 2024-03-05
### Changed
- Update protobuf version for reported CVE in protobuf lib
- Ensure that invalid versions do not cause a nil panic

## [1.29.4] - 2024-02-21
### Changed
- spanner: allow spicedb to run with head or head-1 migration

## [1.29.2] - 2024-02-21
### Changed
- address PR feedback
- fixes integer overflow in ForEachChunk
- chunking correctness: add repro for DispatchCheck
- reproduce integer overflow in ForEachChunk
- fixes pgx min connection count always being set to max count
- Reduce memory usage of WriteSchema
- Prevent the staleness of an optimized revision from exceeding the gc window
- Clarify that the datastore-revision-quantization-max-staleness-percent is a float value
- Add a basic Spanner datastore README.md
- Add MySQL version file and README.md
- Tidy e2e go.mod
- Bump google.golang.org/api from 0.152.0 to 0.161.0
- Tidy e2e go.mod
- Bump google.golang.org/grpc from 1.59.0 to 1.61.0
- Tidy e2e go.mod
- Bump github.com/aws/aws-sdk-go from 1.45.26 to 1.50.10
- Bump golang from 1.21.5-alpine3.18 to 1.21.6-alpine3.18
- Bump github.com/prometheus/client_golang from 1.17.0 to 1.18.0
- Update runc dependency for reported vuln in runc
- Bump github.com/jackc/pgx/v5 from 5.4.3 to 5.5.2
- Add a retry to PG connections to reduce test flakiness
- Fix typo in datastore

## [1.29.1-rc1] - 2024-01-30
### Changed
- WIP: Try to remove PG GC flakes
- Add additional datastore revision tests
- Fix NewForHLC for unpadded strings
- Ensure NewForHLC returns an error if the incoming decimal is invalid
- Add additional logging to e2e
- implements schema watch support for MemDB
- fix broken v1alpha gRPC reflection support
- Change telemetry failure to a warning and have Postgres check for its unique ID on startup
- Add invalid permission tests onto the various permissions APIs
- Add some invalid schema tests as per a recently reported error
- Respect dispatch concurrency limits for clusterdispatch

## [1.29.0] - 2024-01-18
### Changed
- Have spiceerrors.MustBugf contain the full stack trace of the bug
- compiler: adds a helper method to require any object type prefix
- makes less verbose CRDB connection balacer
- Switch from a [2]int64 to a struct with an int64 and a uint32, which saves a few bytes
- Test fix for HLC parsing change
- Add interface for extracting the underlying integer(s) from a revision for more efficient storage
- Switch HLC to use to int64s as storage
- Add HLC benchmark tests
- Add timeout to watch buffers
- Watch revision checkpoints using memdb datastore
- expose revision parsing function for tests
- Add Union and Merge to Set
- Lint fixes
- Add ONRSet tests and optimize a bit
- Switch namespace diff to use the shared set type
- Switch from Add to Insert where the return value is not used
- Optimize the set implementation
- Add basic micro benchmarks for Set operations
- tidy e2e mod
- format analyzers go.work.sum
- tidy e2e mod
- tidy e2e mod
- Fix flakiness of HLC changes test
- tidy e2e mod
- Bump github.com/jzelinskie/stringz from 0.0.2 to 0.0.3
- Bump github.com/google/uuid from 1.4.0 to 1.5.0
- Bump golang.org/x/mod from 0.13.0 to 0.14.0
- Bump go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp
- Bump github.com/samber/lo from 1.38.1 to 1.39.0
- Update e2e test
- Switch to using CRDB's decimal library, which reduce memory
- Remove encoding.BinaryMarshaler from datastore.Revision as it appears to no longer be used
- Cleaner revision handling
- pkg/proto: upgrade vtprotobuf to support WKTs
- .github: use public-read-only docker account
- fixes CRDB datastore not reporting proper GRPC codes
- authenticate with docker to raise rate-limits
- Ignore the source position when diffing permission expressions
- Update grpc-health-probe
- Run go get -u golang.org/x/crypto/...
- Move to Go 1.21.5 for external vuln fixed in crypto lib
- Some refactoring and add migration to remove old changestreams in Spanner
- Use logger from context in RedactAndLogSensitiveConnString
- Allow for configuration of the checkpoint heartbeat in watch and wire up schema watch to use the command line flag for all supported datastores
- Add support for schema updates and checkpoint updates to:
- Remove WatchSchema and adjust existing Watch API in prep for a combined Watch API supporting rels, schema and checkpoints
- Add an integration test for schema watch
- fixes regression in compiler.Compile() contract
- fixes regression in Schema Watch led schema caching
- update singleflight library to address race
- decorate spans when they are cached or singleflighted
- fixes datastore command not using the `spicedb` prefix
- Make sure to clone metadata in LR before changing
- schemadsl/compiler: optionally skip validation
- Move the diff library into pkg for use by external tooling
- Bump go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc
- tidy analyzer go.work.sum
- e2e mod tidy
- Bump golang from 1.21.3-alpine3.18 to 1.21.4-alpine3.18
- Bump github.com/spf13/cobra from 1.7.0 to 1.8.0
- Bump google.golang.org/api from 0.149.0 to 0.152.0
- Bump go.opentelemetry.io/otel/trace from 1.20.0 to 1.21.0
- Bump cloud.google.com/go/spanner from 1.51.0 to 1.53.0
- make datastore metrics more representative of the actual underlying datastore
- Fix handling of NULL caveats in Postgres watch
- crdb: don't allow relationship counters to go negative
- Don't call ObserveDuration more than one in the observable proxy
- Fix common/gc backoff initialization
- Formatting - move gcDeleter type below fakeGC in common/gc_test
- Fix race condition when accessing metrics in common/gc_test
- adds missing observability for Watch API
- bulk import: reduce allocations by allocation a value buffer
- Fix gc next interval not being reset

## [1.28.0-rc1] - 2023-11-27
### Changed
- use git SHA for vuln fix in grpc-health-probe
- crdb: fix watch error: %!s(<nil>)
- Disable the default GC process in PG GC tests
- Add a `datastore repair` command for revisions from Postgres backups
- this is an unexpected error so use MustBugf
- log as debug possible loops, and log key
- do not double singleflight cluster dispatches
- do not panic on these execution paths
- return error if traversal key is empty
- log messages for possibly loop on singleflight dispatch
- Add additional expand canonical key tests
- Add missing combined recursive test case

## [1.27.1-rc1] - 2023-11-15
### Changed
- actually instantiate the singleflight proxy
- Revert "clone request"
- clone request
- adds a fallback in case the traversal bloom is not present
- use internal/logging instead of zerolog global logger
- added singleflight expand test
- PR feedback: move everything to dispatch package
- update command with the correct bloom filter P value
- rename field to better convey intent
- less conservative bloom filter P value
- export bloom filter factory method
- PR feedback: do not panic on service controllers
- PR feedback: do not clone dispatch request and modify in place
- test bloom filter is propagated through dispatchers
- change bloom filter in proto from string to bytes
- fix incorrect signature in WASM stub
- fix CVE detected by trivy
- fix inefficient singleflight deduplication with bloom filters
- reproduce dispatch deduplication regression

## [1.27.0] - 2023-11-06
### Changed
- Fix handling of recursive calls via singleflight dispatch
- Bump google.golang.org/api from 0.147.0 to 0.149.0
- Fix the caveat expr limit to be processed by our code
- Fix go.work and go.mod
- Bump github.com/prometheus/common from 0.44.0 to 0.45.0
- Fix flake in Postgres GC revision test by ensuring GC is run
- Fix new lint error found as result of the linter update
- Tidy e2e
- Bump google.golang.org/grpc from 1.58.3 to 1.59.0
- Bump github.com/golangci/golangci-lint from 1.54.2 to 1.55.1
- Bump github.com/google/uuid from 1.3.1 to 1.4.0

## [1.27.0-rc1] - 2023-10-31
### Changed
- do not create custom canceled graph error
- fix Trivy CI failure due to docker vuln
- update to released grpc-health-probe SHA
- adds flags to enable block and mutex profiles
- Fix lint error in error string
- Fix error redaction in MySQL driver
- Redact any passwords found in datastore config errors
- introduces flags to set min/max Spanner sessions
- Remove support for MySQL v5
- Ensure all datastores return an error if accessed after Close
- Fix benchmark test for changes in ReadWriteTx
- derive Spanner gRPC connection count from GOMAXPROCS
- implement postgres+pgbouncer as a separate test
- add options for request/response logs
- log payloads and duration as an integer
- Add schema watch support in Spanner datastore driver
- do not open a new Spanner client for version checks
- datastore/crdb: singleflight features
- datastore/proxy: add singleflight proxy
- dispatch: singleflight expand
- fix grpc-health-probe flagged by trivy
- Run postgres datastore tests with pgbouncer
- enables scheduler metrics
- expose spanner OpenCensus metrics as Prometheus metrics
- add client side open-telemetry Spanner tracing
- Revert "add prometheus metric for spanner elapsed time"
- add prometheus metric for spanner elapsed time
- log quantized revision in RemoteClockRevisions
- singleflight dispatch middleware
- return status error instead of original error
- introduces an index to speed up Watch API calls
- Have diff of namespaces and caveats report changes to comments
- Add a golden unit test for parser associativity
- Add support for experimental secondary dispatching
- Add a map of revision parsing functions by engine kind
- disable spanner's gzip compression
- spanner: revert optimistic concurrency control
- tracing ux: enriches traversal operations
- propagates gRPC errors in graph
- middleware/consistency: fix source of atLeast
- tracing ux: add target to lookup subjects name
- tracing ux: add target to dispatch reachable resources name
- tracing ux: spanner: add span to transaction user function
- tracing ux: remove superfluous span
- tracing ux: spanner: tracing in readonly tx
- tracing ux: remove superfluous now span
- tracing ux: spanner enrich WriteRelationships user func
- tracing ux: remove superfluous span, and enrich
- tracing ux: remove superfluous span
- tracing ux: enrich query relationships span
- tracing ux: show dispatch subproblem in span name
- add missing calls to RowIterator.Stop
- .github: consoldiate build, datastore tests
- .github: consolidate trivvy actions
- .github: use buildjet cache
- .github: move to buildjet
- spanner: s/interface/any
- spanner: use optimistic concurrency for rw txns

## [1.26.0-rc2] - 2023-10-17
### Changed
- middleware: add metric for tracking consistency
- spanner: go back to 1 connection pool for now, while we test the effects of other changes in isolation
- spanner: split read/write, add connpool config
- bump dependencies
- Bump go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp
- Switch health probe to v0.4.21
- Add a test for inclusion of the @ sign in tuple strings
- Switch to custom built grpc_health_probe temporarily
- Update health probe in Dockerfiles
- Update Go version in the Dockerfile
- Update go x/net library for Go http2 vuln

## [1.26.0-rc1] - 2023-10-11
### Changed
- fix list formatting in crdb overlap doc
- Reenable -race on unit tests and fix tests
- Move to Go 1.21.3 to fix a reported Go vuln
- Merge branch 'main' into improve-grouping
- enable the experimental apis in the http gateway
- Fix benchmark test errors
- Update groupItems to use less memory
- Extract the type system and reachability graph into pkg
- Change BulkCheckPermission response to match order of the request
- include SpiceDB version in issue template
- Use lo.Uniq function
- Merge branch 'main' into fast-compare-tuple
- use serialization error reason in Postgres datastore
- PR review: add comments
- enrich GC logging messages
- serialization errors are not well handled in the Postgres datastore: - they tend to be logged by the `pgx` driver with potentially very   large SQL statements - they are not retried, nor there is a backoff. There are also other   types of errors that are not being retried, like `conn closed`   (which pgx considers safe to retry) - they return to the client as unknown instead of the recommended   aborted gRPC code - no information is logged on retries in `debug` level - logs can get extremely verbose due to large SQL and args
- adds new index to help support DeleteRelationships with limit
- add tuned GC index
- Compare tuples without stringifying
- Increase allowable timeout on memdb panic test
- Tidy e2e
- Bump github.com/grpc-ecosystem/go-grpc-middleware/v2
- Bump github.com/prometheus/client_golang from 1.16.0 to 1.17.0
- Bump sigs.k8s.io/controller-runtime from 0.15.0 to 0.16.2
- Order onr comparisons by cardinality for short-circuiting
- runs Postgres tests with all 3 latest versions
- Add better error messaging and tests for memdb serialization error
- Fix a panic in Postgres revision parsing for incompatible ZedTokens
- crdb: better max retry errors (especially with retries disabled)
- Inform Docker of the port that spicedb listens on
- *: go 1.21 API changes
- Update to grpc 1.57 and golang 1.21.1
- Bump go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc
- Bump google.golang.org/grpc from 1.56.2 to 1.57.0

## [1.25.0] - 2023-09-01
### Changed
- Bump golang from 1.20-alpine3.18 to 1.21-alpine3.18
- Fixes for the watching schema cache
- README: swap to our DNS for rpm/debian packages

## [1.25.0-rc4] - 2023-08-30
### Changed
- removes streaming bulk check API, which is not implemented
- Invert the flag for the new schema watching cache

## [1.25.0-rc3] - 2023-08-30
### Changed
- Revert "Add support for `self` keyword in schema for referencing a resource as a subject"

## [1.25.0-rc2] - 2023-08-29
### Changed
- Have invalid cursors return a proper error
- Add dispatching support for self
- Clarify self in comments
- Add self support to compiler
- Add generator and builder support for self
- Add lexer and parser support for self keyword in schema
- makes PaginatedIterator.Close() idempotent
- introduces fix for dotenv log messages in cobrautil
- chore: add github issue templates
- README: update fury instructions to add GPG
- changes logging level for logger used during startup
- Implement a schema cache which updates using a SchemaWatch API
- Update authzed-go to increase allowed schema size

## [1.25.0-rc1] - 2023-08-23
- Merge branch 'main' into test-envfile
- switch to authzed/cel-go
- postgres: adds tracing to revision GC
- *: refresh README/CONTRIBUTING
- Load config values from ./spicedb.env
- Fix handling of resources chunk in error state and add some tests
- Address review comments
- Fix flakiness in grouping test
- Move to latest version of authzed-go
- Address review feedback
- Add bulk check to the consistency tests
- Address (my own) review feedback
- introduce Bulk Check API Support
- Fix flaky LR dispatch count test
- pin spanner emulator to 1.5.8
- disable datastore hedging by default

## [1.24.0] - 2023-08-10

## [1.24.0-rc3] - 2023-08-10
- internal: drop last usage of legacy proto lib
- *: extract gRPC balancer into external library
- Significantly reduce the number of goroutines used in the checking stream

## [1.24.0-rc2] - 2023-08-09
- e2e: go mod tidy
- pkg/schemadsl/compiler: add a unit test for prefixedPath
- all: support multiple slashes in object and caveat names
- .github: use 4-core runners for more jobs

## [1.24.0-rc1] - 2023-08-07
- Add CreateTouchDeleteTouchTest
- Add test
- Fix mysql WriteRelationships query
- Make sure to output logs for mage:test
- bump github.com/dalzilio/rudd
- Add integration tests for graceful shutdown of SpiceDB
- *: drop legacy proto library
- Clarify that development error positions are 1-indexed
- Remove trivy scanning of debug images
- Change how dispatches are counted in LookupResources
- Change MySQL definition columns to LONGBLOB in prep for supporting larger definitions
- Move to Go 1.20.7 to fix vuln in go stdlib
- Bump google.golang.org/api from 0.133.0 to 0.134.0
- Bump github.com/outcaste-io/ristretto from 0.2.2 to 0.2.3
- Bump github.com/go-co-op/gocron from 1.30.1 to 1.31.0
- Bump github.com/aws/aws-sdk-go from 1.44.307 to 1.44.314
- proto: add missing transitive dependency
- magefiles: add buf format linter
- actually disable gateway server when disabled
- internal/datastore/proxy: record zero relationships read
- goreleaser: cut individual brew versions
- update prometheuspb module to new upstream domain
- removes Spanner phased migrations for upcoming release
- add test for pgx query tracing
- add OTEL tracing to pgx Queries
- Fix length of slice in partial eval of caveats
- bump all dependencies
- Change IP Address type to opaque, since it doesn't export anything besides its one method
- Small suggested fix
- Add a parser test for use of optional fields in caveat expressions
- Enable optional types support in caveats
- Change deprecated CEL function call
- Ensure that the properly typed parameters environment is passed to the CEL deserialization, to be used by expression evaluation
- Switch from AnyType (which represents the *proto* Any type) to DynType, which is equivalent to the *Go* any type
- Update CEL to v0.17.1 and use new env.PartialVars and types.Unknown to properly get missing variables from caveat expression execution
- move as many tools as possible into the mage module
- Remove the old "dashboard" from SpiceDB
- Remove unused usersets splitting from datastore interface
- adds a lint:trivy command to mage
- scan goreleaser nightly image with trivy
- update analyzers
- fixes incorrect usage of backoff in GC

## [1.23.1] - 2023-07-14

## [1.23.1-rc1] - 2023-07-14
- Have remote dispatches be counted in the dispatch count
- Fix flakiness in hedging test
- Fixing a bug in the comment
- bump grpc health probe to v0.4.19

## [1.23.0] - 2023-07-12

## [1.23.0-rc4] - 2023-07-12
- update to building with go 1.20.6 to fix vuln reports
- updates to pgx 5.4.2

## [1.23.0-rc3] - 2023-07-11
- add missing Rows.close()
- fixes memory leak in pgx

## [1.23.0-rc2] - 2023-07-07
- Reenable support of deleting caveats relationships without specifying the caveat
- crdb: stats should use follower reads

## [1.23.0-rc1] - 2023-07-03
- Update e2e mod
- Bump github.com/envoyproxy/protoc-gen-validate from 1.0.1 to 1.0.2
- Update generated protocol buffers
- Update to latest grpc and grpcutil
- Update e2e mod
- Bump github.com/golangci/golangci-lint from 1.52.2 to 1.53.3
- Deduplicate entrypoints for reachability graph
- Fix flakiness of LR test around duplicate caveated results
- genutil/slicez: rehome chunking logic
- gentuil/mapz: move Set type, impl ZeroLogMarshal
- genutil/mapz: init
- pkg/genutil/slicez: init
- *: s/stringz.SliceContains/slices.Contains
- don't use the init context for crdb pool initialization
- restores the streaming otel middleware
- update to grpc-middleware v2 rc5
- more robust nil caveat context handling for Spanner
- Deduplicate resources in Check dispatch
- Reconnect logging and tracing into the disconnect LR/RR context
- f
- Add check traces to assertion errors for better debugging
- Reduce allocations necessary for cursor work
- Add dispatch version to dispatch cursors to prevent mismatched cursor usage
- Remove intermediate names from cursors to reduce their size
- Remove the revision from dispatch cursors to make them smaller
- goreleaser: sort tags by creation date, don't upload RCs to homebrew
- Add durations to check debug tracing
- Change the max depth exceeded error to be well-typed

## [1.22.2] - 2023-06-21
- disambiguate order assertion in streaming APIs
- reuse existing IsInTest function
- only add dependencies where actually needed
- make sure both chains have the same middlewares
- use composite interface for middleware types
- wrap middleware in an order asserting interceptor
- decouples middleware modifications

## [1.22.1] - 2023-06-19

## [1.22.1-rc1] - 2023-06-19
- Parallelize the chunked dispatches in reachable resources
- Redo how context is handled around limits
- Fix generated membership for the development package when using indirect caveats
- correct SetGlobalLogger in main.go

## [1.22.0] - 2023-06-12
- Change CREATE error into a well-typed error with details

## [1.22.0-rc8] - 2023-06-12
- default to disabled to make sure SpiceDB does not fail to start
- Add additional test type check
- Move to go 1.20.5 to fix a reported vuln in Go libs
- Avoid an extra datastore roundtrip once all results have been found
- Have the stream timeout middleware report a cause for the context cancelation
- Ensure that context cancelation in LR propagates
- document request overlap in crdb/README.md
- crdb: report readiness as soon as min connections are available
- add request-scoped overlap key option for crdb

## [1.22.0-rc7] - 2023-06-07
- Deduplicate LookupResources results when limits are unspecified
- Fix a deadlock in preloaded task runner
- correctly chunks large bootstrap files

## [1.22.0-rc6] - 2023-06-07
- Switch ReachableResources to use BySubject ordering
- Fix issue with BySubject ordering in memdb

## [1.22.0-rc5] - 2023-06-05
- Re-parallelize the checking of resources in LookupResources
- Bump github.com/lib/pq from 1.10.7 to 1.10.9
- Bump github.com/Masterminds/squirrel from 1.5.3 to 1.5.4
- Bump go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp

## [1.22.0-rc4] - 2023-06-01
- more robust server defaults using optgen defaults
- move TerminationError to spiceerrors package
- Parallelize the entrypoint computation in ReachableResources
- introduces a flag to define the maximum relationship context size
- make the linter happy
- enrich with metadata termination-causing errors
- introduce termination.Error to support termination-log
- Use correct error type for duplicate relationships
- run go run mage.go lint:go
- e2e: update go mod and sum
- internal/services/v1: return ZedTokens on schema operations
- update authzed-go to bring in schema ZedTokens
- allow connpool flags to be written as conn-pool
- Don't emit a warning for gRPC cancelation
- Add a test to ensure LookupResource cursors are stable across calls
- fix crdb datastore readiness log message

## [1.22.0-rc2] - 2023-05-25
- Also remove the possible panics from the cursor updating code
- Remove the panic on combineCursor
- Additional SQL tests for order BySubject

## [1.22.0-rc1] - 2023-05-24
- Avoid reallocation of slice in preloaded task runner
- fix typo in StreamReadTimeout option
- Add ability to query relationships, sorted by subject
- Update CONTRIBUTING with info on mage
- internal/services/v1: add descriptions for how bulk export works
- internal/services/v1: parameterize default and max batch sizes
- internal/services/v1: refactor export tuple iteration to defer close cursors
- go mod tidy
- internal/services/v1: rename bulkload to bulkimport
- internal/services/v1: add bulk export to the experimental service
- Fix closeafterusage linter for underscores in assign statements
- healthcheck test: don't share testing.T with subtests and speed up test
- run the test suite for MySQL8 as well
- handle nullable information_schema.tables.table_rows
- increase timeout for health check tests
- Remove parallel and log to see why test is flaky
- Force custom plans in Postgres
- Add support for cursors on read relationships
- Add missing relationship after rebase to deletion test
- Rebase fixes
- Fix lint warning
- Address review feedback
- Use shared consistent hashing for caveat contexts
- Address review feedback
- Ensure resourcesubjectsmap produces a stable, sorted order
- Requested changes
- Make sure to cancel all reachable resources contexts when LR has completed
- Fix benchmark tests for changes to test runner
- WASM hasher fixes
- Lint fixes
- Have consistency use the revision found within a cursor, if specified
- Have consistency tests issue cursored LR calls
- Add the revision into the dispatch cursor to ensure it doesn't change
- Add cursor support for LookupResources
- Reimplement LR in fully streaming mode
- Rename Lookup -> LookupResources and make streaming in prep for cursor support
- Add support for a limit on ReachableResources
- Make reachability graph have a stable ordering
- Initial cursor changes for supporting cursors in ReachableResources
- Skip emitting watch changes in memdb that are empty
- Address review feedback
- Update bulk write for multi-phase migration
- Add multi-phase migration for spanner's use of changelog table vs changestream
- Change spanner to use native changestreams support for Watch API
- Change memdb WriteRelationships to skip updating TOUCHed rows that do not have difference in caveat name or caveat context
- Change MySQL WriteRelationships to skip updating TOUCHed rows that do not have difference in caveat name or caveat context
- Change CRDB WriteRelationships to skip updating TOUCHed rows that do not have difference in caveat name or caveat context
- Add a test to ensure that TOUCH operations that do not change caveats also do not raise watch events
- Use UPSERT for TOUCH in Postgres
- Change namespace+relation validation to use a single DB lookup
- Add unit tests for all type system operations
- add leaky bucket for crdb health checking, rate limiter pgx connections
- clean up pgxcommon
- crdb: add a connection-balancing retry-aware connection pool
- README: add Netflix <> Caveats shoutout
- Add support for limited deletion of relationships
- Add additional eventually to integrate test to try to reduce flakiness
- Small feedback fixes
- Add retries around spawning images in servetesting
- Short circuit union and all when there is a single child
- Switch check dispatch to use a special preloaded task runner
- Fix reachable resource TTU over subject relations
- only run image tests in cmd/spicedb
- Add require.Eventually to remove flakiness on image tests
- balancer: rewrite the consistent hashring balancer to avoid recomputates
- add magefiles for running project commands
- Skip logging of serialization errors in pgx
- Fix comment
- internal/services/v1: add a bulk load test
- all: address linter warnings
- e2e: go mod tidy
- internal/services: move bulk load to experimental service
- internal/services: move v1 rewriteError to a shared location
- pkg/tuple: add a neutral relationship interface
- services/v1: implement BulkLoadRelationships
- datastore/spanner: implement bulk load
- datastore/mysql: implement bulk load
- datastore/memdb: implement bulkload
- datastore/crdb+postgres: implement BulkLoad
- pkg/datastore: add an option to disable retries on RWTs
- datastore: add a new BulkLoad method for quickly writing lots of relationships
- fix potential missing PSK error and defer leak
- updates dockertest to improve error msg with pool.Retry()
- reduces flakiness by using dockertest pool.Retry
- move to paths-filter build-skipping strategy
- Regenerate protos for new version of buf
- Add better logging of config
- add path filtering to merge_group too
- fixes buf diff caused by bumping version in authzed/actions/buf-generate
- prepares for merge-queue enablement

## [1.21.0] - 2023-05-11
- README: add missing sudo
- goreleaser: fix brew PR config
- Add a datastore benchmark for mixtures of CREATE and TOUCH

## [1.21.0-rc1] - 2023-05-08
- Fix serialization of custom types in caveat context
- Skip initializing the telemetry collector if telemetry is disabled
- Move to Go 1.20.4 to fix a reported vuln in Go
- Regenerate protos
- Tidy e2e
- Cherrypick 9262817fc91872ed8f336934262a9fce09880128
- build(deps): bump cloud.google.com/go/spanner from 1.42.0 to 1.45.1
- build(deps): bump github.com/dustin/go-humanize from 1.0.0 to 1.0.1
- build(deps): bump go.buf.build/protocolbuffers/go/prometheus/prometheus
- doc: add information on when `insecure` is okay to the crdb readme
- Fix flake in PG test
- internal/datastore/common: test that all revisions eventually get selected
- internal/datastore/common: only prune old revisions upon mutations
- pkg/cmd/server: extend cache lifetimes to take into account revision staleness
- pkg/cmd/datastore: make revision staleness configurable from the command line
- internal/datastore/common: crossfade revisions when we switch
- Add pagination support to ReverseQueryRelationships
- Add v1 integration test for stream timeout
- Fix flake in the proxy test
- Fix issue where Watch in Postgres was looping endlessly
- dockerfile: fix path to include /usr/local/bin
- pkg/cmd/server: enable gzip server compression
- Fix typo in comment
- use expanded tuple comparison query condition in MySQL
- use expanded tuple comparison query condition in CRDB
- Move to goreleaser v4
- Revert "Revert "goreleaser: publish PRs to brew and not commits""
- Revert "goreleaser: publish PRs to brew and not commits"
- Revert "goreleaser: publish PRs to brew and not commits"
- Merge branch 'main' into paginated-datastore-additional-fixes
- Add a prometheus counter over the different kinds of write operations
- remove 2 allocations from ReadRelationships streaming loop
- correctly return gRPC code on cancellation
- datastore/memdb: remove extra tuple allocation from cursor filter function
- pkg/datastore: temporarily remove the BySubject sort order
- pkg/datastore/test: add a conformance test for iterator error handling
- datastore/common: add tuple strategy to resume paginated responses
- services/v1: make the datastore page size a flag
- services/v1: use paginated datastore API for ReadRelationships
- datastore/all: implement pagination
- datastore/memdb: implement pagination
- pkg/datastore/test: add a pagination test suite
- pkg/datastore: add cursors to the iterator interface
- all: fix linter errors
- pkg/datastore: move options to pkg and add order and start position

## [1.20.0] - 2023-04-19
- Revert "goreleaser: publish PRs to brew and not commits"
- Check the defaults for hashring construction
- Fix flakiness in serve-testing with pre-populated data by squashing the memdb revisions after the data is populated
- pkg/cmd: disable cmdline profile
- goreleaser: publish PRs to brew and not commits
- Add WASM development test for extended IDs
- Tidy go mod files for recent changes
- Update API protos and add additional testing
- integrationtesting: add a consistency test for extnded IDs
- all: extend object ID processing
- pkg/datastore: add extended object ID storage test
- datastore/mysql: extend object ID storage
- add backoff to crdb tx resets
- Export source repository location
- Allow for reuse and replace of the serverversion middleware
- Fix bound variables in e2e
- Bind all loop variable uses in function closures
- makes hashring spread configurable
- Make sure to bind vars in table driven tests
- Fix deduplication bug in reachable resources
- Set a subdictionary key for pgx logs adaptor
- Move to Go version 1.20.3 to fix reported vuln in Go 1.20.2
- datastore/benchmark: refactor to use uniform initialization
- datastore/benchmark: add a datastore driver benchmark
- Move to v4 of kuberesolver
- Fix flakiness of graceful termination test
- gomod: bump cobrautil
- cmd/server: configure hashring replication factor using a singleton picker
- Revert "introduces configurable dispatch hashring"

## [1.19.1] - 2023-04-13

## [1.19.0] - 2023-03-30
- add flags for connection max lifetime jitter
- postgres, crdb: update pgx to v5

## [1.18.1] - 2023-03-29
- introduces configurable dispatch hashring
- extend graceful termination test
- datastore/postgres: use implicit transactions for reads
- datastore/crdb: use implicit transactions for reads
- Skip checking of relation on direct computed_userset
- reverts caveat covering index
- Add better logs for when datastore or dispatcher is not ready
- fix 1.20 linter errors with deprecated rand
- prevents misleading log messages by avoiding Rollback if committed
- Change schema compiler to squash union and intersection trees
- use cgr.dev/chainguard/busybox as base instead of distroless.dev/busybox
- datastore/postgres: fix invalid slice appends in snapshots
- datastore/postgres: fix invalid slice appends in snapshots
- rebase on top of main and fix conflicts
- Update doc text of new interceptor
- Have GC index only contain those relationships that actually need to be GCed
- Add additional querying tests
- Add a GC covering index
- redesign query interception
- reconcile covering index branch and covering index test
- add caveat covering index for PostgreSQL
- Add Index Only select test to PG datastore

## [1.18.0] - 2023-03-21
- datastore/postgres: fix invalid slice appends in snapshots
- spanner: close row iterators when done
- Bump golang.org/x/mod from 0.8.0 to 0.9.0
- changes the order gRPC prom middleware
- fixes comeback of the MacOS microsecond precision problem
- datastore/postgres: synthesize revisions instead of joining for namespace and caveats
- pkg/datastore/test: test assumption about namespace synthesized revs
- fixes GC behaviour with out-of-window head revision
- mysql: head revision is always valid
- adds revision GC test to all datastores
- fixes failures with FullyConsistent when GC window elapses
- propagate option to disable stats in spanner datastore
- internal/datastore: default to maxing connpools
- internal/testserver: set pg max connections
- pkg/cmd: default writepools to half readpools
- internal: consistency for crdb/pg options
- internal/datastore: split conn pools for postgres
- internal/datastore/crdb: split read/write connpools
- .github: bump to Go 1.20.2
- README: add debug containers
- README: refresh with more dev instructions
- makes the caveat context size configurable
- datastore/postgres: make proto revisions backward compatible
- datastore/postgres: rename the visible function to txVisible
- datastore/postgres: remove commented out imports
- datastore/postgres: adjust prepared queries and their comments
- datastore/postgres: add doc comments
- datastore/postgres: check the presence of txids consistently
- datastore/postgres: eliminate transactions using index before running visible function
- datastore/postgres: silence panic linter for revision serialization
- datastore/postgres: switch to DB snapshots as primary component of revisions
- Bump golang from 1.19-alpine3.16 to 1.20-alpine3.16
- Cherrypick 0c404922d6485727a373202ae5cfaf553d7ce2cd
- Bump github.com/rs/zerolog from 1.28.0 to 1.29.0
- Bump github.com/hashicorp/go-memdb from 1.3.3 to 1.3.4
- Add prometheus metric for GC failure in datastore

## [1.17.0] - 2023-02-22
- Skip loading of head revision on write calls
- Have validation for WriteRelationships batch load namespaces
- fix linter error and vuln
- Use the shared relationships validation in dev package
- Move to Golang 1.19.6 to bring some security fixes
- does not return an error if GC windows aren't aligned
- fix spanner telemetry collection
- adds caveats to AppliedSchemaChanges
- improve error message and add link to spicebd.dev
- Fix MySQL parseTime check to use the DSN lib
- Remove now-unused caveats flag
- Remove experimental flag for caveats
- Change caveats run to bulk lookup the caveats definitions
- make cache collector unregister on close
- makes dispatch metrics toggleable
- pkg/cache: implement a central collector
- fix linter error
- change release notes update mode

## [1.16.2] - 2023-02-03
- .github: use authzed/actions/setup-go
- .github: use 8 cores for codeql
- Fix streamtimeout and add tests for it and remote dispatch timeout
- Add timeout on dispatched API calls to ensure they are all eventually terminated
- Add stream timeout to dispatch
- Add streaming API timeout interceptor to V1 APIs
- Change LookupResources to use the task runner to ensure its workers are canceled when the context is canceled
- Fixes after rebase
- Add caching of caveats and of the Lookup calls
- Change namespace and caveat list methods to return both the definition and the last updated revision
- Break out ListAllCaveats and LookupCaveatsWithNames to mirror the namespace methods
- Rename LookupNamespaces and ListNamespaces
- Rename ReadNamespace to ReadNamespaceByName to match caveat function name
- Add common interface for not found errors in datastore
- Add explicit tests to ensure namespace and caveats not found return the expected errors
- Add Postgres integration tests when commit_timestamp is not enabled
- split into two goreleaser files
- .github: add missing permissions to nightly
- .github: add missing env to nightly workflow
- Add release process for dev builds (nightlies)
- Fix hanging of REST gateway on close of SpiceDB
- Fix flakiness of cert test
- Tidy e2e
- Bump github.com/prometheus/client_model from 0.2.0 to 0.3.0
- Bump go.buf.build/protocolbuffers/go/prometheus/prometheus
- Bump golang.org/x/tools from 0.4.0 to 0.5.0
- Update GHA version
- introduce BootstrapFileContents
- introduce BootstrapFileContents
- Add an explicit error if parseTime is missing from MySQL URIs
- Add expiration consistency test and fix conversion issue with caveat context back into structpb for timestamps/durations
- Update quay consistency test to support geo-restricted anonymous users
- Remove return left in previous PR
- Add some additional MySQL GC tests
- Only write caveats that have been possibly updated
- Remove TODOs in caveat CEL code
- pkg/cache: default TTL of 2x quantization window
- Update names of errors to make them read better
- Tidy all the mods
- Add better, well-typed errors for caveat expression failures
- Add consistency test using a map in caveats
- Add a consistency test for the ipaddress type for caveats
- fixes positional argument errors failing silently
- Part 2 of consistency tests using caveats
- Add additional option to CEL to compile caveat macro expressions
- .github: explicit github token for buf-generate
- Make sure to catch error tokens in caveat parsing
- fixes problem with CEL not resolving protobuf types
- Expose the V1 API debug information in dev package

## [1.16.1] - 2023-01-17
- pkg/tuple: add tests for ref string funcs
- internal/dispath: use tuple funcs in span attributes
- internal/namespace: fix canonicalization test
- tuple: deduplicate string manipulations
- *: use strings.Cut where applicable
- *: minimize usage of Sprintf in the critical path
- Fix flake in debug tests
- Allow for configurable batch sizes for testing and change consistency test to use smaller batch sizes
- Add additional consistency test cases
- Add a distinct validation error type for schema write
- Add consistency test for reading relationships
- Cleanup lock handling in task runner
- Consistency test reimplementation
- Add an API test for deleting a relationship that does not exist
- adds Ctx() calls everywhere
- adjust test config to use the previously provided defaults
- make RegisterDatastoreFlagsWithPrefix use the default method as source
- make sure flag defaults and DefaultDatastoreConfig are the same
- Update reported min version for Postgres
- refactor datastore flags to make them reusable
- Update README
- Fix deadlock in reachable resources in certain conditions
- Ensure there are no dangling goroutines in ReachableResources, LookupSubjects or LookupResources

## [1.16.0] - 2023-01-04
- Parallelize the datastore consistency tests
- Fixes for rebasing
- Make integration test suite timeout longer for the new tests
- Switch to a simplified direct check that uses two queries
- Update build tags for new consistency test
- Add variation of the consistency test suite that runs all consistency assertions under each datastore
- Improve performance of direct checks on relations by using targeted SQL queries in supported datastores
- Change validation file loader to use chunks for updates
- Add benchmark test for a very wide direct relation
- Regenerate validate files for changes
- Fix spanner imports after their move of types
- Remove spaces for comparison due to recent changes
- Regenerate e2e for changes
- Cherrypick 8e3d6c576abc1a6b4aa5ec54f1306b041670fbc6
- Cherrypick 986055d052e2f1fc0c0fc57a91e974430412accc
- Bump golang.org/x/tools from 0.3.0 to 0.4.0
- Bump github.com/rs/cors from 1.8.2 to 1.8.3
- Bump github.com/cespare/xxhash/v2 from 2.1.2 to 2.2.0
- reference libraries through awesome spicedb
- Add support for caveats in development package
- Add datastore GC command to synchronously run GC
- Update pkg/cmd/datastore/datastore.go
- Fix panic in new code after rebase
- Add test to ensure MustBugf raises a panic during testing
- Feedback and rebase updates
- Add panic check analyzer to the lint check
- Address all invalid uses of panics as determined by the linter
- Add a linter for identifying undesirable panic statements
- Add a chunk size check to be extra careful
- Fix bug in reachable resources that was causing extra work
- Add accessor in the dev package for V1 API
- Add exponential backoff to the GC worker for datastores
- datastore/proxy: add prom metrics to datastore operations
- redesign middleware modification API
- redesigns middlware options for RunnableServer
- Add test for dispatch metadata on all endpoints
- caching proxy did not close ristretto
- cancellation is not a bad thing, make it info
- address leak on cancellation
- fix leaking resources when Complete() errors
- Combine the debug tests and add additional testing
- Fix debug tracing for batch dispatches
- Change the resourcesSubjectsMap in reachable resources to be used read-only after construction to prevent overlapping access
- Add read-only multimap
- gomod: update cobraotel to support sample ratios
- change description wording
- Change confusing flag help output.
- Remove spaces found on GHA vs local testing
- Add another relationship parsing test
- Fix revision checking in memdb to allow for past `now`
- Add support for caveated subjects in expected relations
- Add caveat support to expand dispatch
- Update dispatch and core protos in prep for caveats in expand dispatch
- Add a conditional caveat test for debug
- Tidy e2e
- Add caveat name to eval info in debug trace
- Tidy go mods
- Add support for caveats in the debug of Checks
- Update to latest authzed-go
- Add retries to the estimated size test to remove flakiness
- Add prom metric for number of batch check dispatches
- datastore/postgres: sylistic changes
- cmd/datastore: increase default watch buffer length to 1024
- datastore: use pre-declared revision key functions
- datastore/common: remove vestigial type definition
- datastore/common: convert pointer to struct copy for performance reasons
- datastore/postgres: use pg_visible_in_snapshot and repeatable read isolation to improve watch
- datastore: make change tracking code generic, add batch change loading in postgres
- cmd/datastore: expose the watch buffer length as a datastore param
- datastore/postgres: remove some old test debug code
- Add warning when PG max connection count is lower than min
- datastore/crdb: use a well-typed map for database connection string
- datastore/crdb: detect version and pick appropriate watch query
- datastore/crdb: re-enable watch API privilege test
- datastore/crdb: lower the watch timeout back down with min_checkpoint_frequency param
- datastore/crdb: upgrade to v22.2.0 to get arm support
- Move to golang 1.19.4
- Link to annotated paper
- Adjust estimated query count metric to only count dispatch if it was necessary
- Fix go.mod for grpc import and regen e2e
- Bump github.com/cenkalti/backoff/v4 from 4.1.3 to 4.2.0
- Bump go.opentelemetry.io/otel/trace from 1.10.0 to 1.11.1
- Cherrypick 730715816d4c84845014cc5cad4081024f829dd7
- Cherrypick 53228cda0637f23410a45df0a84fe7f3612e1b3c
- Bump golang.org/x/tools from 0.1.12 to 0.3.0
- Add support for caveat name and context to tuple syntax
- Remove old error message from CRDB test
- Add a metric for estimated check direct queries
- Fix metadata on ErrCannotWriteToPermission
- Fix the flake in the estimated size test for nsdefs
- internal/datastore: remove unused lock
- Switch the namespace cache to use estimated costs and no serialization
- Add configurable concurrency limits per dispatch type
- Add trace debugging setting for use in the development API
- Fix returning of debug information in Check for intersections and exclusions
- Immediately close tuple iterators once we are done working with them
- Add new lint check to the GHA linter pass
- Add custom linter to find usages of specified types where Close() is not immediately synchronously called
- Fix memdb to always generate unique revision IDs
- Return a more descriptive error for watch when not enabled
- Add brief sleeps to fix flaky test on macos
- replace dgraph ristretto with outcaste
- redesign gateway close
- fix cert test hanging indefinitely
- gracefully terminate HTTP Gateway

## [1.15.0] - 2022-11-21
- Catch nil values for FoundSubjectsByResourceID map and return as errors
- Note on running integration tests
- Add datastore test for writing empty array caveats
- Add len checks to WriteCaveats before attempting to write nothing
- Add context and default timeout for validationfile loading
- Return InvalidArgument if caveats are disabled in WriteRels call
- Improve the error message for duplicate rels within a single WriteRelationships call
- Fix for PG when schema is specified in the db url
- Have validationloader validate relationship updates as well
- Abstract the relationship update validation code into its own package
- datastore/postgres: remove the compensation code for migration phases
- fixes broken docker compose link
- Small builder improvements
- Update e2e mod
- Fix import ordering after move of util package
- Switch remaining caveat builder functions to use the shared functions
- Move datasets for dispatch into their own package
- Rename membership package to `developmentmembership` to give context on when its used
- Move caveat helper functions for building into the caveats package and add some tests
- Move generic utilities into pkg
- Move all protocol buffer equality checking into testutil
- Make sure ReadSchema returns caveats as well
- re-enable v1 services API test
- implement StableNamespaceReadWriteTest with caveats
- Fix validating datastore to ensure that ellipsis is the only relation on wildcards
- Add better tracking of flaky validator and make sure to clone tuples for testing
- Add support for caveats in LookupSubjects API
- Fix test flake in loader by sorting the expected tuples
- test rewriteError handles context errors
- let potential status.Status errors propagate
- reduce logging noise
- return proper gRPC errors on context cancellation and deadline errors
- do not log error if context was cancelled
- reduce log noise by not logging out canceled queries
- Provide additional capabilities around schema writing
- Refactor the datastore testfixtures for better code reuse
- Fix observable proxy to use the more efficient namespace lookup
- Refactor schema handling into nicer helper methods
- Remove support for the v1alpha1 API
- Add caveats support to LookupResources
- Change parallel checker to use ComputeBulkCheck
- Move ComputeCheck into its own package and add support for bulk checking
- Implement support for caveat tracking in reachable resources
- Change dispatch message format for LookupResources in prep for supporting caveats
- Update BaseSubjectSet to support caveat expressions

## [1.14.1] - 2022-11-07
- cmd/serve: fix deprecated usage of jaeger
- Fix size of the expand channels to prevent goroutines from blocking
- Add goleak checking to all the dispatch tests
- datastore/cache: clear the RWT namespace cache when writing namespaces
- Fix panic in validationfile loader when no schema is specified
- explicitly document why cache is disabled
- disable caching in wasm tests
- do not cache protobuf
- also prevent poisoning in build

## [1.14.0] - 2022-11-03
- .github: update Go version
- goreleaser: fix version passed to linker
- goreleaser: revert SBOMs
- Tidy e2e after updates
- Cherrypick aa5a880247a9e3af56d0787d9e19abdce96296b0
- Bump github.com/go-co-op/gocron from 1.17.0 to 1.17.1
- Bump google.golang.org/grpc from 1.49.0 to 1.50.1
- Bump go.buf.build/protocolbuffers/go/prometheus/prometheus
- Add shorter timeouts and better config to gRPC dialing in tests
- datastore: DeleteNamespace => DeleteNamespaces
- Change experimental caveats flag to be handled at the service level
- do not run CRDB migration in transaction
- pg: move column defaults to backfill migration
- specifies cache-dependency-path to prevent poisoning
- Update golang.org/x/text to fix reported CVE
- Update authzed-go for the additional validation rules
- internal/datastore: separating context proxy
- pkg/cmd: observe caching datastore
- internal/datastore: add observable proxy
- fixes caching of loop variable in the wrong place
- introduce caveat support in MySQL
- some improvements in Postgres caveat implementation
- do not cancel all datastore tests if one fails
- use setup-go caching
- parallel integration tests
- parallel PSQL tests
- improvements to CI
- parallelize consistent hashring test
- refactor: add context to write methods within a datastore transaction
- Breakout datastore tests into a matrix
- Breakout the integration test suite from unit tests
- Mark devtools gRPC endpoint enabled by default for the serve-devtools command
- Don't return the caveat key in the ObjectDefinitionNames in v1alpha1 WriteSchemaResponse
- datastore/postgres: stop casting xid in queries altogether
- Elide updates of namespaces which have not changed at all
- .github: fix yamllint warnings
- include sboms in release
- Tidy e2e mod
- Improve comment on caveats source
- Add additional tests around caveat deserialization and type checking
- Abstract out the common caveat context check
- Lint fixes
- Add comment to referencedParameters
- Rename namespacePath in compiler
- Unexport compileCaveat, since it is only used for testing
- Fix typo
- Fix error marshaling
- Add some additional set tests
- Change slice to start as nil
- Change all caveat dispatch tests to use caveats defined in schema
- Fix error marshaling
- Fix comment formatting
- Have the validation file loader write caveats found in schema
- Small schema compiler improvements
- Change schema compilation to take in a single schema
- Remove writing of caveats in non-caveated standard datastore test data
- Fix tests by making ListCaveats always return, even if caveats are disabled
- Add support for defining caveats in schema, and associated type checking of their use
- implement caveats for spanner
- v1alpha1: re-expose revision hashing functions for downstream software
- datastore/postgres: fix gc to only delete data below cleaned transaction xmin
- datastore/postgres: add index to accelerate aggregate xid8 queries
- datastore/memdb: drop vestigial revision in snapshot reader
- datasore: test that all datastores serialize revisions compatible with dispatch
- datastore/postgres: add a test for inverted revisions on concurrent transactions
- refactor: make the decimal revisions implementation public
- e2e: update go.mod and go.sum
- refactor: switch datastore.Revision to a comparable interface everywhere
- datastore/postgres: add the snapshot xmin as the fractional part of the datastore revision
- introduce caveat support in CockroachDB
- sever namespace read context
- implements "isSubtreeOf" function for maps
- fix usages of zerolog global logger
- test delete caveated rel with RelationTupleUpdate_DELETE
- fix more instances of zerolog marshall recursion
- Add some additional lexer test cases
- Add missing generator test case
- Add additional invalid relationship test
- Add additional type system validation tests
- Fix error logging
- Add additional namespace diff tests around caveats
- Rename AllowedPublicNamespaceWithCaveat
- Rename Caveat to AllowedCaveat in the ns builder package
- Add additional encoding and type tests
- Add min and max validation to caveat type references
- Add missing parameter types to caveats test data
- Temporarily ignore errors from creating caveats in datastore tests
- Fixes and improvements after rebasing
- Comment out caveat-based tests until all datastores support them
- Add type checking of caveats used for subjects in relationship updates
- Lint fix
- Update namespace diffing and schema checking to handle allowed types with caveats
- Drop context from write calls on caveat interface, as it isn't necessary
- Add full type checking and conversion of caveat parameters
- Add validation of caveats as part of the type system validation
- Change type system to use a Resolver interface for clearer instantiation
- Add schema generator support for the new with syntax
- Add compiler support for the new `with caveat` syntax
- Add lexing and parsing for *referencing* caveats in relation definitions
- Add a namespace proxy cache test suite using a real datastore
- internal: store serialized protos in caches
- Tech Debt cleanup: move LogOnError into common datastore package
- bump to go-grpc-middleware/v2 everywhere
- unwrap error before passing it to zerolog in custom marshallers
- use spicedb logging package
- add variadic name filter to ListCaveats
- store definition proto instead of serialized expression
- adds exhaustive testing of CREATE/TOUCH semantics
- update authzed-go with PermissionService reqs /w context
- Add zerolog expr statement check to the linter
- Add expression statement lint check to catch missing Send or Msg on zerolog statements
- Change lookup resources to use batch checking for non-union results
- Add benchmark test for a lookup resources call with intersections and exclusions
- fix calls to logger that are not being sent
- rn Caveat->CaveatDefinition, Expression->SerializedExpression
- add missing caveats to migration assumption test
- introduce relationship filtering by caveat name
- introduce ListCaveats method in CaveatReader
- make Datastore implement CaveatReader/Storer
- run Postgres phased tests in parallel
- return revision in ReadCaveatByName
- change signature of DeleteCaveats to use names
- add timeout to datastore docker boot
- adds caveat support to postgres
- improve logging for spicedb migrate cmd
- goreleaser: push linux packages to gemfury
- .github: bump goreleaser, pro
- datastore/postgres: pass migration batch size from the command line
- datastore/postgres: provide alternate paths for finding the implicitly named pkey
- datastore/postgres: test all phases of migration
- datastore/postgres: add code to support various migration phases
- datastore/postgres: add a migration phase flag
- datastore/postgres: change the migrations to use incremental backfill
- fix datastore integration tests not running
- example: legal-guardian
- example: time-bound permission
- example: allow if resource was created before subject
- add application attributes example
- add ip allowlists example
- update to go 1.19.2 to fix vulns
- temporarily disable async logging
- Implement structured errors for all user facing errors
- improve logging of http/grpc servers
- revamps logging
- datastore/postgres: make xid8 type package private
- datastore/postgres: write only xid, drop support for ids
- datastore/postgres: write tx and xid, read only xid
- datastore/postgres: change watch API to use new xids
- datastore/common: use revisions instead of uint64 for GC progress
- datastore/postgres: write tx and xid, read only tx
- datastore: remove specific future revision error
- datastore/postgres: write gc logs without context logger

## [1.13.0] - 2022-10-04
- Add caveats flag to disable writing by default on all datastores
- Bump go.uber.org/goleak from 1.1.12 to 1.2.0
- Bump sigs.k8s.io/controller-runtime from 0.12.3 to 0.13.0
- Change check dispatching to support caveat expr evaluation
- Remove manual validation code now that it is being generated
- Update validate for recent proto change
- Update e2e deps
- Cherrypick fd8086675cd739e585238b3391e42dba85630028
- Cherrypick d53a47039d70d56374fb60bcc99a0c0caae0d019
- Cherrypick 583ea3c64818d03551bf67805e0f1ba8231e02ef
- Cherrypick df461872245757dd84b2716a2646f8467e67288d
- Cherrypick a178f18604700b82c44ce86771a33c8900492d24
- Update e2e deps
- Cherrypick 5b5e7fc867777e8dcd3f94f21dcb165c1cfe4e34
- Bump go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp
- Bump mvdan.cc/gofumpt from 0.3.1 to 0.4.0
- Bump github.com/lib/pq from 1.10.6 to 1.10.7
- Bump github.com/aws/aws-sdk-go from 1.44.90 to 1.44.109
- Switch check dispatching to use the new MembershipSet
- Reorganize the dispatch check protos a bit in preparation for using MembershipSet for checks
- docker: switch to chainguard base images
- internal: add docker build tag to transitives
- Start work for dispatch for caveats by adding MembershipSet
- introduce caveat support in WriteRelationships
- pkg/cache: implement metrics for noop cache
- Change workflow to run on releases and upload release assets
- Add Github wasm build workflow
- internal: add test caches
- pkg/cmd: refactor cache defaults into vars
- pkg/cmd: clean up caching
- internal/datastore: set ns cache to be 16MiB
- pkg/cmd: add memory percentages for cache sizes
- update cobrautil to latest commit - introduces new go OTel module versions - enables insecure jaeger flag
- internal/dispatch: gofumpt 4.0
- Add development package WASM tests to CI
- Improve the development package WASM interface
- Change caching to sort order resource and subject IDs
- Fix chunking util to never call for an empty chunk
- Fix performance for large schema writes in V1Alpha1
- Have cluster dispatch key computation skip the per-prcoess hash, which is not used for dispatching
- Update comment on DispatchCheckKey
- Switch process-specific hash to use memhash directly
- Switch to dispatch keys computed progressively
- Add child check to the difference function
- Better names and additional context within the check implementation
- Add additional consistency tests
- Fix race condition in removing the element from the slice
- Lint fixes
- Add additional consistency tests to further exercise the batch check code
- Add additional assertions to some of the consistency tests
- Implement support for batch checking in dispatch
- Change Check in preparation for batch by removing the reduction functions and changing the passing of state
- Add basic benchmarks for different kinds of checks
- bump max retries for crdb in tests
- Change all user-visible type errors into proper wrapped error structs
- forward GCMaxOperationTime and SplitAtUsersetCount to MySQL DS
- demonstrate snapshot reads for memdb
- more cosmetic changes from PR review
- clarify in comment MemDB does not enforce uniqueness
- panic if caveats are attempted to be written
- add custom errors for not found name/id
- address review comments by @josephschorr
- generate with recently introduced vtprotobuf
- rename internal memdb struct to align with proto name
- datastore no longer responsible to check if caveat exists
- serialize name with caveat and test
- add interface docs
- test name uniqueness
- uses uint64 as caveatID instead of string
- remove caveat type
- introduce Caveat ID
- pluralize interface methods
- adjust naming and use structpb
- spike named/anonymous caveats
- Add a test for writing and reading back a serialized namespace in the datastores
- internal/datastore: use proto.Marshal
- proto: revert back to upstream gRPC
- *: replace encoding with VT methods where possible
- internal/datastore: UnmarshalVT namespaces
- Ensure that internal errors are returned before dev errors
- bump kuberesolver to pick up serviceaccount token refresh
- Disable GC in datastore drivers when in read-only mode
- gomod: bump compress to v1.15.10
- internal/dispatch: SizeVT() for cache estimates
- internal/dispatch: VTClone() to avoid reflection
- pkg/proto: adopt vtprotobuf
- internal/dispatch: snappy s2 compression for gRPC
- update to Go 1.19.1 to fix govulncheck CVEs
- update golang.org/x/net to fix CVE
- replace custom pgxpool collector with opensource module
- Fix fallback for MySQL stats to fix test flake
- don't generate nsswitch.conf (base images have it now)
- Add govulncheck to the linters
- Add an error case for redefining a type in schema

## [1.12.0] - 2022-09-08
- Remove perm system limit checks and rely upon defaults
- also add nsswitch.conf to release
- use distroless.dev/static like Dockerfile.release
- Add better error messaging around create rel conflicts
- Update buf dependencies
- Update e2e authzed-go
- Run go mod tidy
- Update authzed-go client
- Change v1alpha1 WriteSchema to only read namespaces it needs
- turn PGX's info events into debug
- removes duplicated v1.Relationship validation
- Add an error returned if WriteRelationships contains two or more operations on the same relationship
- Add datastore tests around deletion
- Cherrypick 4a47afa3086fff16de9a11d5451d69600f8b6aa2
- Bump cloud.google.com/go/spanner from 1.36.0 to 1.37.0
- Updates for lib changes
- Bump github.com/go-co-op/gocron from 1.16.1 to 1.17.0
- Bump github.com/aws/aws-sdk-go from 1.44.67 to 1.44.90
- Bump go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc
- Bump github.com/jackc/pgx/v4 from 4.16.1 to 4.17.1
- Bump go.opentelemetry.io/otel from 1.8.0 to 1.9.0
- Update e2e go.mod for lib changes
- Cherrypick 28e02a79eec6f343e28adf32172833e5127576c3
- Cherrypick fab679632ecc6f1a3e10cbb64091f118c349d359
- Bump github.com/prometheus/client_golang from 1.12.2 to 1.13.0
- Bump github.com/rs/zerolog from 1.27.0 to 1.28.0
- Bump github.com/grpc-ecosystem/grpc-gateway/v2 from 2.11.2 to 2.11.3
- Bump alpine from 3.16.1 to 3.16.2
- Add configurable limits for write and delete relationship APIs
- newly added lookup subject metrics weren't being unregistered
- Update pkg/caveats/env.go
- Update pkg/caveats/eval.go
- invoke hack/install-tools.sh and generate
- test timestamp operations default to UTC
- add tests for all types defined
- test PartialValue() returns error if not partial
- test duplicate variables fails
- Add ability to get the expression string from a compiled caveat
- Add a wrapping envelope to caveats to allow for different types down the road if necessary
- Add additional eval tests
- Add nested variables test
- Add support for custom types in caveats and implement a prototype IPAddress type
- Add MaxCost to the caveat evaluator
- Add evaluation of caveats and change caveats to require bool expressions
- Add caveat serialization and deserialization
- Add basic compilation of CEL-based caveats
- rename common.go to a more descriptive pgx.go
- move pgx specific code from datastore/common to datastore/postgres/common
- adds stats collection to CRDB datastore
- Update to Go 1.19 and fix lint warnings
- Fully implement the LookupSubjects API
- replace variadic signature with slices
- core.RelationTuple should never be empty
- removes v1.RelationshipUpdate from datastore.ReadWriteTransaction
- Fix another error check
- Remove unnecessary err check
- Fix various error references
- fixes link to go-memdb
- Revert "Add a nightly build for spicedb"
- add a nightly build for spicedb
- Change the chunk sizing to use a shared, single constant and add tests for verification
- Implement DispatchLookupSubjects for finding all reachable subjects of a particular type for a resource and permission
- Convert QueryRelationships to use an internal type which supports multiple resource IDs
- fix critical CVE of runc via dockertest
- adds trivy security scanner to lint GitHub Action
- :debug -> :latest-debug
- Use distroless.dev/static base, add debug variants
- rollback default retries to 10
- review comments
- test stale connections
- retry on 2 additional errors - unexpected EOF when connection is closed by CRDB - CRDB draining and not accepting new connections
- add logging when a retry happens
- add missing retry on HeadRevision
- sets default datastore retries down to 3
- propagates the health-check interval configuration to pgx
- Update spanner.go
- update spanner emulator logs
- bump crdb in tests to 22.1.5
- README: add OpenSSF best practices badge
- address gofumpt errors with go 1.19
- fix: panic on OptimizedRevisionFunction.OptimizedRevision
- test: reproduce panic on OptimizedRevisionFunction.OptimizedRevision
- hack: mv install-tools into hack dir

## [1.11.0] - 2022-08-02
- Skip checking of permissions for relationships in WriteSchema
- switch crdb watch feature detection to only require CONTROLCHANGEFEED
- Fix bounds on check test
- Update e2e go mod for changes
- Cherrypick a342ad6ff901b18a67bffb311ca495ac2efb46b7
- Cherrypick d26a6750f71cdd6423c6f571fa34e9b5eb6ff3b6
- Cherrypick 83727f9ae7f44b78361568b4b8376f3e2d07e2bb
- Cherrypick 05fbb1b84b4d0f9a1e4e4346a35e30166feaf8f2
- Cherrypick c4d8e5a2f15cb4a3e56e483156af936fec909c22
- Cherrypick 198c314c0cefa64cec3008a2bab62d4beccb587c
- Bump go.buf.build/protocolbuffers/go/prometheus/prometheus
- Update generated protos for new grpc
- Tidy for depbot updates
- Cherrypick de29e11b767dc50008d0860a9a389bbe1f3a4a4e
- Bump cloud.google.com/go/spanner from 1.34.0 to 1.36.0
- Bump go.opentelemetry.io/otel/trace from 1.7.0 to 1.8.0
- Bump sigs.k8s.io/controller-runtime from 0.12.2 to 0.12.3
- Bump github.com/go-co-op/gocron from 1.15.0 to 1.16.1
- Bump alpine from 3.16.0 to 3.16.1
- add feature detection to datastores and disable the watch api if the underlying datastore can't support it.
- remove rangefeed config from crdb migrations
- Add ability to enable the REST gateway for the test server
- e2e: fix linter errors
- fix linter warnings and errors
- use the server configured limit for reachable resources
- add a test for async dispatch concurrency limit
- use the server configured concurrency limit for parallel checks
- add logger to context for graph check tests
- limit created goroutines for graph resolution steps
- Add logger to context in testserver
- Add issue links to readme
- Change reachable resources to support multiple subjects IDs
- Add widegroups benchmark test for lookup
- Add benchmark test suite
- Add ability to trace a check request
- add a flag for disable stats, and use it for e2e
- Have WASM development package return updated validation YAML

## [1.10.0] - 2022-07-25
- Further fixes to memdb to not panic post-close
- Add a dispatch test that verifies delete preconditions across datastores
- internal/datastore: add tests for pg/mysql GC
- internal/datastore: rename shared gc logic
- internal/datastore: id column for nsconfig tables
- internal/datastore: common gc logic
- internal/datastore: gc mysql namespaces
- internal/datastore: gc postgres namespaces
- Add a fallback into the MySQL driver for rel count
- Increase the testing timeout to 30m
- examples: mv https://github.com/authzed/examples
- Prevent release version errors from blocking SpiceDB startup
- pkg/cmd: document use of application default credentials with spanner
- Add retries to MySQL stats test, which can occasionally get back empty stats
- Add a WebAssembly interface for invoking the SpiceDB dev package
- Enable Watch API in REST gateway
- Make read tx REPEATABLE_READ
- flake fix: sometimes 500ms is not enough time for the services to start and send requests, so the cert is invalid by the time the first request is sent
- Bump github.com/aws/aws-sdk-go from 1.44.47 to 1.44.48
- Bump cloud.google.com/go/spanner from 1.33.0 to 1.34.0
- Tidy for updates
- Bump google.golang.org/api from 0.82.0 to 0.86.0
- Bump github.com/stretchr/testify from 1.7.5 to 1.8.0
- Bump sigs.k8s.io/controller-runtime from 0.12.1 to 0.12.2
- Bump github.com/aws/aws-sdk-go from 1.44.26 to 1.44.47
- Tidy for depbot
- Bump github.com/prometheus/common from 0.34.0 to 0.35.0
- Bump github.com/go-co-op/gocron from 1.13.0 to 1.15.0
- Cherrypick 09847a3b1836e64946eb6dfdaeb23bad951f3455
- Bump github.com/spf13/cobra from 1.4.0 to 1.5.0
- Bump golang.org/x/tools from 0.1.10 to 0.1.11
- Add log of the connection state for dispatcher IsReady check
- examples: move k8s deployment into examples
- *: add NOTICE
- expose /healthz endpoint when using the http gateway
- add cert rotation test
- watch TLS certs for changes
- Return an error instead of calling panic() when debug.ReadBuildInfo() is unavailable

## [1.9.0] - 2022-06-20
- Cleanup the core messages now that v0 is gone
- migrations: split into atomic and non-atomic portions
- make the manager ensure the version has been really written
- move transacting responsibility to the underlying driver
- test: do not run nested transaction in migration
- update spanner datastore with transactional migration changes
- updates all datastores to use new Driver interface with transaction type
- make driver responsible for writing the version
- fix typo
- introduces Transact function in migrate.Driver
- Add `--tags` to ensure the proper tag is used for local builds
- Ensure the released version appears with a staring `v` prefix
- Start moving from the externally-defined developer API to an internally defined set of types
- Update generated proto files for recent version changes
- Switch to using a tools.go for all tools necessary
- fix unchecked error on os.Setenv
- introduces generics-based migration function
- set default migration timeout to 1 hour
- use WithTimeout instead of WithDeadline
- adds new "migration-timeout" flag to "spicedb migrate"
- propagate context fo migration functions
- fixes documentation typo
- introduces context argument to Driver.Close interface
- Skip checking and redispatch in reachability when seeing duplicates
- Abstract health status management into a helper package and add datastore status
- Remove remaining references to v0 API (except developer API)
- postgres: rename migration variable to reduce confusion
- Shorten the prefixes on cache keys to save some memory
- Update e2e go mod and sum
- Bump google.golang.org/api from 0.78.0 to 0.82.0
- Handle case where memdb is closed before a transaction completes
- Update e2e for dep updates
- Cherrypick db94b297831f0b286998c6a1d61d35d61041c86c
- Cherrypick a8f7fe04a97fff45bf3b2aa81c7cfa1a73066339
- Cherrypick f47d4f35c6e5cdf0643001e559536812e4164dd7
- Cherrypick 527dd8595a13170578344c8f64e614fb47b5399e
- Cherrypick 5ed7d9b07c9a6c4f14c4db1904a3ce869b4193e6
- Update e2e for changes
- Bump github.com/lib/pq from 1.10.5 to 1.10.6
- Bump github.com/prometheus/client_golang from 1.12.1 to 1.12.2
- Bump github.com/Masterminds/squirrel from 1.5.2 to 1.5.3
- Cherrypick 374df04fd665f43fd551b5bf41d72cf55a457075
- Bump github.com/grpc-ecosystem/grpc-gateway/v2 from 2.10.0 to 2.10.3
- Bump alpine from 3.15 to 3.16.0
- Fix health check test
- Update internal/dispatch/remote/cluster.go
- Add dispatch ready, health check integration
- Change integration test to not use v0
- Change _this warnings into errors
- Remove use of _this in predefined testdata
- Remove V0 API
- Change all non-schema consistency tests to use schema
- Rename middleware consistency test for easier test filtering

## [1.8.0] - 2022-06-01
- Use new and improved warning and note markdown
- Add list of available quickstarts
- Fix markdown lints
- Add readme for quickstart examples
- Clarify SpiceDB will not have a schema
- Include brief descriptions for example scripts
- Put quickstart docker compose samples under examples/
- Move the cache implementation behind an interface, to allow for different implementations based on platform
- refactor(schemadsl): remove unused field
- Change to storing the tupleset in entrypoint, to remove the need for the operations path
- Add basic internal cache for reachability graph construction
- Add more details to the missing child error message
- Skip dispatching in reachable resources if there are no further entrypoints available
- Propagate context in Version
- Propagate context in migrations
- support xDS as a dispatch resolver option
- caching dispatch: unregister prometheus metrics on close
- Prevent duplicate checks in ParallelChecker
- Updates after rebase and review
- Updates for rebase
- Adjust handling of streams to work around the fact that gRPC streams are not concurrent safe
- Add dispatch count tracking and caching to reachable resources
- Change lookup to use the reachable resources API
- Implement a dispatchable reachable resources API
- Implement a reachability graph
- reduce flakiness of newenemy e2e test
- cmd: default retries to 10, applies to more datastores
- mysql: convert to datstore v2 API
- spanner: convert to datastore v2 API
- datastore: forward logger through separated context
- crdb: convert to datastore v2 API
- postgres: convert postgres to datastore v2 API
- datastore: update common SQL code to support v2
- datastore: remove NamespaceCacheKey method
- Update call sites for datastore v2 and remove namespace manager.
- add preconditions checking helper
- datastore: add a namespace caching proxy
- datastore: v2 datastore proxy implementations
- memdb: add a datastore v2 implementation
- datastore: update tests for v2 interface
- datastore: add a v2 interface
- switch telemetry log to debug
- log successful telemetry attempts
- lint e2e directory
- update newenemy test for datastore v2
- Upgrade CI crdb version to 21.2.10
- pg: set timezone to utc for revision selection
- Add middleware to return the server version when requested, unless disabled
- Merge branch 'main' into mysql-revision-quantization
- Linter fixes
- Update revision translation funcs
- Fully manual dependency update
- Tidy
- Cherrypick b4b96c09c19807babb3cbbe6e724a69d7371a3f6
- Cherrypick d7e57dc126c4aa28f6617615ef2cb2b9194e171b
- Cherrypick cbd6f9e31e8b9b835ad30c24d84d038d4f59ee02
- build(deps): bump github.com/aws/aws-sdk-go from 1.44.4 to 1.44.5
- Lower test timeouts threshold
- Fix potential overflow
- Fix up revision consts and comments
- Fix rebase error with datastore engine opts
- Add a custom analyzers package for custom lint checks
- Add spanner emulator host as datastore option
- Add spanner emulator env var detection
- crdb: detect broken pipe as resettable error
- disable renovatebot
- go mod tidy
- Cherrypick e08c9281d9b130f14fcdd8646b166ac031c52321
- build(deps): bump github.com/prometheus/common from 0.33.0 to 0.34.0
- build(deps): bump go.buf.build/protocolbuffers/go/prometheus/prometheus
- build(deps): bump google.golang.org/grpc from 1.45.0 to 1.46.0
- build(deps): bump github.com/aws/aws-sdk-go from 1.43.31 to 1.44.4
- Merge branch 'main' into mysql-revision-quantization
- Implement revision quantization for MySQL

## [1.7.1] - 2022-05-02
- k8s: add dispatch enabled comment
- also test empty authorization header
- refactor tests to be table driven
- gofumpt the file
- addresses server panic when no token is provided
- Switch to using Engines for the engine parameter
- postgres: handle negative relationhip count estimates
- pkg/cmd: catch nil registry initialization
- Add mysql to 'datastore-engine' flag's help text

## [1.7.0] - 2022-04-27
- remove remaining references to revision fuzzing
- Add a check on startup for the last released version of SpiceDB
- dispatch: fix NPE possibility from nil check response
- Add dispatch tests and permissions test to ensure errors are handled properly
- Allow prometheus metrics to be fully disabled for datastores
- *: bump to go 1.18
- internal/telemetry: report go version, git commit
- README: adjust feature wording and links
- Combine unit and integration jobs
- mysql: use a stable unique ID for stats
- mysql: fix linter errors
- update to a version of rudd that doesn't race
- README: refresh features, make CTAs scannable
- postgres: use cached optimized revisions
- datastore: refactor common revision functions
- Fix revive lint warning in pkg/namespace
- Fix revive lint warning in internal/namespace
- Add integration testing for the migrate command
- Merge branch 'main' into mysql-datastore-seed-in-initialization
- sets mysql manager singleton
- gomod: bump to go 1.17
- gomod: bump xxhash to v2
- fix linter errors
- makes cli application return non-zero error code on errors
- utf8 is an alias to utf8mb3, which is deprecated
- Update head flag to include spanner
- regenerate datastore options
- reoder mysql datastore option
- advertise spanner support
- wireup mysql to cli
- mysql: run ANALYZE TABLE before Statistics in tests
- Update error code for invalid preshared keys to `Unauthenticated`
- Change build-test to use a local docker build with a different tag to prevent pulling the normal image accidentally
- Add support for multiple preshared keys
- rename query for clarity
- Merge branch 'main' into mysql-datastore-seed-in-initialization
- gomod: bump cobrautil
- .github: add e2e tidy lint step
- Have the Docker-image based test suite run solely those tests
- moves seeding to the initialization of the datastore
- Only run MySQL tests in CI
- mysql: refactor tests to share builders
- e2e: refresh go.mod
- e2e: prevent identifiers from being keywords
- postgres: add a test for optimized revision checking
- postgres: re-use the datastore builder container
- postgres: always use utc for calculating revision
- datastore: relax revision constraint in test
- postgres: refactor revisions to separate file
- postgres: change to revision quantization
- postgres: address linter errors
- memdb: refactor revisions to separate file
- memdb: change to revision quantization
- datastore: change revision fuzzing to quantization
- Change to all relations having canonical cache keys
- Tidy up go.mod
- Update canonicalization for nil support
- Feedback adjustments
- Add extended description of canonicalization
- Switch annotation to require a validated type system and address other feedback
- Regenerate with the correct version
- Lint fixes
- Have the validationfile loader also validate the type system and annotate all namespaces
- Disable canonical cache key usage and aliasing when checking on a shared resource and subject type
- Add namespace manager to caching dispatcher and use to get the canonicalized cache key
- Add alias and cache key fields to relation and add code to fill them in
- Add functions for computing the aliases and canonical cache keys of permissions
- Switch MySQL tests to explicitly specify amd64
- CODEOWNERS: init
- introduces mysql datastore (#525)
- Have the GC index for Postgres be created concurrently
- k8s: add RBAC and flesh out example
- create spanner changelog entries client side
- crdb: coalesce rellationship estimate to handle 0 relationship case
- .github: add back contents permission on release
- .github: grant github token package write
- split telemetry registry, read datastore stats, rework reporter timing
- internal/telemetry: init

## [1.6.0] - 2022-04-11
- Add more detail to the max depth error and handle as a dev error
- Rename `any` to `union` to fix conflict with new any name in Go 1.18
- Merge branch 'main' into datastore-stats-interface
- update straggler dependencies
- update all dependencies
- add memdb implementation for datastore stats
- add spanner implemenation for datastore stats
- add crdb implementation for datastore stats
- add postgres implementation for datastore stats
- remove postgres on crdb test
- rename postgres migrations to show order
- add proxy implementations for datastore stats
- add a datastore statistics interface and tests
- .github: fix passing of secrets to shared actions
- *: yamllint
- .github: migrate to authzed/actions
- Consolidate crdb tx retry into resets
- Fix nil access issue in developer API when missing an expected subject
- pkg/cmd: use cobrautil version command
- gprc: allow setting the max number of workers per server
- Add index and fix limit on Postgres GC
- Add support for `nil` in schema
- Decorate namespace definitions protos with source information
- Move to a common ErrorWithSource for returning source information on errors
- Merge branch 'main' into fix-pg-timezone
- Merge branch 'main' into force-custom-plan
- Bypass pgx auto preparing statement for revision range query
- Change default transaction row timestamp to UTC
- Update cmd/spicedb/restgateway_integration_test.go
- Fix handling of REST gateway options and add an integration test
- expose usagemetric read middleware
- use cache key that matches chaosd version
- switch to chaosd clock attack
- bump crdb to 21.2.7
- Add clock skew error as resetable
- .github: push to dockerhub, use in readme
- protect prom metric registration with a lock
- dispatch: use option pattern for cluster dispatcher to match others
- datastore: refactor and run performance tests against all impls
- protect lastQuantizedRevision with atomics
- consistency tests: use dispatch api and run in parallel
- test: test bursty traffic with dispatch
- add prefixes to lookup metrics
- README: add ports to docker, add config section
- Rename conversion functions
- PR feedback
- Upgrade buf to 1.1.0
- Add core proto message and replace v0 usage

## [1.5.0] - 2022-03-10
- Have the check warning only apply to relations, not permissions
- Add trace log for auth interceptor used
- Add test for writing empty schemas
- Remove Clone call on metadata filtering on namespaces
- lookup: fall back to a slow path (list all + check) when necessary
- Add warnings for namespaces definitions using v0-only constructs
- Update renovate.json (#466)
- Update renovate.json
- bump gofumpt to 1.3.0 and fix new formatting issues
- Allow renovatebot
- README updates
- Update e2e go mod for depbot changes
- Cherrypick 4b68c108d1699b6902fe0f20825cb48353ee7a5d
- build(deps): bump github.com/envoyproxy/protoc-gen-validate
- Cherrypick 65124030a5435f976baaf0c2f4ee1f01c7fecd2a
- build(deps): bump go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp
- Update e2e go mod for depbot changes
- build(deps): bump github.com/go-co-op/gocron from 1.11.0 to 1.13.0
- build(deps): bump google.golang.org/api from 0.63.0 to 0.70.0
- build(deps): bump github.com/envoyproxy/protoc-gen-validate
- build(deps): bump github.com/jackc/pgx/v4 from 4.14.1 to 4.15.0
- Have errors raised by the type system from schema construction in the devcontext be properly contextualized
- Temporarily disable deprecation lint check for v0
- Fix support for pipes in object IDs
- add a function that returns the head revision for any datastore engine
- build(deps): bump go.opentelemetry.io/otel/trace from 1.3.0 to 1.4.1
- build(deps): bump go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc
- build(deps): bump github.com/aws/aws-sdk-go from 1.42.44 to 1.43.8
- don't allocate max_int length slices
- Fix handling of removing allowed wildcards on relations
- Add command line flags for setting the sizes of caches
- add a non-caching namespace manager
- internal/datastore: singleflight revision updates
- revisions: clarify follower read delay
- spanner: tie garbage collector lifecycle to top-level waitgroup
- spanner: add some jitter to gc interval
- spanner: add tracing
- spanner: stop iterators when finished
- spanner: support credentials json file for authn
- add a cloud spanner datastore implementation
- refactor changes to use datastore revisions
- Refactor common datastore code. Split on a fixed userset count instead of estimated size. Factor out revision quantization and expiration code.
- add a standard Close method on migration drivers
- Ignore already closed tx during reset
- Refactor crdb tx retries and resets
- Add consts, comments, test checks
- Add tx retry test
- simplify error code checking for sqlstate errors
- Reset execution for failed retries
- Add retries with a newly acquired connection
- Fix version command
- Add line and column info to expected relations validation errors
- build(deps): bump golang.org/x/tools from 0.1.8 to 0.1.9
- Small error fixes and improvements in validationfile
- Ensure development package works without context changes
- Change validationfile parsing to be YAML based
- dispatch: clean up metrics on close
- util: grpc dial shouldn't overwrite a missing preshared key
- util: return a disable httpserver if enabled = false
- bump optgen, generate ToOption funcs
- testserver: use middleware to create unique datastores per token
- datastore: compute cache key
- dispatcher: fetch datastore from context
- nsm: add a context namespace manager that always fetches ds from context
- services: retrieve datastore from context
- Update authzed-go to bring in the API validation regex fixes
- Cleanup the parsing in validationfile to return better error messages and be more well defined
- Extract relationships block parsing into its own function and have it return errors with context
- Add more context to schema parse errors
- Move the bulk of the dev API impl into its own package
- service tests: add a new TestServer that loads all services
- server config: allow passing in a preconfigured dispatcher and datastore
- consistency middleware: don't require datastore
- add universal consistency middleware
- testutil: use cmp to compare objects and ignore the difference between nil slices and empty slices
- support buffconn for grpc server config
- e2e: consistent use of contexts
- don't use `t` to log in cleanup functions, since they may continue to run after the test has completed.
- bump dependencies
- spicedb config: pluggable authentication
- increase max offset for crdb cluster in e2e tests
- allow setting middleware after complete
- buf.yaml: check for wire compatibility
- formatting
- seperate shared middleware from grpc/dispatch specific middleware
- refactor cmd/
- add datastore and dispatcher injection middleware
- make consistency middleware play nicely with other middleware
- make dispatch api and consistency middleware public
- Add a builder for server config, support injection
- Add a config object for spicedb servers
- Merge branch 'main' into fix-assertions-parsing
- allow CORS to be enabled on the HTTP gateway
- allow gateway backend to be overridden
- Fix parsing of assertions YAML to handle all errors
- run goimports after last merge conflict
- Merge branch 'main' into test-serving-with-health-check
- test that gRPC health check is available in serve-testing cmd
- introduce gRPC health-check for serve-testing
- internal/middleware: add tests for usagemetrics
- gha: run build/test workflows on e2e suite changes
- use errgroups to parallelize preflight checks
- add response metrics to all calls where missing
- write empty usage metrics if none were set on the context
- attempt to avoid failed crdb range splits in e2e
- services/v0: convert line numbers safely
- .github: add CodeQL lint workflow
- Fix deletion of empty namespaces in CRDB datastore

## [1.4.0] - 2022-01-11
- Fixes for lint issues
- Update tooling and testing to properly handle wildcards
- Update membershipset to handle wildcards
- SECURITY FIX: Ensure wildcard is properly handled in Lookup
- Update UserSet to a SubjectSet that can handle wildcards
- remove unused validation package
- bump authzed-go to 0.4.1
- e2e: refactor into a table test
- crdb: touch overlap key on namespace write
- simplify build for e2e
- e2e: test namespace consistency
- fix head command: flag named inconsistently
- bump dependencies
- bump dependencies
- e2e: ensure schemas have ranges that land on all nodes
- bump e2e deps
- pkg/testutil: ensure types in RequireEqualEmptyNil
- Update to the latest branched version of ristretto
- balancer: protect rand source with a mutex
- zerolog: fix non-camelCase fields
- golangci-lint: enable whitespace
- golangci-lint: enable wastedassign
- golangci-lint: enable unconvert
- golangci-lint: enable tenv
- golangci-lint: enable stylecheck
- golangci-lint: enable promlinter
- golangci-lint: enable prealloc
- *: fixes common typos
- golangci-lint: enable makezero
- golangci-lint: enable importas
- golangci-lint: enable ifshort
- golangci-lint: enable gosec
- golangci-lint: enable goprintffuncname
- golangci-lint: enable errorlint
- *: gofumpt broken files
- go.mod: vendor linter tools
- .github: move to the latest gofumpt
- internal/testfixtures: pass linter
- internal/dispatch: remove caching stutter
- internal/graph: remove lint exception
- pkg/consistent: remove lint exception
- pkg/graph: remove lint exception
- pkg/membership: remove stutter
- pkg/migrate: remove lint exception
- pkg/namespace: fix snake_case variable
- pkg/schemadsl: remove InputSource stutter
- Use RunE for serve-testing command
- Use RunE for serve command
- Use RunE for serve-devtools command
- Use RunE with migrate and head commands

## [1.3.0] - 2021-12-23
- goreleaser: use github for release notes
- run separate test suites in separate jobs
- stabilize lookup tests
- adjust hedging test constants
- close dispatcher when devcontext is disposed
- enable race detector in CI
- remove a write to shared dispatch requests
- protect memdb access with a mutex
- protect lexer state with a mutex
- Pull wildcard empty relation check from authzed-go
- Remove public references
- Move common validation on writes to the validation proxy
- Enable dispatch of wildcards, fix consistency tests for them, and add additional consistency tests
- Improve error messaging around use of wildcard types in included relations
- Update authzed-go reference
- Update go.sum
- Add in-progress wildcard support
- Add additional validation to relationship writes in datastores
- Add assertions support to consistency tests
- Implement support for wildcards in the schema package
- Implement wildcard support in the type system
- Add validation methods for resource ID and subject ID in tuple package, and allow wildcards in subject IDs
- Add middleware to run the handwritten validation methods in the protos
- Update authzed-go dependency to pull in proto changes
- add a mapping test to fail when new query options added
- fix iterator leak in hedging datastore proxy
- upgrade optgen
- add negative tests to datastore relationship checks
- change datastore ReverseQueryTuples to use function style arguments
- fix imports
- replace uses of ... with an ellipsis constant
- change datastore QueryTuples to use function style arguments
- rename datastore SyncRevision to HeadRevision
- rename datastore Revision to OptimizedRevision
- pkg/cmd: extract signal handling with grace period
- test v0 preconditions in parallel
- fix testserver test to not depend on error message
- deduplicate simultaneous requests for the same namespace
- perform CheckPermission pre-flight checks in parallel
- Disable e2e github step
- Add rebase squash to contributing guidelines
- Ignore comments when loading relationship tuples in test mode
- remove namespace cache key expiration
- user revisions for loading namespaces in handlers
- use revisions for loading namespaces in graph resolvers
- update namespace manager for versioned namespaces
- update datastore proxies to use versioned namespace reads
- update crdb datastore to use versioned namespace reads
- update postgres datastore to use versioned namespace reads
- update memdb datastore to use versioned namespace reads
- use revisioned namespaces in datastore interface and acceptance tests
- fix: copy max lifetime when passing options to the datastore
- internal/dispatch: return cachingRedispatch
- pkg/cmd: root programName and share ExampleServe
- remove e2e timeout
- pkg/cmd: import pkg/cmd as cmdutil
- pkg/cmd: add serve package
- pkg/cmd: add migrate package
- .github: add pkg/cmd to cli label
- pkg/cmd: add devsvc command
- pkg/cmd: move root to own package
- pkg/cmd: add version command
- pkg/cmd: export root command
- add a cap to iterations for newenemy test
- support both config object (for cobra bindings) and functional options for datastore
- Log revision skew values
- model datastore cli options
- use canonical terms for memdb datastore
- change memdb datastore to use a single transaction log for namespaces and relationships
- fix v1 watch test to not delete non-existent relationship
- test that namespaces are removed from list when deleted
- extract out change tracking across revisions from postgres
- internal/dispatch: extract combined dispatcher
- pin an old watchmaker
- goreleaser: fix tag in docker release notes

## [1.2.0] - 2021-12-01
- Bump go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp
- Bump github.com/ory/dockertest/v3 from 3.8.0 to 3.8.1
- Bump github.com/benbjohnson/clock from 1.2.0 to 1.3.0
- Bump go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc
- Bump github.com/jackc/pgx/v4 from 4.13.0 to 4.14.1
- Bump github.com/lib/pq from 1.10.3 to 1.10.4
- Bump alpine from 3.14.2 to 3.15.0
- Bump github.com/Masterminds/squirrel from 1.5.1 to 1.5.2
- Bump github.com/grpc-ecosystem/grpc-gateway/v2 from 2.6.0 to 2.7.0
- Bump github.com/jackc/pgtype from 1.8.1 to 1.9.1
- Bump github.com/aws/aws-sdk-go from 1.41.15 to 1.42.16
- Bump go.opentelemetry.io/otel/trace from 1.1.0 to 1.2.0
- Bump golang from 1.17.2-alpine3.13 to 1.17.3-alpine3.13
- Add dispatch and cached dispatch counts to response trailer metadata
- Use the same default follower read delay as crdb
- Add follower read documentation
- Add follower read delay option
- properly calculate virtualnode ids for uint16 replicationFactor
- Add proper dispatch and cached dispatch tracking
- Move buf to 1.0.0-rc8
- Add revision support to v1alpha1 schema API
- Add tracking of excluded relations in Lookup and only cache if no relations were excluded from the subproblem
- enable caching on local subproblems
- share key func between cache and consistent hash
- separate flag for optional dispatch cluster CA config
- hashring: use uint16 for replication factor
- rm -rf servok
- README: fix flags, links, and project description
- .github: add more automatic labeling patterns
- remove dot at the end of the log
- use consistent-hash load balancer with kubernetes endpoint resolver for dispatch
- Add log warning to emphasize persistence/scale issues in memdb
- Add cross version API consistency tests
- Have consistency tests run Expand on all permissions
- Use token for package write permission
- services/v1: fix intersection tree conversion
- feat: add v1 Watch API implementation
- .github: move golangci timeout into config
- have crdb raise an error on duplicate tuple create
- add a String implementation for memdb tuple entries
- prevent duplicate tuple creation in memdb datastore
- .github: bump golangci-lint version
- .github: add 5m timeout to golangci-lint
- .github: pin gofumports version
- Add docker login action for ghcr
- use logger from context
- add middleware to copy metadata from context to logger
- Add a middleware to generate and propagate requestIDs.
- support UDS listening on grpc servers
- cmd/serve: revert dispatch-cluster flags changes
- Remove duplicate image templates
- use buffered channels for lookup results
- support https in download API
- Add github container registry as release target
- gomod: bump to cobrautil v0.0.6
- README: remove --no-tls flags from read and k8s
- cmd: remove no-tls, rename tls, dispatch flags
- cmd: rename internal dispatch to cluster dispatch
- use the grpc_health_probe binary from the official images
- cmd: consistent flags for http/grpc servers
- Bump golang from 1.17.1-alpine3.13 to 1.17.2-alpine3.13
- really allow dependabot
- allow dependabot
- update cla worfklow to allow dependabot
- bump dependencies
- bump dependencies
- proxy: use buffered channels and only let one subrequest write a result
- README: remove license badge
- README: rm mailinglist badge, add docs badge
- docs: remove everything
- update readme for v1.1.0
- update builder image name to make it more unique
- Add DepthRequired to dispatch and use for cache checking
- docs: fix typo in dashboard landing page
- docs: add glossary
- README: add swagger link for HTTP API
- Handle the case where RELEASE SAVEPOINT fails with a retry
- Add caching to Lookup dispatcher
- adds an http download api to devtools
- Add serve-testing option to README
- .github: add goreleaser key
- docker image v prefix
- Improve Docker docs

## [1.1.0] - 2021-10-26
- release: support additional platforms
- simplify release dockerfile
- Add additional docs on ZedTokens and LookupResources
- Cleanup the CachingDispatcher when binary shuts down
- remove nsswitch file from release image
- fix docker release images
- .github: label hidden files as tooling
- cmd: default HTTP server to 8443
- e2e: plumb http server flags
- gateway: appease the linter
- gomod: bump to authzed-go v0.3.0
- gateway: add config docstrings
- internal/gateway: use prom namespace & subsystem
- internal/gateway: test tracing propagation
- internal/auth: remove authn annotator
- gateway: serve OpenAPI Schema at /openapi.json
- internal/gateway: add otel middleware
- cmd: add TLS flags for gateway server
- gateway: extract into package and add metrics
- cmd: expand all string input
- add JSON/HTTP API server via gRPC gateway
- Typo fix
- Follow same name convention as exixting indexes
- Adds index on transations table timestamp
- .github: add CLA workflow
- gomod: bump grpcutil
- Add an integration test for the test server
- Update the dockertest version
- set a very short ttl in the crdb e2e tests
- fix the postgres prom GC metrics to respect enable prom option
- track original and hedged datastore request durations separately
- use mocked time for testing request heding
- add prometheus metrics to the heding datastore
- add request hedging as an option to the serve command
- add a datastore proxy which does request hedging
- multiarch docker image releases
- Make sure to use the checked possibly-nil pointer in memdb
- Switch to use the temporary branch of Ristretto until https://github.com/dgraph-io/ristretto/pull/286 is merged
- Make sure to cleanup goroutine generated by the namespace manager and the parser
- .github: disable flaky caching in golangci action
- Add documentation about ZedTokens/Zookies and consistency
- Fix ordering of zed arguments in the dashboard
- Use Docker entrypoint instead of CMD. Enables using spicedb from docker directly. docker run quay.io/authzed/spicedb serve --grpc-preshared-key "somerandomkeyhere" --grpc-no-tls
- Fix: small error
- Add Must* methods for any methods that can panic in tuple pkg
- Update handling of datastore Close to disconnect connections and change to use an errgroup to clean up Postgres GC worker
- bump testreadbadzookie timeout
- lint: lint all markdown files
- .github: split linting and building actions
- Add a selecting a datastore document
- .github: add kubeval linting
- k8s: init basic deployment
- Add gauges for transaction and relationship count removed by GC
- Add prometheus metric for postgres GC duration
- Add background garbage collection to Postgres data store
- Add Dispose method on datastore in prep for GC worker for postgres
- ensure e2e doesn't time out when it would have succeeded
- increase gc window for revision expiration
- fix TestReadBadZookieFlake
- docs: fixes minor spelling mistakes
- build(deps): bump google.golang.org/grpc from 1.40.0 to 1.41.0
- build(deps): bump github.com/aws/aws-sdk-go from 1.40.47 to 1.40.53
- install completions when installing via brew
- allow head install from brew
- dashboard: correct zed usage

## [1.0.0] - 2021-09-30
### Changed
- update readme for homebrew
- add homebrew release
- README: mention devtooling API
- dispatch: only fail on unexpected errors
- gomod: tidy
- .github: auto label tests
- lint: add golangci-lint
- staticcheck: rm deprecated calls
- ineffassign: remove all ineffective assignments
- structcheck: remove all unused fields
- unused: remove unused funcs
- goimports: fix all local/thirdparty splits
- errcheck: handle all errors explicitly
- govet: fix all mutex copies
- deadcode: remove all unused code
- make deleterelationship tests more permissive
- always observe the crdb retries histogram
- add test for v1 DeleteRelationships
- services/v1: test error messages
- fix linter errors
- add a prometheus bucket for zero retries
- allow cached quantized revisions to be used
- update migration name
- unwrap cockroach retry logic on read methods
- fix package path in goreleaser
- pkg/tuple: add pretty print for sub/obj refs
- pkg/tuple: add MustParse and use it in tests
- services/v1: add write tests
- services/v1: verify updates' types & subject
- pkg/tuple: avoid overflow on panic
- pkg/tuple: add relationship parsing
- add version command
- e2e: tweak constants to reduce flakes
- generate options for crdb / spicedb test abstractions
- fix v1 ReadRelationship to save modified query
- fix memdb modifying source builder state
- statistically determine new enemy invulnerability
- use the iterations it took to reproduce the newenemy problem to inform the number of times we test for invulnerability
- test that crdb is vulnerable to newenemy if protections are disabled
- start test process locally
- determine transaction overlap keys from namespace prefixes
- handle crdb retries
- prevent new enemy by forcing transaction overlap
- remove smart sleeping
- prevent newenemy with smart sleeping
- add a (failing) test for new enemy behavior
- fix all linter errors in internal/services
- remove ellipsis from remaining test cases
- buf: remove non-existent authzed-api path
- Change all legacy tuple string formats to ellide ellipsis
- add a test for v1 ReadRelationships
- fix relationship filter precondition checking
- add a test for v1 CheckPermission
- set fetch depth for goreleaser
- add a note about head migrations
- add a test for consistency properties to the hash ring
- .github: enforce linting with whitelisted TODOs
- datastore/test: pass go lint
- datastore/proxy: pass go lint
- datastores/psql: pass go lint
- datastore/memdb: pass go lint
- datastore: add docstrings to pass go lint
- datastore/crdb: pass go lint
- auth: simplify preshared key func

## [0.0.3] - 2021-09-24
### Changed
- cmd: delete crdb migration script
- Implement consistency testing for written V1 endpoints
- README: move install into getting started

## [0.0.2] - 2021-09-23
### Changed
- Implement V0 DeleteConfigs API
- datastore: create type for QueryTuple filtering
- datastore: rename WithUserset to WithSubjectFilter
- datastore: consistently name var relationFilter
- pkg/tuple: print error with all invalid panics
- datastore: handle preconditions with pgx.ErrNoRows
- *: migrate to new v1.RelationshipFilter
- pkg/tuple: validate in conversions
- Add a datastore proxy that validates all calls
- services/v1: implement WriteRelationships
- internal/datastore: adopt v1.Precondition
- fix datastore delete implementations
- re-enable ci tests
- make zedtokens binary compatible with all versions of zookie
- build(deps): bump github.com/fatih/color from 1.12.0 to 1.13.0
- build(deps): bump github.com/aws/aws-sdk-go from 1.40.35 to 1.40.47
- Update otel to v1.0.0
- build(deps): bump github.com/rs/zerolog from 1.24.0 to 1.25.0
- build(deps): bump github.com/lib/pq from 1.10.2 to 1.10.3
- build(deps): bump golang from 1.17.0-alpine3.13 to 1.17.1-alpine3.13
- *.yaml: lint all YAML files
- .github: add yamllint
- bump ci to go 1.17
- use goreleaser to build binaries and packages
- fix readonly test server
- small cleanups
- rework service initialization to more cleanly handle required interceptors
- Raise an error if type info is missing on a Lookup
- Add ExpandPermissionTree to the V1 API
- Add the basic local start command to the README.md
- add the test server as a spicedb subcommand
- internal/datastore: exercise DeleteRelationships
- internal/datastore: add delete preconditions test
- internal/services/v1: init DeleteRelationships
- Implement V1 LookupResources API
- change zed-testserver to use reflection and real server
- fix typos in main method
- Rename developer-service command
- Move root run to a `serve` subcommand
- Have dashboard take the migration status of the datastore into account
- Add basic dashboard for guidance to new users
- Add a better first run experience that shows the command to run when no other arguments are specified
- helper function for revisions and zedtokens from context
- Change V1 schema write to delete any unreferenced object types
- Implement the V1 schema service
- bump bufbuild in gha
- use relationreference instead of onr for lookup dispatch
- move generated protos back to authzed-go
- add v1 CheckPermission implementation
- move to internal proto imports, remove smartclient
- Add ListNamespaces and remove IsEmpty
- fix the error rewrite for ErrAlwaysFail
- v1: add the read method
- Switch memdb to always store config bytes
- Consistency middleware for V1 API
- use authless reflection implementation from grpcutil
- add a zedtoken internal implementation
- change datastore to handle new object filters from v1
- Prepare the consistency test suite for the V1 API
- switch to validation middleware
- .github: fix buf push action
- fix buf build
- add v1 proto definitions
- proto: consolidate protos and generate internally
- proto: rehome authzed API definitions
- .github: add API labels
- fix cluster dispatch error handling
- fix linter build lines for go 1.17
- add a mapping datastore proxy implementation which encodes namespace names
- buf: consolidate into one buf.gen.yaml
- Fix test for recent permissions check PR
- add a default nsswitch.conf file
- build(deps): bump alpine from 3.14.1 to 3.14.2
- Enable better reporting of schema errors
- Add a test for updating a schema and its checks on relationships
- build(deps): bump github.com/aws/aws-sdk-go from 1.40.27 to 1.40.35
- add a gh workflow step to do a build of the container image
- build(deps): bump github.com/aws/aws-sdk-go from 1.40.27 to 1.40.34
- build(deps): bump github.com/rs/zerolog from 1.23.0 to 1.24.0
- build(deps): bump google.golang.org/grpc from 1.39.0 to 1.40.0
- Use the proper sync revision for type checking on schema/namespace changes
- handle more error and shutdown conditions on startup
- rework the way the consistent backend client startup works
- make consistent backend client more idiomatic
- remove the unnecessary short circuits
- document the lookaside cache handling
- rename smartclient to consistent backend client
- log whether an internal expand was recursive
- change the internal grpc port
- rename the prom metrics variables in caching dispatch
- better lookup request logging
- show the contents of the git diff for protos
- remove the tracer code that's no longer used
- split and refactor graph and dispatch
- spicedb: use the internal API everywhere
- spicedb: add an internal API smartclient
- spicedb: add an internal redispatch API
- Disallow relationship writes on permissions
- Add validation of relationships in the developer API context
- README: fix build instructions and add links
- Bootstrap file support
- .github: fix go mod tidy check
- dependencies: go mod tidy
- .github: add step for diffing go generate output
- go.mod: use upstream grpcutil
- README: link Quay badge to tags tab
- build(deps): bump github.com/aws/aws-sdk-go from 1.40.16 to 1.40.27

## [0.0.1] - 2021-08-23
### Changed
- First release.

[#2353]: https://github.com/authzed/spicedb/issues/2353

[Unreleased]: https://github.com/authzed/spicedb/compare/v1.49.1...HEAD
[1.49.1]: https://github.com/authzed/spicedb/compare/v1.49.0...v1.49.1
[1.49.0]: https://github.com/authzed/spicedb/compare/v1.48.0...v1.49.0
[1.48.0]: https://github.com/authzed/spicedb/compare/v1.47.1...v1.48.0
[1.47.1]: https://github.com/authzed/spicedb/compare/v1.46.2...v1.47.1
[1.46.2]: https://github.com/authzed/spicedb/compare/v1.46.0...v1.46.2
[1.46.0]: https://github.com/authzed/spicedb/compare/v1.45.4...v1.46.0
[1.45.4]: https://github.com/authzed/spicedb/compare/v1.45.3...v1.45.4
[1.45.3]: https://github.com/authzed/spicedb/compare/v1.45.2...v1.45.3
[1.45.2]: https://github.com/authzed/spicedb/compare/v1.45.1...v1.45.2
[1.45.1]: https://github.com/authzed/spicedb/compare/v1.45.0...v1.45.1
[1.45.0]: https://github.com/authzed/spicedb/compare/v1.44.4...v1.45.0
[1.44.4]: https://github.com/authzed/spicedb/compare/v1.44.3...v1.44.4
[1.44.3]: https://github.com/authzed/spicedb/compare/v1.44.2...v1.44.3
[1.44.2]: https://github.com/authzed/spicedb/compare/v1.44.1...v1.44.2
[1.44.1]: https://github.com/authzed/spicedb/compare/v1.44.0...v1.44.1
[1.44.0]: https://github.com/authzed/spicedb/compare/v1.43.0...v1.44.0
[1.43.0]: https://github.com/authzed/spicedb/compare/v1.42.1...v1.43.0
[1.42.1]: https://github.com/authzed/spicedb/compare/v1.42.0...v1.42.1
[1.42.0]: https://github.com/authzed/spicedb/compare/v1.41.0...v1.42.0
[1.41.0]: https://github.com/authzed/spicedb/compare/v1.40.1...v1.41.0
[1.40.1]: https://github.com/authzed/spicedb/compare/v1.40.0...v1.40.1
[1.40.0]: https://github.com/authzed/spicedb/compare/v1.39.1...v1.40.0
[1.39.1]: https://github.com/authzed/spicedb/compare/v1.39.0...v1.39.1
[1.39.0]: https://github.com/authzed/spicedb/compare/v1.38.1...v1.39.0
[1.38.1]: https://github.com/authzed/spicedb/compare/v1.38.0...v1.38.1
[1.38.0]: https://github.com/authzed/spicedb/compare/v1.37.2...v1.38.0
[1.37.2]: https://github.com/authzed/spicedb/compare/v1.37.1...v1.37.2
[1.37.1]: https://github.com/authzed/spicedb/compare/v1.37.0...v1.37.1
[1.37.0]: https://github.com/authzed/spicedb/compare/v1.36.1...v1.37.0
[1.36.1]: https://github.com/authzed/spicedb/compare/v1.35.3...v1.36.1
[1.35.3]: https://github.com/authzed/spicedb/compare/v1.35.2...v1.35.3
[1.35.2]: https://github.com/authzed/spicedb/compare/v1.35.1...v1.35.2
[1.35.1]: https://github.com/authzed/spicedb/compare/v1.35.0...v1.35.1
[1.35.0]: https://github.com/authzed/spicedb/compare/v1.34.0...v1.35.0
[1.34.0]: https://github.com/authzed/spicedb/compare/v1.33.1...v1.34.0
[1.33.1]: https://github.com/authzed/spicedb/compare/v1.33.0...v1.33.1
[1.33.0]: https://github.com/authzed/spicedb/compare/v1.32.0...v1.33.0
[1.32.0]: https://github.com/authzed/spicedb/compare/v1.31.0...v1.32.0
[1.31.0]: https://github.com/authzed/spicedb/compare/v1.30.1...v1.31.0
[1.30.1]: https://github.com/authzed/spicedb/compare/v1.30.0...v1.30.1
[1.30.0]: https://github.com/authzed/spicedb/compare/v1.29.5...v1.30.0
[1.29.5]: https://github.com/authzed/spicedb/compare/v1.29.4...v1.29.5
[1.29.4]: https://github.com/authzed/spicedb/compare/v1.29.2...v1.29.4
[1.29.2]: https://github.com/authzed/spicedb/compare/v1.29.1-rc1...v1.29.2
[1.29.1-rc1]: https://github.com/authzed/spicedb/compare/v1.29.0...v1.29.1-rc1
[1.29.0]: https://github.com/authzed/spicedb/compare/v1.28.0-rc1...v1.29.0
[1.28.0-rc1]: https://github.com/authzed/spicedb/compare/v1.27.1-rc1...v1.28.0-rc1
[1.27.1-rc1]: https://github.com/authzed/spicedb/compare/v1.27.0...v1.27.1-rc1
[1.27.0]: https://github.com/authzed/spicedb/compare/v1.27.0-rc1...v1.27.0
[1.27.0-rc1]: https://github.com/authzed/spicedb/compare/v1.26.0-rc2...v1.27.0-rc1
[1.26.0-rc2]: https://github.com/authzed/spicedb/compare/v1.26.0-rc1...v1.26.0-rc2
[1.26.0-rc1]: https://github.com/authzed/spicedb/compare/v1.25.0...v1.26.0-rc1
[1.25.0]: https://github.com/authzed/spicedb/compare/v1.25.0-rc4...v1.25.0
[1.25.0-rc4]: https://github.com/authzed/spicedb/compare/v1.25.0-rc3...v1.25.0-rc4
[1.25.0-rc3]: https://github.com/authzed/spicedb/compare/v1.25.0-rc2...v1.25.0-rc3
[1.25.0-rc2]: https://github.com/authzed/spicedb/compare/v1.25.0-rc1...v1.25.0-rc2
[1.25.0-rc1]: https://github.com/authzed/spicedb/compare/v1.24.0...v1.25.0-rc1
[1.24.0]: https://github.com/authzed/spicedb/compare/v1.24.0-rc3...v1.24.0
[1.24.0-rc3]: https://github.com/authzed/spicedb/compare/v1.24.0-rc2...v1.24.0-rc3
[1.24.0-rc2]: https://github.com/authzed/spicedb/compare/v1.24.0-rc1...v1.24.0-rc2
[1.24.0-rc1]: https://github.com/authzed/spicedb/compare/v1.23.1...v1.24.0-rc1
[1.23.1]: https://github.com/authzed/spicedb/compare/v1.23.1-rc1...v1.23.1
[1.23.1-rc1]: https://github.com/authzed/spicedb/compare/v1.23.0...v1.23.1-rc1
[1.23.0]: https://github.com/authzed/spicedb/compare/v1.23.0-rc4...v1.23.0
[1.23.0-rc4]: https://github.com/authzed/spicedb/compare/v1.23.0-rc3...v1.23.0-rc4
[1.23.0-rc3]: https://github.com/authzed/spicedb/compare/v1.23.0-rc2...v1.23.0-rc3
[1.23.0-rc2]: https://github.com/authzed/spicedb/compare/v1.23.0-rc1...v1.23.0-rc2
[1.23.0-rc1]: https://github.com/authzed/spicedb/compare/v1.22.2...v1.23.0-rc1
[1.22.2]: https://github.com/authzed/spicedb/compare/v1.22.1...v1.22.2
[1.22.1]: https://github.com/authzed/spicedb/compare/v1.22.1-rc1...v1.22.1
[1.22.1-rc1]: https://github.com/authzed/spicedb/compare/v1.22.0...v1.22.1-rc1
[1.22.0]: https://github.com/authzed/spicedb/compare/v1.22.0-rc8...v1.22.0
[1.22.0-rc8]: https://github.com/authzed/spicedb/compare/v1.22.0-rc7...v1.22.0-rc8
[1.22.0-rc7]: https://github.com/authzed/spicedb/compare/v1.22.0-rc6...v1.22.0-rc7
[1.22.0-rc6]: https://github.com/authzed/spicedb/compare/v1.22.0-rc5...v1.22.0-rc6
[1.22.0-rc5]: https://github.com/authzed/spicedb/compare/v1.22.0-rc4...v1.22.0-rc5
[1.22.0-rc4]: https://github.com/authzed/spicedb/compare/v1.22.0-rc2...v1.22.0-rc4
[1.22.0-rc2]: https://github.com/authzed/spicedb/compare/v1.22.0-rc1...v1.22.0-rc2
[1.22.0-rc1]: https://github.com/authzed/spicedb/compare/v1.21.0...v1.22.0-rc1
[1.21.0]: https://github.com/authzed/spicedb/compare/v1.21.0-rc1...v1.21.0
[1.21.0-rc1]: https://github.com/authzed/spicedb/compare/v1.20.0...v1.21.0-rc1
[1.20.0]: https://github.com/authzed/spicedb/compare/v1.19.1...v1.20.0
[1.19.1]: https://github.com/authzed/spicedb/compare/v1.19.0...v1.19.1
[1.19.0]: https://github.com/authzed/spicedb/compare/v1.18.1...v1.19.0
[1.18.1]: https://github.com/authzed/spicedb/compare/v1.18.0...v1.18.1
[1.18.0]: https://github.com/authzed/spicedb/compare/v1.17.0...v1.18.0
[1.17.0]: https://github.com/authzed/spicedb/compare/v1.16.2...v1.17.0
[1.16.2]: https://github.com/authzed/spicedb/compare/v1.16.1...v1.16.2
[1.16.1]: https://github.com/authzed/spicedb/compare/v1.16.0...v1.16.1
[1.16.0]: https://github.com/authzed/spicedb/compare/v1.15.0...v1.16.0
[1.15.0]: https://github.com/authzed/spicedb/compare/v1.14.1...v1.15.0
[1.14.1]: https://github.com/authzed/spicedb/compare/v1.14.0...v1.14.1
[1.14.0]: https://github.com/authzed/spicedb/compare/v1.13.0...v1.14.0
[1.13.0]: https://github.com/authzed/spicedb/compare/v1.12.0...v1.13.0
[1.12.0]: https://github.com/authzed/spicedb/compare/v1.11.0...v1.12.0
[1.11.0]: https://github.com/authzed/spicedb/compare/v1.10.0...v1.11.0
[1.10.0]: https://github.com/authzed/spicedb/compare/v1.9.0...v1.10.0
[1.9.0]: https://github.com/authzed/spicedb/compare/v1.8.0...v1.9.0
[1.8.0]: https://github.com/authzed/spicedb/compare/v1.7.1...v1.8.0
[1.7.1]: https://github.com/authzed/spicedb/compare/v1.7.0...v1.7.1
[1.7.0]: https://github.com/authzed/spicedb/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/authzed/spicedb/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/authzed/spicedb/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/authzed/spicedb/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/authzed/spicedb/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/authzed/spicedb/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/authzed/spicedb/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/authzed/spicedb/compare/v0.0.3...v1.0.0
[0.0.3]: https://github.com/authzed/spicedb/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/authzed/spicedb/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/authzed/spicedb/releases/tag/v0.0.1
