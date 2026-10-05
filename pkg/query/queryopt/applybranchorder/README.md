# applybranchorder

Consumes typed BranchOrder recommendations and applies full occurrence permutations to unions/intersections. Honors valid recommendations; unsupported or stale targets get structured declines. No predictive producer is enabled by default. Malformed permutations fail validation.

Unit tests inspect recommendations/dispositions. The shared Rapid semantics harness
checks all three operations, including misleading estimates or reversed recommendations.
Run `go test -race ./pkg/query/queryopt/applybranchorder`.
