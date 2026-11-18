// Package queryprofile selects explicit, versioned query execution policies.
package queryprofile

import (
	"errors"
	"fmt"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/queryopt/parallelcheck"
)

// Name identifies an execution policy.
type Name string

const (
	// Current preserves the normal optimizer selection.
	Current Name = "current"
	// NativeParityV1 enables only classified native-equivalent behavior.
	NativeParityV1 Name = "native-parity-v1"
)

// Options holds resolved deployment limits.
type Options struct {
	// CheckConcurrency is the maximum child concurrency within each Check reducer.
	CheckConcurrency uint16
	// DispatchChunkSize is the maximum number of targets in each dispatch batch.
	DispatchChunkSize uint16
}

// Profile is an immutable execution policy resolved before serving requests.
type Profile struct {
	name    Name
	options Options
}

// Resolve validates a profile and its resolved limits. An empty name selects Current.
func Resolve(name Name, options Options) (Profile, error) {
	if name == "" {
		name = Current
	}

	if name != Current && name != NativeParityV1 {
		return Profile{}, fmt.Errorf("unknown query profile %q", name)
	}

	if options.CheckConcurrency == 0 || options.DispatchChunkSize == 0 {
		return Profile{}, errors.New("query profile limits must be positive")
	}

	return Profile{name: name, options: options}, nil
}

// Name returns the policy name; the zero profile preserves Current behavior.
func (p Profile) Name() Name {
	if p.name == "" {
		return Current
	}

	return p.name
}

// Options returns the resolved deployment limits.
func (p Profile) Options() Options { return p.options }

// BuildOptions preserves native execution shape while retaining canonical identities.
func (p Profile) BuildOptions() []query.BuildOption {
	if p.Name() == NativeParityV1 {
		return []query.BuildOption{query.WithSchemaExecutionOrder(), query.WithDeferredCaveats()}
	}

	return nil
}

// StructuralOptimizers selects only classified transformations for this request.
func (p Profile) StructuralOptimizers(params queryopt.RequestParams) []optimization.Optimizer {
	if p.Name() == NativeParityV1 {
		// Resolve already validated this deployment limit. Check-only recommendations
		// preserve schema shape and do not consult learned signals.
		return []optimization.Optimizer{parallelcheck.MustNew(p.options.CheckConcurrency)}
	}

	return queryopt.OptimizersForRequest(params)
}

// DecisionOptimizers selects recommendation producers and consumers for the policy.
func (p Profile) DecisionOptimizers() []optimization.Optimizer {
	if p.Name() == NativeParityV1 {
		return nil
	}

	return queryopt.DecisionOptimizers()
}

// CollectSignals reports whether execution contributes learned observations.
func (p Profile) CollectSignals() bool { return p.Name() == Current }

// ContextOptions selects the native-aligned direct-read and recursion strategies.
func (p Profile) ContextOptions() []query.ContextOption {
	if p.Name() != NativeParityV1 {
		return nil
	}

	return []query.ContextOption{
		query.WithCheckConcurrency(p.options.CheckConcurrency),
		query.WithCheckExecution(query.CheckExecutionOptions{
			BatchChecks:                  true,
			BatchSize:                    p.options.DispatchChunkSize,
			TargetedRecursion:            true,
			BaseFirstExclusion:           true,
			StrictSubjectMatching:        true,
			ExhaustiveIntersectionArrows: true,
			StopAfterDirectMatch:         true,
			CoalesceDirectWildcards:      true,
			UnfilteredSingleTypeReads:    true,
			BroadSingleUsersetReads:      true,
		}),
	}
}
