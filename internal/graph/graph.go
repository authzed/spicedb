package graph

import (
	"context"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
)

// Ellipsis relation is used to signify a semantic-free relationship.
const Ellipsis = "..."

// CheckResult is the data that is returned by a single check or sub-check.
type CheckResult struct {
	Resp *v1.DispatchCheckResponse
	Err  error
}

// ExpandResult is the data that is returned by a single expand or sub-expand.
type ExpandResult struct {
	Resp *v1.DispatchExpandResponse
	Err  error
}

// LookupResult is the data that is returned by a single lookup or sub-lookup.
type LookupResult struct {
	Resp *v1.DispatchLookupResponse
	Err  error
}

// ReduceableCheckFunc is a function that can be bound to a execution context.
type ReduceableCheckFunc func(ctx context.Context, resultChan chan<- CheckResult)

// Reducer is a type for the functions Any and All which combine check results.
type Reducer func(ctx context.Context, requests []ReduceableCheckFunc) CheckResult

// AlwaysFail is a ReduceableCheckFunc which will always fail when reduced.
func AlwaysFail(ctx context.Context, resultChan chan<- CheckResult) {
	resultChan <- checkResultError(NewAlwaysFailErr(), 0, 0)
}

// ReduceableExpandFunc is a function that can be bound to a execution context.
type ReduceableExpandFunc func(ctx context.Context, resultChan chan<- ExpandResult)

// AlwaysFailExpand is a ReduceableExpandFunc which will always fail when reduced.
func AlwaysFailExpand(ctx context.Context, resultChan chan<- ExpandResult) {
	resultChan <- expandResultError(NewAlwaysFailErr(), 0, 0)
}

// ExpandReducer is a type for the functions Any and All which combine check results.
type ExpandReducer func(
	ctx context.Context,
	start *v0.ObjectAndRelation,
	requests []ReduceableExpandFunc,
) ExpandResult

// ReduceableLookupFunc is a function that can be bound to a execution context.
type ReduceableLookupFunc func(ctx context.Context, resultChan chan<- LookupResult)

// LookupReducer is a type for the functions which combine lookup results.
type LookupReducer func(ctx context.Context, limit uint32, requests []ReduceableLookupFunc) LookupResult

func decrementDepth(md *v1.ResolverMeta) *v1.ResolverMeta {
	return &v1.ResolverMeta{
		AtRevision:     md.AtRevision,
		DepthRemaining: md.DepthRemaining - 1,
	}
}

func max(x, y uint32) uint32 {
	if x < y {
		return y
	}
	return x
}
