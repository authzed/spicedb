package graph

import (
	"context"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
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
	resultChan <- checkResultError(NewAlwaysFailErr(), emptyMetadata)
}

// ReduceableExpandFunc is a function that can be bound to a execution context.
type ReduceableExpandFunc func(ctx context.Context, resultChan chan<- ExpandResult)

// AlwaysFailExpand is a ReduceableExpandFunc which will always fail when reduced.
func AlwaysFailExpand(ctx context.Context, resultChan chan<- ExpandResult) {
	resultChan <- expandResultError(NewAlwaysFailErr(), emptyMetadata)
}

// ExpandReducer is a type for the functions Any and All which combine check results.
type ExpandReducer func(
	ctx context.Context,
	start *core.ObjectAndRelation,
	requests []ReduceableExpandFunc,
) ExpandResult

// ReduceableLookupFunc is a function that can be bound to a execution context.
type ReduceableLookupFunc func(ctx context.Context, resultChan chan<- LookupResult)

// LookupReducer is a type for the functions which combine lookup results.
type LookupReducer func(ctx context.Context, parentReq ValidatedLookupRequest, limit uint32, requests []ReduceableLookupFunc) LookupResult

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

var emptyMetadata = &v1.ResponseMeta{}

func combineResponseMetadata(existing *v1.ResponseMeta, responseMetadata *v1.ResponseMeta) *v1.ResponseMeta {
	lookupExcludedDirect := existing.LookupExcludedDirect
	if responseMetadata.LookupExcludedDirect != nil {
		lookupExcludedDirect = append(lookupExcludedDirect, responseMetadata.LookupExcludedDirect...)
	}

	lookupExcludedTtu := existing.LookupExcludedTtu
	if responseMetadata.LookupExcludedTtu != nil {
		lookupExcludedTtu = append(lookupExcludedTtu, responseMetadata.LookupExcludedTtu...)
	}

	return &v1.ResponseMeta{
		DispatchCount:        existing.DispatchCount + responseMetadata.DispatchCount,
		DepthRequired:        max(existing.DepthRequired, responseMetadata.DepthRequired),
		CachedDispatchCount:  existing.CachedDispatchCount + responseMetadata.CachedDispatchCount,
		LookupExcludedDirect: lookupExcludedDirect,
		LookupExcludedTtu:    lookupExcludedTtu,
	}
}

func ensureMetadata(subProblemMetadata *v1.ResponseMeta) *v1.ResponseMeta {
	if subProblemMetadata == nil {
		subProblemMetadata = emptyMetadata
	}

	return &v1.ResponseMeta{
		DispatchCount:        subProblemMetadata.DispatchCount,
		DepthRequired:        subProblemMetadata.DepthRequired,
		CachedDispatchCount:  subProblemMetadata.CachedDispatchCount,
		LookupExcludedDirect: subProblemMetadata.LookupExcludedDirect,
		LookupExcludedTtu:    subProblemMetadata.LookupExcludedTtu,
	}
}

func addCallToResponseMetadata(metadata *v1.ResponseMeta) *v1.ResponseMeta {
	// + 1 for the current call.
	return &v1.ResponseMeta{
		DispatchCount:        metadata.DispatchCount + 1,
		DepthRequired:        metadata.DepthRequired + 1,
		CachedDispatchCount:  metadata.CachedDispatchCount,
		LookupExcludedDirect: metadata.LookupExcludedDirect,
		LookupExcludedTtu:    metadata.LookupExcludedTtu,
	}
}
