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

func (cr CheckResult) ResultError() error {
	return cr.Err
}

// ExpandResult is the data that is returned by a single expand or sub-expand.
type ExpandResult struct {
	Resp *v1.DispatchExpandResponse
	Err  error
}

func (er ExpandResult) ResultError() error {
	return er.Err
}

// ReduceableExpandFunc is a function that can be bound to a execution context.
type ReduceableExpandFunc func(ctx context.Context, resultChan chan<- ExpandResult)

// AlwaysFailExpand is a ReduceableExpandFunc which will always fail when reduced.
func AlwaysFailExpand(_ context.Context, resultChan chan<- ExpandResult) {
	resultChan <- expandResultError(NewAlwaysFailErr(), emptyMetadata)
}

// ExpandReducer is a type for the functions Any and All which combine check results.
type ExpandReducer func(
	ctx context.Context,
	start *core.ObjectAndRelation,
	requests []ReduceableExpandFunc,
) ExpandResult

func decrementDepth(md *v1.ResolverMeta) *v1.ResolverMeta {
	return &v1.ResolverMeta{
		AtRevision:     md.GetAtRevision(),
		DepthRemaining: md.GetDepthRemaining() - 1,
		TraversalBloom: md.GetTraversalBloom(),
	}
}

var emptyMetadata = &v1.ResponseMeta{}

func ensureMetadata(subProblemMetadata *v1.ResponseMeta) *v1.ResponseMeta {
	if subProblemMetadata == nil {
		subProblemMetadata = emptyMetadata
	}

	return &v1.ResponseMeta{
		DispatchCount:       subProblemMetadata.GetDispatchCount(),
		DepthRequired:       subProblemMetadata.GetDepthRequired(),
		CachedDispatchCount: subProblemMetadata.GetCachedDispatchCount(),
		DebugInfo:           subProblemMetadata.GetDebugInfo(),
	}
}

func addCallToResponseMetadata(metadata *v1.ResponseMeta) *v1.ResponseMeta {
	// + 1 for the current call.
	return &v1.ResponseMeta{
		DispatchCount:       metadata.GetDispatchCount() + 1,
		DepthRequired:       metadata.GetDepthRequired() + 1,
		CachedDispatchCount: metadata.GetCachedDispatchCount(),
		DebugInfo:           metadata.GetDebugInfo(),
	}
}

func addAdditionalDepthRequired(metadata *v1.ResponseMeta) *v1.ResponseMeta {
	return &v1.ResponseMeta{
		DispatchCount:       metadata.GetDispatchCount(),
		DepthRequired:       metadata.GetDepthRequired() + 1,
		CachedDispatchCount: metadata.GetCachedDispatchCount(),
		DebugInfo:           metadata.GetDebugInfo(),
	}
}
