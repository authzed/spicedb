package computed

import (
	"context"

	cexpr "github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/dispatch"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// DebugOption defines the various debug level options for Checks.
type DebugOption int

const (
	// NoDebugging indicates that debug information should be retained
	// while performing the Check.
	NoDebugging DebugOption = 0

	// BasicDebuggingEnabled indicates that basic debug information, such
	// as which steps were taken, should be retained while performing the
	// Check and returned to the caller.
	//
	// NOTE: This has a minor performance impact.
	BasicDebuggingEnabled DebugOption = 1

	// TraceDebuggingEnabled indicates that the Check is being issued for
	// tracing the exact calls made for debugging, which means that not only
	// should debug information be recorded and returned, but that optimizations
	// such as batching should be disabled.
	//
	// WARNING: This has a fairly significant performance impact and should only
	// be used in tooling!
	TraceDebuggingEnabled DebugOption = 2
)

// CheckParameters are the parameters for the ComputeCheck call. *All* are required.
type CheckParameters struct {
	ResourceType  tuple.RelationReference
	Subject       tuple.ObjectAndRelation
	CaveatContext map[string]any
	AtRevision    datastore.Revision
	MaximumDepth  uint32
	DebugOption   DebugOption
	CheckHints    []*v1.CheckHint
	SchemaHash    datalayer.SchemaHash
}

// ComputeCheck computes a check result for the given resource and subject, computing any
// caveat expressions found.
func ComputeCheck(
	ctx context.Context,
	d dispatch.Check,
	ts *caveattypes.TypeSet,
	params CheckParameters,
	resourceID string,
	dispatchChunkSize uint16,
) (*v1.ResourceCheckResult, *v1.ResponseMeta, error) {
	resultsMap, meta, di, err := computeCheck(ctx, d, ts, params, []string{resourceID}, dispatchChunkSize)
	if err != nil {
		return nil, meta, err
	}

	spiceerrors.DebugAssertf(func() bool {
		return (len(di) == 0 && meta.DebugInfo == nil) || (len(di) == 1 && meta.DebugInfo != nil)
	}, "mismatch in debug information returned from computeCheck")

	return resultsMap[resourceID], meta, err
}

// ComputeBulkCheck computes a check result for the given resources and subject, computing any
// caveat expressions found.
func ComputeBulkCheck(
	ctx context.Context,
	d dispatch.Check,
	ts *caveattypes.TypeSet,
	params CheckParameters,
	resourceIDs []string,
	dispatchChunkSize uint16,
) (map[string]*v1.ResourceCheckResult, *v1.ResponseMeta, []*v1.DebugInformation, error) {
	return computeCheck(ctx, d, ts, params, resourceIDs, dispatchChunkSize)
}

// ComputeBulkCheckChunk computes a check result for each resource of chunk, with any caveat expressions that it finds.
// It dispatches chunk with its hint as given and does not chunk it again.
func ComputeBulkCheckChunk(
	ctx context.Context,
	d dispatch.Check,
	ts *caveattypes.TypeSet,
	params CheckParameters,
	chunk dispatch.Chunk,
) (map[string]*v1.ResourceCheckResult, *v1.ResponseMeta, []*v1.DebugInformation, error) {
	return computeCheckChunks(ctx, d, ts, params, []dispatch.Chunk{chunk}, len(chunk.ResourceIDs))
}

func computeCheck(ctx context.Context,
	d dispatch.Check,
	ts *caveattypes.TypeSet,
	params CheckParameters,
	resourceIDs []string,
	dispatchChunkSize uint16,
) (map[string]*v1.ResourceCheckResult, *v1.ResponseMeta, []*v1.DebugInformation, error) {
	chunks := dispatch.ChunkerFromContext(ctx).Chunk(ctx, params.ResourceType.ObjectType, resourceIDs, dispatchChunkSize)
	return computeCheckChunks(ctx, d, ts, params, chunks, len(resourceIDs))
}

// computeCheckChunks dispatches the chunks one at a time, in order.
// It stops at the first error. resourceCount is the number of resource IDs in all chunks.
func computeCheckChunks(ctx context.Context,
	d dispatch.Check,
	ts *caveattypes.TypeSet,
	params CheckParameters,
	chunks []dispatch.Chunk,
	resourceCount int,
) (map[string]*v1.ResourceCheckResult, *v1.ResponseMeta, []*v1.DebugInformation, error) {
	debugging := v1.DispatchCheckRequest_NO_DEBUG
	switch params.DebugOption {
	case BasicDebuggingEnabled:
		debugging = v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING
	case TraceDebuggingEnabled:
		debugging = v1.DispatchCheckRequest_ENABLE_TRACE_DEBUGGING
	}

	setting := v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS
	if resourceCount == 1 {
		setting = v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT
	}

	results := make(map[string]*v1.ResourceCheckResult, resourceCount)
	metadata := &v1.ResponseMeta{}

	bf, err := v1.NewTraversalBloomFilter(uint(params.MaximumDepth))
	if err != nil {
		return nil, nil, nil, spiceerrors.MustBugf("failed to create new traversal bloom filter")
	}

	caveatRunner := cexpr.NewCaveatRunner(ts)

	// TODO(jschorr): Should we make this run in parallel via the preloadedTaskRunner?
	debugInfo := make([]*v1.DebugInformation, 0)
	for _, chunk := range chunks {
		cctx := dispatch.WithRoutingHintIfNeeded(ctx, chunk.Hint)
		checkResult, err := d.DispatchCheck(cctx, &v1.DispatchCheckRequest{
			ResourceRelation: params.ResourceType.ToCoreRR(),
			ResourceIds:      chunk.ResourceIDs,
			ResultsSetting:   setting,
			Subject:          params.Subject.ToCoreONR(),
			Metadata: &v1.ResolverMeta{
				AtRevision:     params.AtRevision.String(),
				DepthRemaining: params.MaximumDepth,
				TraversalBloom: bf,
				SchemaHash:     []byte(params.SchemaHash),
			},
			Debug:      debugging,
			CheckHints: params.CheckHints,
		})

		if checkResult.Metadata.DebugInfo != nil {
			debugInfo = append(debugInfo, checkResult.Metadata.DebugInfo)
		}

		if resourceCount == 1 {
			metadata = checkResult.Metadata
		} else {
			metadata = &v1.ResponseMeta{
				DispatchCount:       metadata.DispatchCount + checkResult.Metadata.DispatchCount,
				DepthRequired:       max(metadata.DepthRequired, checkResult.Metadata.DepthRequired),
				CachedDispatchCount: metadata.CachedDispatchCount + checkResult.Metadata.CachedDispatchCount,
				DebugInfo:           nil,
			}
		}

		if err != nil {
			return results, metadata, debugInfo, err
		}

		for _, resourceID := range chunk.ResourceIDs {
			computed, err := computeCaveatedCheckResult(ctx, caveatRunner, params, resourceID, checkResult)
			if err != nil {
				return results, metadata, debugInfo, err
			}
			results[resourceID] = computed
		}
	}
	return results, metadata, debugInfo, nil
}

func computeCaveatedCheckResult(ctx context.Context, runner *cexpr.CaveatRunner, params CheckParameters, resourceID string, checkResult *v1.DispatchCheckResponse) (*v1.ResourceCheckResult, error) {
	result, ok := checkResult.ResultsByResourceId[resourceID]
	if !ok {
		return &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_NOT_MEMBER,
		}, nil
	}

	if result.Membership == v1.ResourceCheckResult_MEMBER {
		return result, nil
	}

	dl := datalayer.MustFromContext(ctx)
	reader := dl.SnapshotReader(params.AtRevision, params.SchemaHash)
	sr, err := reader.ReadSchema(ctx)
	if err != nil {
		return nil, err
	}

	caveatResult, err := runner.RunCaveatExpression(ctx, result.Expression, params.CaveatContext, sr, cexpr.RunCaveatExpressionNoDebugging)
	if err != nil {
		return nil, err
	}

	if caveatResult.IsPartial() {
		missingFields, _ := caveatResult.MissingVarNames()
		return &v1.ResourceCheckResult{
			Membership:        v1.ResourceCheckResult_CAVEATED_MEMBER,
			Expression:        result.Expression,
			MissingExprFields: missingFields,
		}, nil
	}

	if caveatResult.Value() {
		return &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_MEMBER,
		}, nil
	}

	return &v1.ResourceCheckResult{
		Membership: v1.ResourceCheckResult_NOT_MEMBER,
	}, nil
}
