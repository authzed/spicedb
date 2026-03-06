package computed

import (
	"context"

	cexpr "github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/dispatch"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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

func computeCheck(ctx context.Context,
	d dispatch.Check,
	ts *caveattypes.TypeSet,
	params CheckParameters,
	resourceIDs []string,
	dispatchChunkSize uint16,
) (map[string]*v1.ResourceCheckResult, *v1.ResponseMeta, []*v1.DebugInformation, error) {
	debugging := v1.DispatchCheckRequest_NO_DEBUG
	switch params.DebugOption {
	case BasicDebuggingEnabled:
		debugging = v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING
	case TraceDebuggingEnabled:
		debugging = v1.DispatchCheckRequest_ENABLE_TRACE_DEBUGGING
	}

	setting := v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS
	if len(resourceIDs) == 1 {
		setting = v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT
	}

	// Ensure that the number of resources IDs given to each dispatch call is not in excess of the maximum.
	results := make(map[string]*v1.ResourceCheckResult, len(resourceIDs))
	metadata := &v1.ResponseMeta{}

	bf, err := v1.NewTraversalBloomFilter(uint(params.MaximumDepth))
	if err != nil {
		return nil, nil, nil, spiceerrors.MustBugf("failed to create new traversal bloom filter")
	}

	caveatRunner := cexpr.NewCaveatRunner(ts)

	// TODO(jschorr): Should we make this run in parallel via the preloadedTaskRunner?
	debugInfo := make([]*v1.DebugInformation, 0)
	_, err = slicez.ForEachChunkUntil(resourceIDs, dispatchChunkSize, func(resourceIDsToCheck []string) (bool, error) {
		checkResult, err := d.DispatchCheck(ctx, &v1.DispatchCheckRequest{
			ResourceRelation: params.ResourceType.ToCoreRR(),
			ResourceIds:      resourceIDsToCheck,
			ResultsSetting:   setting,
			Subject:          params.Subject.ToCoreONR(),
			Metadata: &v1.ResolverMeta{
				AtRevision:     params.AtRevision.String(),
				DepthRemaining: params.MaximumDepth,
				TraversalBloom: bf,
			},
			Debug:      debugging,
			CheckHints: params.CheckHints,
		})

		if checkResult.Metadata.DebugInfo != nil {
			debugInfo = append(debugInfo, checkResult.Metadata.DebugInfo)
		}

		if len(resourceIDs) == 1 {
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
			return false, err
		}

		for _, resourceID := range resourceIDsToCheck {
			computed, err := computeCaveatedCheckResult(ctx, caveatRunner, params, resourceID, checkResult, params.DebugOption)
			if err != nil {
				return false, err
			}
			results[resourceID] = computed
		}

		return true, nil
	})
	return results, metadata, debugInfo, err
}

func computeCaveatedCheckResult(ctx context.Context, runner *cexpr.CaveatRunner, params CheckParameters, resourceID string, checkResult *v1.DispatchCheckResponse, debugOption DebugOption) (*v1.ResourceCheckResult, error) {
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
	reader := dl.SnapshotReader(params.AtRevision)
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

	// The caveat evaluated to false, resulting in NOT_MEMBER.
	// Only collect per-caveat diagnostics when debugging is enabled, to avoid
	// leaking caveat expression internals and context values in production
	// responses, and to avoid the cost of re-evaluation on the hot path.
	if debugOption != NoDebugging {
		caveatEvalInfo, err := collectCaveatEvalInfo(ctx, runner, result.Expression, params.CaveatContext, sr)
		if err != nil {
			// If we fail to collect diagnostics, still return the NOT_MEMBER result
			// without diagnostics rather than failing the entire check.
			return &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_NOT_MEMBER,
			}, nil
		}

		return &v1.ResourceCheckResult{
			Membership:     v1.ResourceCheckResult_NOT_MEMBER,
			CaveatEvalInfo: caveatEvalInfo,
		}, nil
	}

	return &v1.ResourceCheckResult{
		Membership: v1.ResourceCheckResult_NOT_MEMBER,
	}, nil
}

// collectCaveatEvalInfo re-evaluates a caveat expression with debug information
// enabled to collect per-leaf caveat evaluation results. This is called only when
// the initial evaluation returned false (NOT_MEMBER), so the re-evaluation cost
// is acceptable since the request is already denied.
//
// This addresses https://github.com/authzed/spicedb/issues/2802 by providing
// applications with information about which specific caveat(s) caused a denial.
func collectCaveatEvalInfo(
	ctx context.Context,
	runner *cexpr.CaveatRunner,
	expr *core.CaveatExpression,
	caveatContext map[string]any,
	reader cexpr.CaveatDefinitionLookup,
) ([]*v1.CaveatEvalResult, error) {
	// Re-run with debug information to collect leaf results.
	debugResult, err := runner.RunCaveatExpression(ctx, expr, caveatContext, reader, cexpr.RunCaveatExpressionWithDebugInformation)
	if err != nil {
		return nil, err
	}

	leafResults := debugResult.LeafCaveatResults()
	if len(leafResults) == 0 {
		return nil, nil
	}

	evalResults := make([]*v1.CaveatEvalResult, 0, len(leafResults))
	for _, leaf := range leafResults {
		evalResult := &v1.CaveatEvalResult{
			CaveatName: leaf.ParentCaveat().Name(),
		}

		// Determine the result.
		if leaf.IsPartial() {
			evalResult.Result = v1.CaveatEvalResult_RESULT_MISSING_SOME_CONTEXT
			missingNames, _ := leaf.MissingVarNames()
			evalResult.MissingContextParams = missingNames
		} else if leaf.Value() {
			evalResult.Result = v1.CaveatEvalResult_RESULT_TRUE
		} else {
			evalResult.Result = v1.CaveatEvalResult_RESULT_FALSE
		}

		// Collect the expression string.
		exprString, err := leaf.ExpressionString()
		if err == nil {
			evalResult.ExpressionString = exprString
		}

		// Collect the context values used during evaluation.
		contextStruct, err := leaf.ContextStruct()
		if err == nil {
			evalResult.Context = contextStruct
		}

		evalResults = append(evalResults, evalResult)
	}

	return evalResults, nil
}
