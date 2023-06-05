package computed

import (
	"context"

	cexpr "github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/util"
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
	ResourceType  *core.RelationReference
	Subject       *core.ObjectAndRelation
	CaveatContext map[string]any
	AtRevision    datastore.Revision
	MaximumDepth  uint32
	DebugOption   DebugOption
}

// ComputeCheck computes a check result for the given resource and subject, computing any
// caveat expressions found.
func ComputeCheck(
	ctx context.Context,
	d dispatch.Check,
	params CheckParameters,
	resourceID string,
) (*v1.ResourceCheckResult, *v1.ResponseMeta, error) {
	resultsMap, metas, err := computeCheck(ctx, d, params, []string{resourceID})
	if err != nil {
		return nil, metas[resourceID], err
	}
	return resultsMap[resourceID], metas[resourceID], err
}

// ComputeBulkCheck computes a check result for the given resources and subject, computing any
// caveat expressions found.
func ComputeBulkCheck(
	ctx context.Context,
	d dispatch.Check,
	params CheckParameters,
	resourceIDs []string,
) (map[string]*v1.ResourceCheckResult, map[string]*v1.ResponseMeta, error) {
	return computeCheck(ctx, d, params, resourceIDs)
}

func computeCheck(ctx context.Context,
	d dispatch.Check,
	params CheckParameters,
	resourceIDs []string,
) (map[string]*v1.ResourceCheckResult, map[string]*v1.ResponseMeta, error) {
	debugging := v1.DispatchCheckRequest_NO_DEBUG
	if params.DebugOption == BasicDebuggingEnabled {
		debugging = v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING
	} else if params.DebugOption == TraceDebuggingEnabled {
		debugging = v1.DispatchCheckRequest_ENABLE_TRACE_DEBUGGING
	}

	setting := v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS
	if len(resourceIDs) == 1 {
		setting = v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT
	}

	// Ensure that the number of resources IDs given to each dispatch call is not in excess of the maximum.
	results := make(map[string]*v1.ResourceCheckResult, len(resourceIDs))
	resultMetadatas := make(map[string]*v1.ResponseMeta, len(resourceIDs))

	// TODO(jschorr): Should we make this run in parallel via the preloadedTaskRunner?
	_, err := util.ForEachChunkUntil(resourceIDs, datastore.FilterMaximumIDCount, func(resourceIDsToCheck []string) (bool, error) {
		checkResult, err := d.DispatchCheck(ctx, &v1.DispatchCheckRequest{
			ResourceRelation: params.ResourceType,
			ResourceIds:      resourceIDsToCheck,
			ResultsSetting:   setting,
			Subject:          params.Subject,
			Metadata: &v1.ResolverMeta{
				AtRevision:     params.AtRevision.String(),
				DepthRemaining: params.MaximumDepth,
			},
			Debug: debugging,
		})
		for _, resourceID := range resourceIDsToCheck {
			resultMetadatas[resourceID] = checkResult.Metadata
		}

		if err != nil {
			return false, err
		}

		for _, resourceID := range resourceIDsToCheck {
			computed, err := computeCaveatedCheckResult(ctx, params, resourceID, checkResult)
			if err != nil {
				return false, err
			}
			results[resourceID] = computed
		}

		return true, nil
	})
	return results, resultMetadatas, err
}

func computeCaveatedCheckResult(ctx context.Context, params CheckParameters, resourceID string, checkResult *v1.DispatchCheckResponse) (*v1.ResourceCheckResult, error) {
	result, ok := checkResult.ResultsByResourceId[resourceID]
	if !ok {
		return &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_NOT_MEMBER,
		}, nil
	}

	if result.Membership == v1.ResourceCheckResult_MEMBER {
		return result, nil
	}

	ds := datastoremw.MustFromContext(ctx)
	reader := ds.SnapshotReader(params.AtRevision)

	caveatResult, err := cexpr.RunCaveatExpression(ctx, result.Expression, params.CaveatContext, reader, cexpr.RunCaveatExpressionNoDebugging)
	if err != nil {
		return nil, err
	}

	if caveatResult.IsPartial() {
		missingFields, _ := caveatResult.MissingVarNames()
		return &v1.ResourceCheckResult{
			Membership:        v1.ResourceCheckResult_CAVEATED_MEMBER,
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
