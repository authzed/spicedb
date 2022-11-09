package computed

import (
	"context"

	cexpr "github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// CheckParameters are the parameters for the ComputeCheck call. *All* are required.
type CheckParameters struct {
	ResourceType       *core.RelationReference
	Subject            *core.ObjectAndRelation
	CaveatContext      map[string]any
	AtRevision         datastore.Revision
	MaximumDepth       uint32
	IsDebuggingEnabled bool
}

// ComputeCheck computes a check result for the given resource and subject, computing any
// caveat expressions found.
func ComputeCheck(
	ctx context.Context,
	d dispatch.Check,
	params CheckParameters,
	resourceID string,
) (*v1.ResourceCheckResult, *v1.ResponseMeta, error) {
	resultsMap, meta, err := computeCheck(ctx, d, params, []string{resourceID})
	if err != nil {
		return nil, meta, err
	}
	return resultsMap[resourceID], meta, err
}

// ComputeBulkCheck computes a check result for the given resources and subject, computing any
// caveat expressions found.
func ComputeBulkCheck(
	ctx context.Context,
	d dispatch.Check,
	params CheckParameters,
	resourceIDs []string,
) (map[string]*v1.ResourceCheckResult, *v1.ResponseMeta, error) {
	return computeCheck(ctx, d, params, resourceIDs)
}

func computeCheck(ctx context.Context,
	d dispatch.Check,
	params CheckParameters,
	resourceIDs []string,
) (map[string]*v1.ResourceCheckResult, *v1.ResponseMeta, error) {
	debugging := v1.DispatchCheckRequest_NO_DEBUG
	if params.IsDebuggingEnabled {
		debugging = v1.DispatchCheckRequest_ENABLE_DEBUGGING
	}

	setting := v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS
	if len(resourceIDs) == 1 {
		setting = v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT
	}

	checkResult, err := d.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		ResourceRelation: params.ResourceType,
		ResourceIds:      resourceIDs,
		ResultsSetting:   setting,
		Subject:          params.Subject,
		Metadata: &v1.ResolverMeta{
			AtRevision:     params.AtRevision.String(),
			DepthRemaining: params.MaximumDepth,
		},
		Debug: debugging,
	})
	if err != nil {
		return nil, checkResult.Metadata, err
	}

	results := make(map[string]*v1.ResourceCheckResult, len(resourceIDs))
	for _, resourceID := range resourceIDs {
		computed, err := computeCaveatedCheckResult(ctx, params, resourceID, checkResult)
		if err != nil {
			return nil, checkResult.Metadata, err
		}
		results[resourceID] = computed
	}
	return results, checkResult.Metadata, nil
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
