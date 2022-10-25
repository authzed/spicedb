package computed

import (
	"context"
	"fmt"

	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/caveats"
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

	env := caveats.NewEnvironment()
	caveatResult, err := runExpression(ctx, env, result.Expression, params.CaveatContext, params.AtRevision)
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

type expressionResult interface {
	Value() bool
	IsPartial() bool
	MissingVarNames() ([]string, error)
}

type syntheticResult struct {
	value bool
}

func (sr syntheticResult) Value() bool {
	return sr.value
}

func (sr syntheticResult) IsPartial() bool {
	return false
}

func (sr syntheticResult) MissingVarNames() ([]string, error) {
	return nil, fmt.Errorf("not a partial value")
}

func runExpression(
	ctx context.Context,
	env *caveats.Environment,
	expr *v1.CaveatExpression,
	context map[string]any,
	revision datastore.Revision,
) (expressionResult, error) {
	if expr.GetCaveat() != nil {
		ds := datastoremw.MustFromContext(ctx)
		reader := ds.SnapshotReader(revision)
		caveat, _, err := reader.ReadCaveatByName(ctx, expr.GetCaveat().CaveatName)
		if err != nil {
			return nil, err
		}

		compiled, err := caveats.DeserializeCaveat(caveat.SerializedExpression)
		if err != nil {
			return nil, err
		}

		// Create a combined context, with the written context taking precedence over that specified.
		untypedFullContext := maps.Clone(context)
		if untypedFullContext == nil {
			untypedFullContext = map[string]any{}
		}

		relationshipContext := expr.GetCaveat().GetContext().AsMap()
		maps.Copy(untypedFullContext, relationshipContext)

		// Perform type checking and conversion on the context map.
		typedParameters, err := caveats.ConvertContextToParameters(
			untypedFullContext,
			caveat.ParameterTypes,
			caveats.SkipUnknownParameters,
		)
		if err != nil {
			return nil, fmt.Errorf("type error for parameters for caveat `%s`: %w", caveat.Name, err)
		}

		result, err := caveats.EvaluateCaveat(compiled, typedParameters)
		if err != nil {
			return nil, err
		}

		return result, nil
	}

	cop := expr.GetOperation()
	boolResult := false
	if cop.Op == v1.CaveatOperation_AND {
		boolResult = true
	}

	for _, child := range cop.Children {
		childResult, err := runExpression(ctx, env, child, context, revision)
		if err != nil {
			return nil, err
		}

		if childResult.IsPartial() {
			return childResult, nil
		}

		switch cop.Op {
		case v1.CaveatOperation_AND:
			boolResult = boolResult && childResult.Value()
			if !boolResult {
				return syntheticResult{false}, nil
			}

		case v1.CaveatOperation_OR:
			boolResult = boolResult || childResult.Value()
			if boolResult {
				return syntheticResult{true}, nil
			}

		case v1.CaveatOperation_NOT:
			return syntheticResult{!childResult.Value()}, nil

		default:
			panic("unknown op")
		}
	}

	return syntheticResult{boolResult}, nil
}
