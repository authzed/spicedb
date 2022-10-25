package graph

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
	ResourceID         string
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
	d dispatch.Dispatcher,
	params CheckParameters,
) (*v1.ResourceCheckResult, *v1.ResponseMeta, error) {
	debugging := v1.DispatchCheckRequest_NO_DEBUG
	if params.IsDebuggingEnabled {
		debugging = v1.DispatchCheckRequest_ENABLE_DEBUGGING
	}

	checkResult, err := d.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		ResourceRelation: params.ResourceType,
		ResourceIds:      []string{params.ResourceID},
		ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
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

	result, ok := checkResult.ResultsByResourceId[params.ResourceID]
	if !ok {
		return &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_NOT_MEMBER,
		}, checkResult.Metadata, nil
	}

	if result.Membership == v1.ResourceCheckResult_MEMBER {
		return result, checkResult.Metadata, nil
	}

	env := caveats.NewEnvironment()
	caveatResult, err := runExpression(ctx, env, result.Expression, params.CaveatContext, params.AtRevision)
	if err != nil {
		return nil, checkResult.Metadata, err
	}

	if caveatResult.IsPartial() {
		missingFields, _ := caveatResult.MissingVarNames()
		return &v1.ResourceCheckResult{
			Membership:        v1.ResourceCheckResult_CAVEATED_MEMBER,
			MissingExprFields: missingFields,
		}, checkResult.Metadata, nil
	} else if caveatResult.Value() {
		return &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_MEMBER,
		}, checkResult.Metadata, nil
	} else {
		return &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_NOT_MEMBER,
		}, checkResult.Metadata, nil
	}
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
		typedParameters, err := caveats.ConvertContextToParameters(untypedFullContext, caveat.ParameterTypes)
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
