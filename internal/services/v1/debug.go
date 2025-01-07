package v1

import (
	"cmp"
	"context"
	"slices"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	cexpr "github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ConvertCheckDispatchDebugInformation converts dispatch debug information found in the response metadata
// into DebugInformation returnable to the API.
func ConvertCheckDispatchDebugInformation(
	ctx context.Context,
	caveatContext map[string]any,
	debugInfo *dispatch.DebugInformation,
	reader datastore.Reader,
) (*v1.DebugInformation, error) {
	if debugInfo == nil {
		return nil, nil
	}

	schema, err := getFullSchema(ctx, reader)
	if err != nil {
		return nil, err
	}

	return convertCheckDispatchDebugInformationWithSchema(ctx, caveatContext, debugInfo, reader, schema)
}

// getFullSchema returns the full schema from the reader.
func getFullSchema(ctx context.Context, reader datastore.Reader) (string, error) {
	caveats, err := reader.ListAllCaveats(ctx)
	if err != nil {
		return "", err
	}

	namespaces, err := reader.ListAllNamespaces(ctx)
	if err != nil {
		return "", err
	}

	defs := make([]compiler.SchemaDefinition, 0, len(namespaces)+len(caveats))
	for _, caveat := range caveats {
		defs = append(defs, caveat.Definition)
	}
	for _, ns := range namespaces {
		defs = append(defs, ns.Definition)
	}

	schema, _, err := generator.GenerateSchema(defs)
	if err != nil {
		return "", err
	}

	return schema, nil
}

func convertCheckDispatchDebugInformationWithSchema(
	ctx context.Context,
	caveatContext map[string]any,
	debugInfo *dispatch.DebugInformation,
	reader datastore.Reader,
	schema string,
) (*v1.DebugInformation, error) {
	converted, err := convertCheckTrace(ctx, caveatContext, debugInfo.Check, reader)
	if err != nil {
		return nil, err
	}

	return &v1.DebugInformation{
		Check:      converted,
		SchemaUsed: strings.TrimSpace(schema),
	}, nil
}

func convertCheckTrace(ctx context.Context, caveatContext map[string]any, ct *dispatch.CheckDebugTrace, reader datastore.Reader) (*v1.CheckDebugTrace, error) {
	permissionType := v1.CheckDebugTrace_PERMISSION_TYPE_UNSPECIFIED
	if ct.ResourceRelationType == dispatch.CheckDebugTrace_PERMISSION {
		permissionType = v1.CheckDebugTrace_PERMISSION_TYPE_PERMISSION
	} else if ct.ResourceRelationType == dispatch.CheckDebugTrace_RELATION {
		permissionType = v1.CheckDebugTrace_PERMISSION_TYPE_RELATION
	}

	subRelation := ct.Request.Subject.Relation
	if subRelation == tuple.Ellipsis {
		subRelation = ""
	}

	permissionship := v1.CheckDebugTrace_PERMISSIONSHIP_NO_PERMISSION
	var partialResults []*dispatch.ResourceCheckResult
	for _, checkResult := range ct.Results {
		if checkResult.Membership == dispatch.ResourceCheckResult_MEMBER {
			permissionship = v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION
			break
		}

		if checkResult.Membership == dispatch.ResourceCheckResult_CAVEATED_MEMBER && permissionship != v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION {
			permissionship = v1.CheckDebugTrace_PERMISSIONSHIP_CONDITIONAL_PERMISSION
			partialResults = append(partialResults, checkResult)
		}
	}

	var caveatEvalInfo *v1.CaveatEvalInfo

	// NOTE: Bulk check gives the *fully resolved* results, rather than the result pre-caveat
	// evaluation. In that case, we skip re-evaluating here.
	// TODO(jschorr): Add support for evaluating *each* result distinctly.
	if permissionship == v1.CheckDebugTrace_PERMISSIONSHIP_CONDITIONAL_PERMISSION && len(partialResults) == 1 &&
		len(partialResults[0].MissingExprFields) == 0 {
		partialCheckResult := partialResults[0]
		spiceerrors.DebugAssertNotNil(partialCheckResult.Expression, "got nil caveat expression")

		computedResult, err := cexpr.RunSingleCaveatExpression(ctx, partialCheckResult.Expression, caveatContext, reader, cexpr.RunCaveatExpressionWithDebugInformation)
		if err != nil {
			return nil, err
		}

		var partialCaveatInfo *v1.PartialCaveatInfo
		caveatResult := v1.CaveatEvalInfo_RESULT_FALSE
		if computedResult.Value() {
			caveatResult = v1.CaveatEvalInfo_RESULT_TRUE
		} else if computedResult.IsPartial() {
			caveatResult = v1.CaveatEvalInfo_RESULT_MISSING_SOME_CONTEXT
			missingNames, _ := computedResult.MissingVarNames()
			partialCaveatInfo = &v1.PartialCaveatInfo{
				MissingRequiredContext: missingNames,
			}
		}

		exprString, contextStruct, err := cexpr.BuildDebugInformation(computedResult)
		if err != nil {
			return nil, err
		}

		caveatName := ""
		if partialCheckResult.Expression.GetCaveat() != nil {
			caveatName = partialCheckResult.Expression.GetCaveat().CaveatName
		}

		caveatEvalInfo = &v1.CaveatEvalInfo{
			Expression:        exprString,
			Result:            caveatResult,
			Context:           contextStruct,
			PartialCaveatInfo: partialCaveatInfo,
			CaveatName:        caveatName,
		}
	}

	// If there is more than a single result, mark the overall permissionship
	// as unspecified if *all* results needed to be true and at least one is not.
	if len(ct.Request.ResourceIds) > 1 && ct.Request.ResultsSetting == dispatch.DispatchCheckRequest_REQUIRE_ALL_RESULTS {
		for _, resourceID := range ct.Request.ResourceIds {
			if result, ok := ct.Results[resourceID]; !ok || result.Membership != dispatch.ResourceCheckResult_MEMBER {
				permissionship = v1.CheckDebugTrace_PERMISSIONSHIP_UNSPECIFIED
				break
			}
		}
	}

	if len(ct.SubProblems) > 0 {
		subProblems := make([]*v1.CheckDebugTrace, 0, len(ct.SubProblems))
		for _, subProblem := range ct.SubProblems {
			converted, err := convertCheckTrace(ctx, caveatContext, subProblem, reader)
			if err != nil {
				return nil, err
			}

			subProblems = append(subProblems, converted)
		}

		slices.SortFunc(subProblems, func(a, b *v1.CheckDebugTrace) int {
			return cmp.Compare(tuple.V1StringObjectRef(a.Resource), tuple.V1StringObjectRef(a.Resource))
		})

		return &v1.CheckDebugTrace{
			TraceOperationId: ct.TraceId,
			Resource: &v1.ObjectReference{
				ObjectType: ct.Request.ResourceRelation.Namespace,
				ObjectId:   strings.Join(ct.Request.ResourceIds, ","),
			},
			Permission:     ct.Request.ResourceRelation.Relation,
			PermissionType: permissionType,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: ct.Request.Subject.Namespace,
					ObjectId:   ct.Request.Subject.ObjectId,
				},
				OptionalRelation: subRelation,
			},
			CaveatEvaluationInfo: caveatEvalInfo,
			Result:               permissionship,
			Resolution: &v1.CheckDebugTrace_SubProblems_{
				SubProblems: &v1.CheckDebugTrace_SubProblems{
					Traces: subProblems,
				},
			},
			Duration: ct.Duration,
			Source:   ct.SourceId,
		}, nil
	}

	return &v1.CheckDebugTrace{
		TraceOperationId: ct.TraceId,
		Resource: &v1.ObjectReference{
			ObjectType: ct.Request.ResourceRelation.Namespace,
			ObjectId:   strings.Join(ct.Request.ResourceIds, ","),
		},
		Permission:     ct.Request.ResourceRelation.Relation,
		PermissionType: permissionType,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: ct.Request.Subject.Namespace,
				ObjectId:   ct.Request.Subject.ObjectId,
			},
			OptionalRelation: subRelation,
		},
		CaveatEvaluationInfo: caveatEvalInfo,
		Result:               permissionship,
		Resolution: &v1.CheckDebugTrace_WasCachedResult{
			WasCachedResult: ct.IsCachedResult,
		},
		Duration: ct.Duration,
		Source:   ct.SourceId,
	}, nil
}
