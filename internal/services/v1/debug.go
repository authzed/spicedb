package v1

import (
	"context"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/protobuf/types/known/structpb"

	cexpr "github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/tuple"
)

// convertDispatchDebugInformation converts dispatch debug information found in the response metadata
// into DebugInformation returnable to the API.
func convertDispatchDebugInformation(
	ctx context.Context,
	caveatContext map[string]any,
	metadata *dispatch.ResponseMeta,
	reader datastore.Reader,
) (*v1.DebugInformation, error) {
	debugInfo := metadata.DebugInfo
	if debugInfo == nil {
		return nil, nil
	}

	caveats, err := reader.ListCaveats(ctx)
	if err != nil {
		return nil, err
	}

	namespaces, err := reader.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	defs := make([]compiler.SchemaDefinition, 0, len(namespaces)+len(caveats))
	for _, caveat := range caveats {
		defs = append(defs, caveat)
	}
	for _, ns := range namespaces {
		defs = append(defs, ns)
	}

	schema, _ := generator.GenerateSchema(defs)
	converted, err := convertCheckTrace(ctx, caveatContext, debugInfo.Check, reader)
	if err != nil {
		return nil, err
	}

	return &v1.DebugInformation{
		Check:      converted[0],
		SchemaUsed: strings.TrimSpace(schema),
	}, nil
}

func convertCheckTrace(ctx context.Context, caveatContext map[string]any, ct *dispatch.CheckDebugTrace, reader datastore.Reader) ([]*v1.CheckDebugTrace, error) {
	traces := make([]*v1.CheckDebugTrace, 0, len(ct.Request.ResourceIds))
	for _, resourceID := range ct.Request.ResourceIds {
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
		checkResult, ok := ct.Results[resourceID]
		if ok {
			if checkResult.Membership == dispatch.ResourceCheckResult_MEMBER {
				permissionship = v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION
			}
			if checkResult.Membership == dispatch.ResourceCheckResult_CAVEATED_MEMBER {
				permissionship = v1.CheckDebugTrace_PERMISSIONSHIP_CONDITIONAL_PERMISSION
			}
		}

		var caveatEvalInfo *v1.CaveatEvalInfo
		if ok && checkResult.Expression != nil {
			computedResult, err := cexpr.RunCaveatExpression(ctx, checkResult.Expression, caveatContext, reader, cexpr.RunCaveatExpressionWithDebugInformation)
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

			contextStruct, err := structpb.NewStruct(computedResult.ContextValues())
			if err != nil {
				return nil, err
			}

			exprString, err := computedResult.ExpressionString()
			if err != nil {
				return nil, err
			}

			caveatName := ""
			if checkResult.Expression.GetCaveat() != nil {
				caveatName = checkResult.Expression.GetCaveat().CaveatName
			}

			caveatEvalInfo = &v1.CaveatEvalInfo{
				Expression:        exprString,
				Result:            caveatResult,
				Context:           contextStruct,
				PartialCaveatInfo: partialCaveatInfo,
				CaveatName:        caveatName,
			}
		}

		if len(ct.SubProblems) > 0 {
			subProblems := make([]*v1.CheckDebugTrace, 0, len(ct.SubProblems))
			for _, subProblem := range ct.SubProblems {
				converted, err := convertCheckTrace(ctx, caveatContext, subProblem, reader)
				if err != nil {
					return nil, err
				}

				subProblems = append(subProblems, converted...)
			}

			traces = append(traces, &v1.CheckDebugTrace{
				Resource: &v1.ObjectReference{
					ObjectType: ct.Request.ResourceRelation.Namespace,
					ObjectId:   resourceID,
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
			})
		}

		traces = append(traces, &v1.CheckDebugTrace{
			Resource: &v1.ObjectReference{
				ObjectType: ct.Request.ResourceRelation.Namespace,
				ObjectId:   resourceID,
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
		})
	}

	return traces, nil
}
