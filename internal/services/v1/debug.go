package v1

import (
	"context"
	"fmt"
	"sort"
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

// convertCheckDispatchDebugInformation converts dispatch debug information found in the response metadata
// into DebugInformation returnable to the API.
func convertCheckDispatchDebugInformation(
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

	schema, _, err := generator.GenerateSchema(defs)
	if err != nil {
		return nil, err
	}

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
	if permissionship == v1.CheckDebugTrace_PERMISSIONSHIP_CONDITIONAL_PERMISSION && len(partialResults) == 1 {
		partialCheckResult := partialResults[0]
		computedResult, err := cexpr.RunCaveatExpression(ctx, partialCheckResult.Expression, caveatContext, reader, cexpr.RunCaveatExpressionWithDebugInformation)
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

	if len(ct.SubProblems) > 0 {
		subProblems := make([]*v1.CheckDebugTrace, 0, len(ct.SubProblems))
		for _, subProblem := range ct.SubProblems {
			converted, err := convertCheckTrace(ctx, caveatContext, subProblem, reader)
			if err != nil {
				return nil, err
			}

			subProblems = append(subProblems, converted)
		}

		sort.Sort(sortByResource(subProblems))

		return &v1.CheckDebugTrace{
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
		}, nil
	}

	return &v1.CheckDebugTrace{
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
	}, nil
}

type sortByResource []*v1.CheckDebugTrace

func (a sortByResource) Len() int      { return len(a) }
func (a sortByResource) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortByResource) Less(i, j int) bool {
	return strings.Compare(stringRes(a[i].Resource), stringRes(a[j].Resource)) < 0
}

func stringRes(resource *v1.ObjectReference) string {
	return fmt.Sprintf("%s:%s", resource.ObjectType, resource.ObjectId)
}
