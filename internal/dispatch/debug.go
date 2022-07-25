package dispatch

import (
	"context"
	"strings"

	"github.com/authzed/spicedb/pkg/tuple"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
)

// ConvertDispatchDebugInformation converts dispatch debug information found in the response metadata
// into DebugInformation returnable to the API.
func ConvertDispatchDebugInformation(ctx context.Context, metadata *dispatch.ResponseMeta, reader datastore.Reader) (*v1.DebugInformation, error) {
	debugInfo := metadata.DebugInfo
	if debugInfo == nil {
		return nil, nil
	}

	namespaces, err := reader.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	schema := ""
	for _, namespace := range namespaces {
		generated, _ := generator.GenerateSource(namespace)
		schema += generated
		schema += "\n\n"
	}

	return &v1.DebugInformation{
		Check:      convertCheckTrace(debugInfo.Check),
		SchemaUsed: strings.TrimSpace(schema),
	}, nil
}

func convertCheckTrace(ct *dispatch.CheckDebugTrace) *v1.CheckDebugTrace {
	if ct == nil {
		return nil
	}

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

	result := v1.CheckDebugTrace_PERMISSIONSHIP_NO_PERMISSION
	if ct.HasPermission {
		result = v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION
	}

	if len(ct.SubProblems) > 0 {
		subProblems := make([]*v1.CheckDebugTrace, 0, len(ct.SubProblems))
		for _, subProblem := range ct.SubProblems {
			subProblems = append(subProblems, convertCheckTrace(subProblem))
		}

		return &v1.CheckDebugTrace{
			Resource: &v1.ObjectReference{
				ObjectType: ct.Request.ResourceAndRelation.Namespace,
				ObjectId:   ct.Request.ResourceAndRelation.ObjectId,
			},
			Permission:     ct.Request.ResourceAndRelation.Relation,
			PermissionType: permissionType,
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: ct.Request.Subject.Namespace,
					ObjectId:   ct.Request.Subject.ObjectId,
				},
				OptionalRelation: subRelation,
			},
			Result: result,
			Resolution: &v1.CheckDebugTrace_SubProblems_{
				SubProblems: &v1.CheckDebugTrace_SubProblems{
					Traces: subProblems,
				},
			},
		}
	}

	return &v1.CheckDebugTrace{
		Resource: &v1.ObjectReference{
			ObjectType: ct.Request.ResourceAndRelation.Namespace,
			ObjectId:   ct.Request.ResourceAndRelation.ObjectId,
		},
		Permission:     ct.Request.ResourceAndRelation.Relation,
		PermissionType: permissionType,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: ct.Request.Subject.Namespace,
				ObjectId:   ct.Request.Subject.ObjectId,
			},
			OptionalRelation: subRelation,
		},
		Result: result,
		Resolution: &v1.CheckDebugTrace_WasCachedResult{
			WasCachedResult: ct.IsCachedResult,
		},
	}
}
