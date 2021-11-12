package services

import (
	"context"
	"io"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
	"github.com/authzed/spicedb/pkg/zookie"
)

type serviceTester interface {
	Name() string
	Check(ctx context.Context, resource *v0.ObjectAndRelation, subject *v0.ObjectAndRelation, atRevision decimal.Decimal) (bool, error)
	Expand(ctx context.Context, resource *v0.ObjectAndRelation, atRevision decimal.Decimal) (*v0.RelationTupleTreeNode, error)
	Write(ctx context.Context, relationship *v0.RelationTuple) error
	Read(ctx context.Context, namespaceName string, atRevision decimal.Decimal) ([]*v0.RelationTuple, error)
	Lookup(ctx context.Context, resourceRelation *v0.RelationReference, subject *v0.ObjectAndRelation, atRevision decimal.Decimal) ([]string, error)
}

// v0ServiceTester tests the V0 API.
type v0ServiceTester struct {
	srv v0.ACLServiceServer
}

func (v0st v0ServiceTester) Name() string {
	return "v0"
}

func (v0st v0ServiceTester) Check(ctx context.Context, resource *v0.ObjectAndRelation, subject *v0.ObjectAndRelation, atRevision decimal.Decimal) (bool, error) {
	checkResp, err := v0st.srv.Check(ctx, &v0.CheckRequest{
		TestUserset: resource,
		User: &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: subject,
			},
		},
		AtRevision: zookie.NewFromRevision(atRevision),
	})
	if err != nil {
		return false, err
	}
	return checkResp.IsMember, nil
}

func (v0st v0ServiceTester) Expand(ctx context.Context, resource *v0.ObjectAndRelation, atRevision decimal.Decimal) (*v0.RelationTupleTreeNode, error) {
	expandResp, err := v0st.srv.Expand(ctx, &v0.ExpandRequest{
		Userset:    resource,
		AtRevision: zookie.NewFromRevision(atRevision),
	})
	if err != nil {
		return nil, err
	}
	return expandResp.TreeNode, nil
}

func (v0st v0ServiceTester) Write(ctx context.Context, tpl *v0.RelationTuple) error {
	_, err := v0st.srv.Write(ctx, &v0.WriteRequest{
		WriteConditions: []*v0.RelationTuple{tpl},
		Updates:         []*v0.RelationTupleUpdate{tuple.Touch(tpl)},
	})
	return err
}

func (v0st v0ServiceTester) Read(ctx context.Context, namespaceName string, atRevision decimal.Decimal) ([]*v0.RelationTuple, error) {
	result, err := v0st.srv.Read(context.Background(), &v0.ReadRequest{
		Tuplesets: []*v0.RelationTupleFilter{
			{Namespace: namespaceName},
		},
		AtRevision: zookie.NewFromRevision(atRevision),
	})
	if err != nil {
		return nil, err
	}

	var tuples []*v0.RelationTuple
	for _, tplSet := range result.Tuplesets {
		tuples = append(tuples, tplSet.Tuples...)
	}
	return tuples, nil
}

func (v0st v0ServiceTester) Lookup(ctx context.Context, resourceRelation *v0.RelationReference, subject *v0.ObjectAndRelation, atRevision decimal.Decimal) ([]string, error) {
	result, err := v0st.srv.Lookup(context.Background(), &v0.LookupRequest{
		User:           subject,
		ObjectRelation: resourceRelation,
		Limit:          ^uint32(0),
		AtRevision:     zookie.NewFromRevision(atRevision),
	})
	if err != nil {
		return nil, err
	}
	return result.ResolvedObjectIds, nil
}

func optionalizeRelation(relation string) string {
	if relation == datastore.Ellipsis {
		return ""
	}

	return relation
}

func deoptionalizeRelation(relation string) string {
	if relation == "" {
		return datastore.Ellipsis
	}

	return relation
}

// v1ServiceTester tests the V1 API.
type v1ServiceTester struct {
	permClient v1.PermissionsServiceClient
}

func (v1st v1ServiceTester) Name() string {
	return "v1"
}

func (v1st v1ServiceTester) Check(ctx context.Context, resource *v0.ObjectAndRelation, subject *v0.ObjectAndRelation, atRevision decimal.Decimal) (bool, error) {
	checkResp, err := v1st.permClient.CheckPermission(ctx, &v1.CheckPermissionRequest{
		Resource: &v1.ObjectReference{
			ObjectType: resource.Namespace,
			ObjectId:   resource.ObjectId,
		},
		Permission: resource.Relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: subject.Namespace,
				ObjectId:   subject.ObjectId,
			},
			OptionalRelation: optionalizeRelation(subject.Relation),
		},
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.NewFromRevision(atRevision),
			},
		},
	})
	if err != nil {
		return false, err
	}
	return checkResp.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, nil
}

func (v1st v1ServiceTester) Expand(ctx context.Context, resource *v0.ObjectAndRelation, atRevision decimal.Decimal) (*v0.RelationTupleTreeNode, error) {
	expandResp, err := v1st.permClient.ExpandPermissionTree(ctx, &v1.ExpandPermissionTreeRequest{
		Resource: &v1.ObjectReference{
			ObjectType: resource.Namespace,
			ObjectId:   resource.ObjectId,
		},
		Permission: resource.Relation,
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.NewFromRevision(atRevision),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return v1svc.TranslateRelationshipTree(expandResp.TreeRoot), nil
}

func (v1st v1ServiceTester) Write(ctx context.Context, relationship *v0.RelationTuple) error {
	_, err := v1st.permClient.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
		OptionalPreconditions: []*v1.Precondition{
			{
				Operation: v1.Precondition_OPERATION_MUST_MATCH,
				Filter:    tuple.MustToFilter(relationship),
			},
		},
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Touch(relationship))},
	})
	return err
}

func (v1st v1ServiceTester) Read(ctx context.Context, namespaceName string, atRevision decimal.Decimal) ([]*v0.RelationTuple, error) {
	readResp, err := v1st.permClient.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType: namespaceName,
		},
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.NewFromRevision(atRevision),
			},
		},
	})
	if err != nil {
		return nil, err
	}

	var tuples []*v0.RelationTuple
	for {
		resp, err := readResp.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		tuples = append(tuples, &v0.RelationTuple{
			ObjectAndRelation: &v0.ObjectAndRelation{
				Namespace: resp.Relationship.Resource.ObjectType,
				ObjectId:  resp.Relationship.Resource.ObjectId,
				Relation:  resp.Relationship.Relation,
			},
			User: &v0.User{
				UserOneof: &v0.User_Userset{
					Userset: &v0.ObjectAndRelation{
						Namespace: resp.Relationship.Subject.Object.ObjectType,
						ObjectId:  resp.Relationship.Subject.Object.ObjectId,
						Relation:  deoptionalizeRelation(resp.Relationship.Subject.OptionalRelation),
					},
				},
			},
		})
	}

	return tuples, nil
}

func (v1st v1ServiceTester) Lookup(ctx context.Context, resourceRelation *v0.RelationReference, subject *v0.ObjectAndRelation, atRevision decimal.Decimal) ([]string, error) {
	lookupResp, err := v1st.permClient.LookupResources(context.Background(), &v1.LookupResourcesRequest{
		ResourceObjectType: resourceRelation.Namespace,
		Permission:         resourceRelation.Relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: subject.Namespace,
				ObjectId:   subject.ObjectId,
			},
			OptionalRelation: optionalizeRelation(subject.Relation),
		},
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.NewFromRevision(atRevision),
			},
		},
	})
	if err != nil {
		return nil, err
	}

	var objectIds []string
	for {
		resp, err := lookupResp.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		objectIds = append(objectIds, resp.ResourceObjectId)
	}

	return objectIds, nil
}
