package services_test

import (
	"context"
	"errors"
	"io"
	"sort"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"

	v1svc "github.com/authzed/spicedb/internal/services/v1"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
	"github.com/authzed/spicedb/pkg/zookie"
)

type serviceTester interface {
	Name() string
	Check(ctx context.Context, resource *core.ObjectAndRelation, subject *core.ObjectAndRelation, atRevision decimal.Decimal) (bool, error)
	Expand(ctx context.Context, resource *core.ObjectAndRelation, atRevision decimal.Decimal) (*core.RelationTupleTreeNode, error)
	Write(ctx context.Context, relationship *core.RelationTuple) error
	Read(ctx context.Context, namespaceName string, atRevision decimal.Decimal) ([]*core.RelationTuple, error)
	Lookup(ctx context.Context, resourceRelation *core.RelationReference, subject *core.ObjectAndRelation, atRevision decimal.Decimal) ([]string, error)
}

// v0ServiceTester tests the V0 API.
type v0ServiceTester struct {
	aclClient v0.ACLServiceClient
}

func (v0st v0ServiceTester) Name() string {
	return "v0"
}

func (v0st v0ServiceTester) Check(ctx context.Context, resource *core.ObjectAndRelation, subject *core.ObjectAndRelation, atRevision decimal.Decimal) (bool, error) {
	checkResp, err := v0st.aclClient.Check(ctx, &v0.CheckRequest{
		TestUserset: core.ToV0ObjectAndRelation(resource),
		User: &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: core.ToV0ObjectAndRelation(subject),
			},
		},
		AtRevision: core.ToV0Zookie(zookie.NewFromRevision(atRevision)),
	})
	if err != nil {
		return false, err
	}
	return checkResp.IsMember, nil
}

func (v0st v0ServiceTester) Expand(ctx context.Context, resource *core.ObjectAndRelation, atRevision decimal.Decimal) (*core.RelationTupleTreeNode, error) {
	expandResp, err := v0st.aclClient.Expand(ctx, &v0.ExpandRequest{
		Userset:    core.ToV0ObjectAndRelation(resource),
		AtRevision: core.ToV0Zookie(zookie.NewFromRevision(atRevision)),
	})
	if err != nil {
		return nil, err
	}
	return core.ToCoreRelationTupleTreeNode(expandResp.TreeNode), nil
}

func (v0st v0ServiceTester) Write(ctx context.Context, tpl *core.RelationTuple) error {
	_, err := v0st.aclClient.Write(ctx, &v0.WriteRequest{
		WriteConditions: []*v0.RelationTuple{core.ToV0RelationTuple(tpl)},
		Updates:         []*v0.RelationTupleUpdate{core.ToV0RelationTupleUpdate(tuple.Touch(tpl))},
	})
	return err
}

func (v0st v0ServiceTester) Read(ctx context.Context, namespaceName string, atRevision decimal.Decimal) ([]*core.RelationTuple, error) {
	result, err := v0st.aclClient.Read(context.Background(), &v0.ReadRequest{
		Tuplesets: []*v0.RelationTupleFilter{
			{Namespace: namespaceName},
		},
		AtRevision: core.ToV0Zookie(zookie.NewFromRevision(atRevision)),
	})
	if err != nil {
		return nil, err
	}

	var tuples []*v0.RelationTuple
	for _, tplSet := range result.Tuplesets {
		tuples = append(tuples, tplSet.Tuples...)
	}
	return core.ToCoreRelationTuples(tuples), nil
}

func (v0st v0ServiceTester) Lookup(ctx context.Context, resourceRelation *core.RelationReference, subject *core.ObjectAndRelation, atRevision decimal.Decimal) ([]string, error) {
	result, err := v0st.aclClient.Lookup(context.Background(), &v0.LookupRequest{
		User:           core.ToV0ObjectAndRelation(subject),
		ObjectRelation: core.ToV0RelationReference(resourceRelation),
		Limit:          ^uint32(0),
		AtRevision:     core.ToV0Zookie(zookie.NewFromRevision(atRevision)),
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(result.ResolvedObjectIds)
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

func (v1st v1ServiceTester) Check(ctx context.Context, resource *core.ObjectAndRelation, subject *core.ObjectAndRelation, atRevision decimal.Decimal) (bool, error) {
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

func (v1st v1ServiceTester) Expand(ctx context.Context, resource *core.ObjectAndRelation, atRevision decimal.Decimal) (*core.RelationTupleTreeNode, error) {
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

func (v1st v1ServiceTester) Write(ctx context.Context, relationship *core.RelationTuple) error {
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

func (v1st v1ServiceTester) Read(ctx context.Context, namespaceName string, atRevision decimal.Decimal) ([]*core.RelationTuple, error) {
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

	var tuples []*core.RelationTuple
	for {
		resp, err := readResp.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		tuples = append(tuples, &core.RelationTuple{
			ObjectAndRelation: &core.ObjectAndRelation{
				Namespace: resp.Relationship.Resource.ObjectType,
				ObjectId:  resp.Relationship.Resource.ObjectId,
				Relation:  resp.Relationship.Relation,
			},
			User: &core.User{
				UserOneof: &core.User_Userset{
					Userset: &core.ObjectAndRelation{
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

func (v1st v1ServiceTester) Lookup(ctx context.Context, resourceRelation *core.RelationReference, subject *core.ObjectAndRelation, atRevision decimal.Decimal) ([]string, error) {
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
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		objectIds = append(objectIds, resp.ResourceObjectId)
	}

	sort.Strings(objectIds)
	return objectIds, nil
}
