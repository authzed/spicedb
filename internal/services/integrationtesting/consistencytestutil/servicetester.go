package consistencytestutil

import (
	"context"
	"errors"
	"io"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	v1svc "github.com/authzed/spicedb/internal/services/v1"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

func ServiceTesters(conn *grpc.ClientConn) []ServiceTester {
	return []ServiceTester{
		v1ServiceTester{v1.NewPermissionsServiceClient(conn), v1.NewExperimentalServiceClient(conn)},
	}
}

type ServiceTester interface {
	Name() string
	Check(ctx context.Context, resource tuple.ObjectAndRelation, subject tuple.ObjectAndRelation, atRevision datastore.Revision, caveatContext map[string]any) (v1.CheckPermissionResponse_Permissionship, error)
	Expand(ctx context.Context, resource tuple.ObjectAndRelation, atRevision datastore.Revision) (*core.RelationTupleTreeNode, error)
	Write(ctx context.Context, relationship tuple.Relationship) error
	Read(ctx context.Context, namespaceName string, atRevision datastore.Revision) ([]tuple.Relationship, error)
	LookupResources(ctx context.Context, resourceRelation tuple.RelationReference, subject tuple.ObjectAndRelation, atRevision datastore.Revision, cursor *v1.Cursor, limit uint32, caveatContext map[string]any) ([]*v1.LookupResourcesResponse, *v1.Cursor, error)
	LookupSubjects(ctx context.Context, resource tuple.ObjectAndRelation, subjectRelation tuple.RelationReference, atRevision datastore.Revision, caveatContext map[string]any) (map[string]*v1.LookupSubjectsResponse, error)
	// NOTE: ExperimentalService/BulkCheckPermission has been promoted to PermissionsService/CheckBulkPermissions
	BulkCheck(ctx context.Context, items []*v1.BulkCheckPermissionRequestItem, atRevision datastore.Revision) ([]*v1.BulkCheckPermissionPair, error)
	CheckBulk(ctx context.Context, items []*v1.CheckBulkPermissionsRequestItem, atRevision datastore.Revision) ([]*v1.CheckBulkPermissionsPair, error)
}

func optionalizeRelation(relation string) string {
	if relation == datastore.Ellipsis {
		return ""
	}

	return relation
}

// v1ServiceTester tests the V1 API.
type v1ServiceTester struct {
	permClient v1.PermissionsServiceClient
	expClient  v1.ExperimentalServiceClient
}

func (v1st v1ServiceTester) Name() string {
	return "v1"
}

func (v1st v1ServiceTester) Check(ctx context.Context, resource tuple.ObjectAndRelation, subject tuple.ObjectAndRelation, atRevision datastore.Revision, caveatContext map[string]any) (v1.CheckPermissionResponse_Permissionship, error) {
	var context *structpb.Struct
	if caveatContext != nil {
		built, err := structpb.NewStruct(caveatContext)
		if err != nil {
			return v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED, err
		}
		context = built
	}

	checkResp, err := v1st.permClient.CheckPermission(ctx, &v1.CheckPermissionRequest{
		Resource: &v1.ObjectReference{
			ObjectType: resource.ObjectType,
			ObjectId:   resource.ObjectID,
		},
		Permission: resource.Relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: subject.ObjectType,
				ObjectId:   subject.ObjectID,
			},
			OptionalRelation: optionalizeRelation(subject.Relation),
		},
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(atRevision),
			},
		},
		Context: context,
	})
	if err != nil {
		return v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED, err
	}
	return checkResp.Permissionship, nil
}

func (v1st v1ServiceTester) Expand(ctx context.Context, resource tuple.ObjectAndRelation, atRevision datastore.Revision) (*core.RelationTupleTreeNode, error) {
	expandResp, err := v1st.permClient.ExpandPermissionTree(ctx, &v1.ExpandPermissionTreeRequest{
		Resource: &v1.ObjectReference{
			ObjectType: resource.ObjectType,
			ObjectId:   resource.ObjectID,
		},
		Permission: resource.Relation,
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(atRevision),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return v1svc.TranslateRelationshipTree(expandResp.TreeRoot), nil
}

func (v1st v1ServiceTester) Write(ctx context.Context, relationship tuple.Relationship) error {
	preconditions := []*v1.Precondition{
		{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.ToV1Filter(relationship),
		},
	}

	if relationship.OptionalExpiration != nil {
		preconditions = nil
	}

	_, err := v1st.permClient.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
		OptionalPreconditions: preconditions,
		Updates:               []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Touch(relationship))},
	})
	return err
}

func (v1st v1ServiceTester) Read(_ context.Context, namespaceName string, atRevision datastore.Revision) ([]tuple.Relationship, error) {
	readResp, err := v1st.permClient.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType: namespaceName,
		},
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(atRevision),
			},
		},
	})
	if err != nil {
		return nil, err
	}

	var rels []tuple.Relationship
	for {
		resp, err := readResp.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		rels = append(rels, tuple.FromV1Relationship(resp.Relationship))
	}

	return rels, nil
}

func (v1st v1ServiceTester) LookupResources(_ context.Context, resourceRelation tuple.RelationReference, subject tuple.ObjectAndRelation, atRevision datastore.Revision, cursor *v1.Cursor, limit uint32, caveatContext map[string]any) ([]*v1.LookupResourcesResponse, *v1.Cursor, error) {
	var builtContext *structpb.Struct
	if caveatContext != nil {
		built, err := structpb.NewStruct(caveatContext)
		if err != nil {
			return nil, nil, err
		}
		builtContext = built
	}

	lookupResp, err := v1st.permClient.LookupResources(context.Background(), &v1.LookupResourcesRequest{
		ResourceObjectType: resourceRelation.ObjectType,
		Permission:         resourceRelation.Relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: subject.ObjectType,
				ObjectId:   subject.ObjectID,
			},
			OptionalRelation: optionalizeRelation(subject.Relation),
		},
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(atRevision),
			},
		},
		OptionalLimit:  limit,
		OptionalCursor: cursor,
		Context:        builtContext,
	})
	if err != nil {
		return nil, nil, err
	}

	var lastCursor *v1.Cursor
	found := []*v1.LookupResourcesResponse{}
	for {
		resp, err := lookupResp.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, nil, err
		}

		found = append(found, resp)
		lastCursor = resp.AfterResultCursor
	}
	return found, lastCursor, nil
}

func (v1st v1ServiceTester) LookupSubjects(_ context.Context, resource tuple.ObjectAndRelation, subjectRelation tuple.RelationReference, atRevision datastore.Revision, caveatContext map[string]any) (map[string]*v1.LookupSubjectsResponse, error) {
	var builtContext *structpb.Struct
	if caveatContext != nil {
		built, err := structpb.NewStruct(caveatContext)
		if err != nil {
			return nil, err
		}
		builtContext = built
	}

	lookupResp, err := v1st.permClient.LookupSubjects(context.Background(), &v1.LookupSubjectsRequest{
		Resource: &v1.ObjectReference{
			ObjectType: resource.ObjectType,
			ObjectId:   resource.ObjectID,
		},
		Permission:              resource.Relation,
		SubjectObjectType:       subjectRelation.ObjectType,
		OptionalSubjectRelation: optionalizeRelation(subjectRelation.Relation),
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(atRevision),
			},
		},
		Context: builtContext,
	})
	if err != nil {
		return nil, err
	}

	found := map[string]*v1.LookupSubjectsResponse{}
	for {
		resp, err := lookupResp.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		found[resp.Subject.SubjectObjectId] = resp
	}
	return found, nil
}

func (v1st v1ServiceTester) BulkCheck(ctx context.Context, items []*v1.BulkCheckPermissionRequestItem, atRevision datastore.Revision) ([]*v1.BulkCheckPermissionPair, error) {
	result, err := v1st.expClient.BulkCheckPermission(ctx, &v1.BulkCheckPermissionRequest{
		Items: items,
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(atRevision),
			},
		},
	})
	if err != nil {
		return nil, err
	}

	return result.Pairs, nil
}

func (v1st v1ServiceTester) CheckBulk(ctx context.Context, items []*v1.CheckBulkPermissionsRequestItem, atRevision datastore.Revision) ([]*v1.CheckBulkPermissionsPair, error) {
	result, err := v1st.permClient.CheckBulkPermissions(ctx, &v1.CheckBulkPermissionsRequest{
		Items: items,
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(atRevision),
			},
		},
	})
	if err != nil {
		return nil, err
	}

	return result.Pairs, nil
}
