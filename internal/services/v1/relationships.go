package v1

import (
	"context"
	"errors"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

// NewPermissionsServer creates a PermissionsServiceServer instance.
func NewPermissionsServer(ds datastore.Datastore,
	nsm namespace.Manager,
	dispatch dispatch.Dispatcher,
	defaultDepth uint32,
) v1.PermissionsServiceServer {
	return &permissionServer{
		ds:           ds,
		nsm:          nsm,
		dispatch:     dispatch,
		defaultDepth: defaultDepth,
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary: grpcmw.ChainUnaryServer(
				grpcvalidate.UnaryServerInterceptor(),
				usagemetrics.UnaryServerInterceptor(),
				consistency.UnaryServerInterceptor(ds),
			),
			Stream: grpcmw.ChainStreamServer(
				grpcvalidate.StreamServerInterceptor(),
				usagemetrics.StreamServerInterceptor(),
				consistency.StreamServerInterceptor(ds),
			),
		},
	}
}

type permissionServer struct {
	v1.UnimplementedPermissionsServiceServer
	shared.WithServiceSpecificInterceptors

	ds           datastore.Datastore
	nsm          namespace.Manager
	dispatch     dispatch.Dispatcher
	defaultDepth uint32
}

func (ps *permissionServer) checkFilterComponent(ctx context.Context, objectType, optionalRelation string, revision decimal.Decimal) error {
	relationToTest := stringz.DefaultEmpty(optionalRelation, datastore.Ellipsis)
	allowEllipsis := optionalRelation == ""
	return ps.nsm.CheckNamespaceAndRelation(ctx, objectType, relationToTest, allowEllipsis, revision)
}

func (ps *permissionServer) checkFilterNamespaces(ctx context.Context, filter *v1.RelationshipFilter, revision decimal.Decimal) error {
	if err := ps.checkFilterComponent(ctx, filter.ResourceType, filter.OptionalRelation, revision); err != nil {
		return err
	}

	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		subjectRelation := ""
		if subjectFilter.OptionalRelation != nil {
			subjectRelation = subjectFilter.OptionalRelation.Relation
		}
		if err := ps.checkFilterComponent(ctx, subjectFilter.SubjectType, subjectRelation, revision); err != nil {
			return err
		}
	}

	return nil
}

func (ps *permissionServer) ReadRelationships(req *v1.ReadRelationshipsRequest, resp v1.PermissionsService_ReadRelationshipsServer) error {
	ctx := resp.Context()

	atRevision, revisionReadAt := consistency.MustRevisionFromContext(ctx)

	if err := ps.checkFilterNamespaces(ctx, req.RelationshipFilter, atRevision); err != nil {
		return rewritePermissionsError(ctx, err)
	}

	queryBuilder := ps.ds.QueryTuples(datastore.TupleQueryResourceFilter{
		ResourceType:             req.RelationshipFilter.ResourceType,
		OptionalResourceID:       req.RelationshipFilter.OptionalResourceId,
		OptionalResourceRelation: req.RelationshipFilter.OptionalRelation,
	}, atRevision)
	if req.RelationshipFilter.OptionalSubjectFilter != nil {
		queryBuilder = queryBuilder.WithSubjectFilter(req.RelationshipFilter.OptionalSubjectFilter)
	}

	tupleIterator, err := queryBuilder.Execute(ctx)
	if err != nil {
		return rewritePermissionsError(ctx, err)
	}
	defer tupleIterator.Close()

	for tuple := tupleIterator.Next(); tuple != nil; tuple = tupleIterator.Next() {
		tupleUserset := tuple.User.GetUserset()

		subjectRelation := ""
		if tupleUserset.Relation != datastore.Ellipsis {
			subjectRelation = tupleUserset.Relation
		}

		err := resp.Send(&v1.ReadRelationshipsResponse{
			ReadAt: revisionReadAt,
			Relationship: &v1.Relationship{
				Resource: &v1.ObjectReference{
					ObjectType: tuple.ObjectAndRelation.Namespace,
					ObjectId:   tuple.ObjectAndRelation.ObjectId,
				},
				Relation: tuple.ObjectAndRelation.Relation,
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: tupleUserset.Namespace,
						ObjectId:   tupleUserset.ObjectId,
					},
					OptionalRelation: subjectRelation,
				},
			},
		})
		if err != nil {
			return err
		}
	}
	if tupleIterator.Err() != nil {
		return status.Errorf(codes.Internal, "error when reading tuples: %s", err)
	}

	return nil
}

func (ps *permissionServer) WriteRelationships(ctx context.Context, req *v1.WriteRelationshipsRequest) (*v1.WriteRelationshipsResponse, error) {
	readRevision, err := ps.ds.HeadRevision(ctx)
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	for _, precond := range req.OptionalPreconditions {
		if err := ps.checkFilterNamespaces(ctx, precond.Filter, readRevision); err != nil {
			return nil, rewritePermissionsError(ctx, err)
		}
	}

	for _, update := range req.Updates {
		if err := ps.nsm.CheckNamespaceAndRelation(
			ctx,
			update.Relationship.Resource.ObjectType,
			update.Relationship.Relation,
			false,
			readRevision,
		); err != nil {
			return nil, rewritePermissionsError(ctx, err)
		}

		if err := ps.nsm.CheckNamespaceAndRelation(
			ctx,
			update.Relationship.Subject.Object.ObjectType,
			stringz.DefaultEmpty(update.Relationship.Subject.OptionalRelation, datastore.Ellipsis),
			true,
			readRevision,
		); err != nil {
			return nil, rewritePermissionsError(ctx, err)
		}

		_, ts, err := ps.nsm.ReadNamespaceAndTypes(ctx, update.Relationship.Resource.ObjectType, readRevision)
		if err != nil {
			return nil, err
		}

		if ts.IsPermission(update.Relationship.Relation) {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"cannot write a relationship to permission %s",
				update.Relationship.Relation,
			)
		}

		isAllowed, err := ts.IsAllowedDirectRelation(
			update.Relationship.Relation,
			update.Relationship.Subject.Object.ObjectType,
			stringz.DefaultEmpty(update.Relationship.Subject.OptionalRelation, datastore.Ellipsis),
		)
		if err != nil {
			return nil, err
		}

		if isAllowed == namespace.DirectRelationNotValid {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"subject %s is not allowed for the resource %s",
				tuple.StringSubjectRef(update.Relationship.Subject),
				tuple.StringObjectRef(update.Relationship.Resource),
			)
		}
	}

	revision, err := ps.ds.WriteTuples(ctx, req.OptionalPreconditions, req.Updates)
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	return &v1.WriteRelationshipsResponse{
		WrittenAt: zedtoken.NewFromRevision(revision),
	}, nil
}

func (ps *permissionServer) DeleteRelationships(ctx context.Context, req *v1.DeleteRelationshipsRequest) (*v1.DeleteRelationshipsResponse, error) {
	readRevision, err := ps.ds.HeadRevision(ctx)
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	if err := ps.checkFilterNamespaces(ctx, req.RelationshipFilter, readRevision); err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	revision, err := ps.ds.DeleteRelationships(ctx, req.OptionalPreconditions, req.RelationshipFilter)
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	return &v1.DeleteRelationshipsResponse{
		DeletedAt: zedtoken.NewFromRevision(revision),
	}, nil
}

func rewritePermissionsError(ctx context.Context, err error) error {
	var nsNotFoundError sharederrors.UnknownNamespaceError
	var relNotFoundError sharederrors.UnknownRelationError

	switch {
	case errors.As(err, &nsNotFoundError):
		fallthrough
	case errors.As(err, &relNotFoundError):
		fallthrough
	case errors.As(err, &datastore.ErrPreconditionFailed{}):
		return status.Errorf(codes.FailedPrecondition, "failed precondition: %s", err)

	case errors.As(err, &graph.ErrRequestCanceled{}):
		return status.Errorf(codes.Canceled, "request canceled: %s", err)

	case errors.As(err, &datastore.ErrInvalidRevision{}):
		return status.Errorf(codes.OutOfRange, "invalid zedtoken: %s", err)

	case errors.As(err, &datastore.ErrReadOnly{}):
		return serviceerrors.ErrServiceReadOnly

	case errors.As(err, &graph.ErrRelationMissingTypeInfo{}):
		return status.Errorf(codes.FailedPrecondition, "failed precondition: %s", err)

	case errors.As(err, &graph.ErrAlwaysFail{}):
		log.Ctx(ctx).Err(err)
		return status.Errorf(codes.Internal, "internal error: %s", err)

	default:
		log.Ctx(ctx).Err(err)
		return err
	}
}
