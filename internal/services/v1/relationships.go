package v1

import (
	"context"
	"errors"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

// RegisterPermissionsServer adds a permissions server to a grpc service registrar
// This is preferred over manually registering the service; it will add required middleware
func RegisterPermissionsServer(
	r grpc.ServiceRegistrar,
	ds datastore.Datastore,
	nsm namespace.Manager,
	dispatch dispatch.Dispatcher,
	defaultDepth uint32,
) *grpc.ServiceDesc {
	s := newPermissionsServer(ds, nsm, dispatch, defaultDepth)

	wrapped := grpcutil.WrapMethods(
		v1.PermissionsService_ServiceDesc,
		grpcvalidate.UnaryServerInterceptor(),
		consistency.UnaryServerInterceptor(ds),
	)

	wrapped = grpcutil.WrapStreams(
		*wrapped,
		grpcvalidate.StreamServerInterceptor(),
		consistency.StreamServerInterceptor(ds),
	)

	r.RegisterService(wrapped, s)
	return &v1.PermissionsService_ServiceDesc
}

func newPermissionsServer(ds datastore.Datastore,
	nsm namespace.Manager,
	dispatch dispatch.Dispatcher,
	defaultDepth uint32,
) v1.PermissionsServiceServer {
	return &permissionServer{ds: ds, nsm: nsm, dispatch: dispatch, defaultDepth: defaultDepth}
}

type permissionServer struct {
	v1.UnimplementedPermissionsServiceServer

	ds           datastore.Datastore
	nsm          namespace.Manager
	dispatch     dispatch.Dispatcher
	defaultDepth uint32
}

func (ps *permissionServer) checkObjectFilterNamespaces(ctx context.Context, filter *v1.ObjectFilter) error {
	return ps.nsm.CheckNamespaceAndRelation(
		ctx,
		filter.ObjectType,
		stringz.DefaultEmpty(filter.OptionalRelation, datastore.Ellipsis),
		filter.OptionalRelation == "",
	)
}

func (ps *permissionServer) checkFilterNamespaces(ctx context.Context, filter *v1.RelationshipFilter) error {
	if err := ps.checkObjectFilterNamespaces(ctx, filter.ResourceFilter); err != nil {
		return err
	}

	if filter.OptionalSubjectFilter != nil {
		if err := ps.checkObjectFilterNamespaces(ctx, filter.OptionalSubjectFilter); err != nil {
			return err
		}
	}

	return nil
}

func (ps *permissionServer) ReadRelationships(req *v1.ReadRelationshipsRequest, resp v1.PermissionsService_ReadRelationshipsServer) error {
	ctx := resp.Context()

	atRevision, revisionReadAt := consistency.MustRevisionFromContext(ctx)

	if err := ps.checkFilterNamespaces(ctx, req.RelationshipFilter); err != nil {
		return rewritePermissionsError(err)
	}

	queryBuilder := ps.ds.QueryTuples(req.RelationshipFilter.ResourceFilter, atRevision)
	if req.RelationshipFilter.OptionalSubjectFilter != nil {
		queryBuilder.WithUsersetFilter(req.RelationshipFilter.OptionalSubjectFilter)
	}

	tupleIterator, err := queryBuilder.Execute(ctx)
	if err != nil {
		return rewritePermissionsError(err)
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

func (ps *permissionServer) DeleteRelationships(ctx context.Context, req *v1.DeleteRelationshipsRequest) (*v1.DeleteRelationshipsResponse, error) {
	if err := ps.checkFilterNamespaces(ctx, req.RelationshipFilter); err != nil {
		return nil, rewritePermissionsError(err)
	}

	revision, err := ps.ds.DeleteRelationships(ctx, req.OptionalPreconditions, req.RelationshipFilter)
	if err != nil {
		return nil, rewritePermissionsError(err)
	}

	return &v1.DeleteRelationshipsResponse{
		DeletedAt: zedtoken.NewFromRevision(revision),
	}, nil
}

func rewritePermissionsError(err error) error {
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
		log.Err(err)
		return status.Errorf(codes.Internal, "internal error: %s", err)

	default:
		log.Err(err)
		return err
	}
}
