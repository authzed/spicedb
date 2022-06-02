package v1

import (
	"context"
	"errors"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

// NewPermissionsServer creates a PermissionsServiceServer instance.
func NewPermissionsServer(
	dispatch dispatch.Dispatcher,
	defaultDepth uint32,
) v1.PermissionsServiceServer {
	return &permissionServer{
		dispatch:     dispatch,
		defaultDepth: defaultDepth,
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary: grpcmw.ChainUnaryServer(
				grpcvalidate.UnaryServerInterceptor(),
				handwrittenvalidation.UnaryServerInterceptor,
				usagemetrics.UnaryServerInterceptor(),
			),
			Stream: grpcmw.ChainStreamServer(
				grpcvalidate.StreamServerInterceptor(),
				handwrittenvalidation.StreamServerInterceptor,
				usagemetrics.StreamServerInterceptor(),
			),
		},
	}
}

type permissionServer struct {
	v1.UnimplementedPermissionsServiceServer
	shared.WithServiceSpecificInterceptors

	dispatch     dispatch.Dispatcher
	defaultDepth uint32
}

func (ps *permissionServer) checkFilterComponent(ctx context.Context, objectType, optionalRelation string, ds datastore.Reader) error {
	relationToTest := stringz.DefaultEmpty(optionalRelation, datastore.Ellipsis)
	allowEllipsis := optionalRelation == ""
	return namespace.CheckNamespaceAndRelation(ctx, objectType, relationToTest, allowEllipsis, ds)
}

func (ps *permissionServer) checkFilterNamespaces(ctx context.Context, filter *v1.RelationshipFilter, ds datastore.Reader) error {
	if err := ps.checkFilterComponent(ctx, filter.ResourceType, filter.OptionalRelation, ds); err != nil {
		return err
	}

	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		subjectRelation := ""
		if subjectFilter.OptionalRelation != nil {
			subjectRelation = subjectFilter.OptionalRelation.Relation
		}
		if err := ps.checkFilterComponent(ctx, subjectFilter.SubjectType, subjectRelation, ds); err != nil {
			return err
		}
	}

	return nil
}

func (ps *permissionServer) ReadRelationships(req *v1.ReadRelationshipsRequest, resp v1.PermissionsService_ReadRelationshipsServer) error {
	ctx := resp.Context()
	atRevision, revisionReadAt := consistency.MustRevisionFromContext(ctx)
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	if err := ps.checkFilterNamespaces(ctx, req.RelationshipFilter, ds); err != nil {
		return rewritePermissionsError(ctx, err)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: 1,
	})

	tupleIterator, err := ds.QueryRelationships(ctx, req.RelationshipFilter)
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
	ds := datastoremw.MustFromContext(ctx)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		for _, precond := range req.OptionalPreconditions {
			if err := ps.checkFilterNamespaces(ctx, precond.Filter, rwt); err != nil {
				return err
			}
		}
		for _, update := range req.Updates {
			if err := tuple.ValidateResourceID(update.Relationship.Resource.ObjectId); err != nil {
				return err
			}

			if err := tuple.ValidateSubjectID(update.Relationship.Subject.Object.ObjectId); err != nil {
				return err
			}

			if err := namespace.CheckNamespaceAndRelation(
				ctx,
				update.Relationship.Resource.ObjectType,
				update.Relationship.Relation,
				false,
				rwt,
			); err != nil {
				return err
			}

			if err := namespace.CheckNamespaceAndRelation(
				ctx,
				update.Relationship.Subject.Object.ObjectType,
				stringz.DefaultEmpty(update.Relationship.Subject.OptionalRelation, datastore.Ellipsis),
				true,
				rwt,
			); err != nil {
				return err
			}

			_, ts, err := namespace.ReadNamespaceAndTypes(
				ctx,
				update.Relationship.Resource.ObjectType,
				rwt,
			)
			if err != nil {
				return err
			}

			if ts.IsPermission(update.Relationship.Relation) {
				return status.Errorf(
					codes.InvalidArgument,
					"cannot write a relationship to permission %s",
					update.Relationship.Relation,
				)
			}

			if update.Relationship.Subject.Object.ObjectId == tuple.PublicWildcard {
				isAllowed, err := ts.IsAllowedPublicNamespace(
					update.Relationship.Relation,
					update.Relationship.Subject.Object.ObjectType)
				if err != nil {
					return err
				}

				if isAllowed != namespace.PublicSubjectAllowed {
					return status.Errorf(
						codes.InvalidArgument,
						"wildcard subjects of type %s are not allowed on %v",
						update.Relationship.Subject.Object.ObjectType,
						tuple.StringObjectRef(update.Relationship.Resource),
					)
				}
			} else {
				isAllowed, err := ts.IsAllowedDirectRelation(
					update.Relationship.Relation,
					update.Relationship.Subject.Object.ObjectType,
					stringz.DefaultEmpty(update.Relationship.Subject.OptionalRelation, datastore.Ellipsis),
				)
				if err != nil {
					return err
				}

				if isAllowed == namespace.DirectRelationNotValid {
					return status.Errorf(
						codes.InvalidArgument,
						"subject %s is not allowed for the resource %s",
						tuple.StringSubjectRef(update.Relationship.Subject),
						tuple.StringObjectRef(update.Relationship.Resource),
					)
				}
			}
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			// One request per precondition and one request for the actual writes.
			DispatchCount: uint32(len(req.OptionalPreconditions)) + 1,
		})

		if err := shared.CheckPreconditions(ctx, rwt, req.OptionalPreconditions); err != nil {
			return err
		}

		return rwt.WriteRelationships(req.Updates)
	})
	if err != nil {
		return nil, rewritePermissionsError(ctx, err)
	}

	return &v1.WriteRelationshipsResponse{
		WrittenAt: zedtoken.NewFromRevision(revision),
	}, nil
}

func (ps *permissionServer) DeleteRelationships(ctx context.Context, req *v1.DeleteRelationshipsRequest) (*v1.DeleteRelationshipsResponse, error) {
	ds := datastoremw.MustFromContext(ctx)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		if err := ps.checkFilterNamespaces(ctx, req.RelationshipFilter, rwt); err != nil {
			return err
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			// One request per precondition and one request for the actual delete.
			DispatchCount: uint32(len(req.OptionalPreconditions)) + 1,
		})

		if err := shared.CheckPreconditions(ctx, rwt, req.OptionalPreconditions); err != nil {
			return err
		}

		return rwt.DeleteRelationships(req.RelationshipFilter)
	})
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
	case errors.As(err, &shared.ErrPreconditionFailed{}):
		return status.Errorf(codes.FailedPrecondition, "failed precondition: %s", err)

	case errors.As(err, &graph.ErrInvalidArgument{}):
		return status.Errorf(codes.InvalidArgument, "%s", err)

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
		log.Ctx(ctx).Err(err).Msg("received unexpected error")
		return err
	}
}
