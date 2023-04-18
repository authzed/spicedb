package v1

import (
	"context"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/jzelinskie/stringz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/middleware"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/streamtimeout"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/relationships"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/pagination"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/util"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

// PermissionsServerConfig is configuration for the permissions server.
type PermissionsServerConfig struct {
	// MaxUpdatesPerWrite holds the maximum number of updates allowed per
	// WriteRelationships call.
	MaxUpdatesPerWrite uint16

	// MaxPreconditionsCount holds the maximum number of preconditions allowed
	// on a WriteRelationships or DeleteRelationships call.
	MaxPreconditionsCount uint16

	// MaximumAPIDepth is the default/starting depth remaining for API calls made
	// to the permissions server.
	MaximumAPIDepth uint32

	// StreamingAPITimeout is the timeout for streaming APIs when no response has been
	// recently received.
	StreamingAPITimeout time.Duration

	// MaxCaveatContextSize defines the maximum length of the request caveat context in bytes
	MaxCaveatContextSize int

	// MaxDatastoreReadPageSize defines the maximum number of relationships loaded from the
	// datastore in one query.
	MaxDatastoreReadPageSize uint64
}

// NewPermissionsServer creates a PermissionsServiceServer instance.
func NewPermissionsServer(
	dispatch dispatch.Dispatcher,
	config PermissionsServerConfig,
) v1.PermissionsServiceServer {
	configWithDefaults := PermissionsServerConfig{
		MaxPreconditionsCount:    defaultIfZero(config.MaxPreconditionsCount, 1000),
		MaxUpdatesPerWrite:       defaultIfZero(config.MaxUpdatesPerWrite, 1000),
		MaximumAPIDepth:          defaultIfZero(config.MaximumAPIDepth, 50),
		StreamingAPITimeout:      defaultIfZero(config.StreamingAPITimeout, 30*time.Second),
		MaxCaveatContextSize:     config.MaxCaveatContextSize,
		MaxDatastoreReadPageSize: defaultIfZero(config.MaxDatastoreReadPageSize, 1_000),
	}

	return &permissionServer{
		dispatch: dispatch,
		config:   configWithDefaults,
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary: middleware.ChainUnaryServer(
				grpcvalidate.UnaryServerInterceptor(true),
				handwrittenvalidation.UnaryServerInterceptor,
				usagemetrics.UnaryServerInterceptor(),
			),
			Stream: middleware.ChainStreamServer(
				grpcvalidate.StreamServerInterceptor(true),
				handwrittenvalidation.StreamServerInterceptor,
				usagemetrics.StreamServerInterceptor(),
				streamtimeout.MustStreamServerInterceptor(configWithDefaults.StreamingAPITimeout),
			),
		},
	}
}

type permissionServer struct {
	v1.UnimplementedPermissionsServiceServer
	shared.WithServiceSpecificInterceptors

	dispatch dispatch.Dispatcher
	config   PermissionsServerConfig
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
	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	if err := ps.checkFilterNamespaces(ctx, req.RelationshipFilter, ds); err != nil {
		return rewriteError(ctx, err)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: 1,
	})

	tupleIterator, err := pagination.NewPaginatedIterator(
		ctx,
		ds,
		datastore.RelationshipsFilterFromPublicFilter(req.RelationshipFilter),
		ps.config.MaxDatastoreReadPageSize,
		options.ByResource,
	)
	if err != nil {
		return rewriteError(ctx, err)
	}
	defer tupleIterator.Close()

	for tpl := tupleIterator.Next(); tpl != nil; tpl = tupleIterator.Next() {
		if tupleIterator.Err() != nil {
			return status.Errorf(codes.Internal, "error when reading tuples: %s", tupleIterator.Err())
		}

		err := resp.Send(&v1.ReadRelationshipsResponse{
			ReadAt:       revisionReadAt,
			Relationship: tuple.ToRelationship(tpl),
		})
		if err != nil {
			return err
		}
	}
	tupleIterator.Close()
	return nil
}

func (ps *permissionServer) WriteRelationships(ctx context.Context, req *v1.WriteRelationshipsRequest) (*v1.WriteRelationshipsResponse, error) {
	ds := datastoremw.MustFromContext(ctx)

	// Ensure that the updates and preconditions are not over the configured limits.
	if len(req.Updates) > int(ps.config.MaxUpdatesPerWrite) {
		return nil, rewriteError(
			ctx,
			NewExceedsMaximumUpdatesErr(uint16(len(req.Updates)), ps.config.MaxUpdatesPerWrite),
		)
	}

	if len(req.OptionalPreconditions) > int(ps.config.MaxPreconditionsCount) {
		return nil, rewriteError(
			ctx,
			NewExceedsMaximumPreconditionsErr(uint16(len(req.OptionalPreconditions)), ps.config.MaxPreconditionsCount),
		)
	}

	// Check for duplicate updates and create the set of caveat names to load.
	updateRelationshipSet := util.NewSet[string]()
	for _, update := range req.Updates {
		tupleStr := tuple.StringRelationshipWithoutCaveat(update.Relationship)
		if !updateRelationshipSet.Add(tupleStr) {
			return nil, rewriteError(
				ctx,
				NewDuplicateRelationshipErr(update),
			)
		}
	}

	// Execute the write operation(s).
	revision, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		// Validate the preconditions.
		for _, precond := range req.OptionalPreconditions {
			if err := ps.checkFilterNamespaces(ctx, precond.Filter, rwt); err != nil {
				return err
			}
		}

		// Validate the updates.
		tupleUpdates := tuple.UpdateFromRelationshipUpdates(req.Updates)
		err := relationships.ValidateRelationshipUpdates(ctx, rwt, tupleUpdates)
		if err != nil {
			return rewriteError(ctx, err)
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			// One request per precondition and one request for the actual writes.
			DispatchCount: uint32(len(req.OptionalPreconditions)) + 1,
		})

		if err := checkPreconditions(ctx, rwt, req.OptionalPreconditions); err != nil {
			return err
		}

		return rwt.WriteRelationships(ctx, tupleUpdates)
	})
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	return &v1.WriteRelationshipsResponse{
		WrittenAt: zedtoken.MustNewFromRevision(revision),
	}, nil
}

func (ps *permissionServer) DeleteRelationships(ctx context.Context, req *v1.DeleteRelationshipsRequest) (*v1.DeleteRelationshipsResponse, error) {
	if len(req.OptionalPreconditions) > int(ps.config.MaxPreconditionsCount) {
		return nil, rewriteError(
			ctx,
			NewExceedsMaximumPreconditionsErr(uint16(len(req.OptionalPreconditions)), ps.config.MaxPreconditionsCount),
		)
	}

	ds := datastoremw.MustFromContext(ctx)

	revision, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		if err := ps.checkFilterNamespaces(ctx, req.RelationshipFilter, rwt); err != nil {
			return err
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			// One request per precondition and one request for the actual delete.
			DispatchCount: uint32(len(req.OptionalPreconditions)) + 1,
		})

		if err := checkPreconditions(ctx, rwt, req.OptionalPreconditions); err != nil {
			return err
		}

		return rwt.DeleteRelationships(ctx, req.RelationshipFilter)
	})
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	return &v1.DeleteRelationshipsResponse{
		DeletedAt: zedtoken.MustNewFromRevision(revision),
	}, nil
}
