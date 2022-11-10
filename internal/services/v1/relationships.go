package v1

import (
	"context"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/jzelinskie/stringz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/middleware"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/util"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
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
}

// NewPermissionsServer creates a PermissionsServiceServer instance.
func NewPermissionsServer(
	dispatch dispatch.Dispatcher,
	config PermissionsServerConfig,
	caveatsEnabled bool,
) v1.PermissionsServiceServer {
	configWithDefaults := PermissionsServerConfig{
		MaxPreconditionsCount: defaultIfZero(config.MaxPreconditionsCount, 1000),
		MaxUpdatesPerWrite:    defaultIfZero(config.MaxUpdatesPerWrite, 1000),
		MaximumAPIDepth:       defaultIfZero(config.MaximumAPIDepth, 50),
	}

	return &permissionServer{
		dispatch:       dispatch,
		config:         configWithDefaults,
		caveatsEnabled: caveatsEnabled,
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
			),
		},
	}
}

type permissionServer struct {
	v1.UnimplementedPermissionsServiceServer
	shared.WithServiceSpecificInterceptors

	dispatch       dispatch.Dispatcher
	config         PermissionsServerConfig
	caveatsEnabled bool
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
		return rewriteError(ctx, err)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: 1,
	})

	tupleIterator, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilterFromPublicFilter(req.RelationshipFilter))
	if err != nil {
		return rewriteError(ctx, err)
	}
	defer tupleIterator.Close()

	for tpl := tupleIterator.Next(); tpl != nil; tpl = tupleIterator.Next() {
		err := resp.Send(&v1.ReadRelationshipsResponse{
			ReadAt:       revisionReadAt,
			Relationship: tuple.ToRelationship(tpl),
		})
		if err != nil {
			return err
		}
	}
	if tupleIterator.Err() != nil {
		return status.Errorf(codes.Internal, "error when reading tuples: %s", tupleIterator.Err())
	}

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
	referencedCaveatNamesWithContext := util.NewSet[string]()
	for _, update := range req.Updates {
		tupleStr := tuple.StringRelationship(update.Relationship)
		if !updateRelationshipSet.Add(tupleStr) {
			return nil, rewriteError(
				ctx,
				status.Errorf(
					codes.InvalidArgument,
					"found duplicate update operation for relationship %s",
					tupleStr,
				),
			)
		}

		// Only load the caveat if we need its type information for context checking.
		if hasNonEmptyCaveatContext(update) {
			if !ps.caveatsEnabled {
				return nil, fmt.Errorf("caveats are currently not supported")
			}
			referencedCaveatNamesWithContext.Add(update.Relationship.OptionalCaveat.CaveatName)
		}
	}

	// Execute the write operation(s).
	// TODO(jschorr): look into loading the type system once per type, rather than once per relationship
	revision, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		// Validate the preconditions.
		for _, precond := range req.OptionalPreconditions {
			if err := ps.checkFilterNamespaces(ctx, precond.Filter, rwt); err != nil {
				return err
			}
		}

		// Load caveats, if any.
		var referencedCaveatMap map[string]*core.CaveatDefinition
		if !referencedCaveatNamesWithContext.IsEmpty() {
			foundCaveats, err := rwt.ListCaveats(ctx, referencedCaveatNamesWithContext.AsSlice()...)
			if err != nil {
				return err
			}

			referencedCaveatMap = make(map[string]*core.CaveatDefinition, len(foundCaveats))
			for _, caveatDef := range foundCaveats {
				referencedCaveatMap[caveatDef.Name] = caveatDef
			}
		}

		// Validate the updates.
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

			// Build the type system for the object type.
			_, ts, err := namespace.ReadNamespaceAndTypes(
				ctx,
				update.Relationship.Resource.ObjectType,
				rwt,
			)
			if err != nil {
				return err
			}

			// Validate that the relationship is not writing to a permission.
			if ts.IsPermission(update.Relationship.Relation) {
				return status.Errorf(
					codes.InvalidArgument,
					"cannot write a relationship to permission %s",
					update.Relationship.Relation,
				)
			}

			// Validate the subject against the allowed relation(s).
			var relationToCheck *core.AllowedRelation
			var caveat *core.AllowedCaveat

			if update.Relationship.OptionalCaveat != nil {
				caveat = ns.AllowedCaveat(update.Relationship.OptionalCaveat.CaveatName)
			}

			if update.Relationship.Subject.Object.ObjectId == tuple.PublicWildcard {
				relationToCheck = ns.AllowedPublicNamespaceWithCaveat(update.Relationship.Subject.Object.ObjectType, caveat)
			} else {
				relationToCheck = ns.AllowedRelationWithCaveat(
					update.Relationship.Subject.Object.ObjectType,
					stringz.DefaultEmpty(
						update.Relationship.Subject.OptionalRelation,
						datastore.Ellipsis),
					caveat)
			}

			isAllowed, err := ts.HasAllowedRelation(
				update.Relationship.Relation,
				relationToCheck,
			)
			if err != nil {
				return err
			}

			if isAllowed != namespace.AllowedRelationValid {
				return status.Errorf(
					codes.InvalidArgument,
					"subjects of type `%s` are not allowed on relation `%v`",
					namespace.SourceForAllowedRelation(relationToCheck),
					tuple.StringObjectRef(update.Relationship.Resource),
				)
			}

			// Validate caveat and its context, if applicable.
			if hasNonEmptyCaveatContext(update) {
				caveat, ok := referencedCaveatMap[update.Relationship.OptionalCaveat.CaveatName]
				if !ok {
					// This won't happen since caveat is type checked above
					panic("caveat should have been type-checked but was not found")
				}

				// Verify that the provided context information matches the types of the parameters defined.
				_, err := caveats.ConvertContextToParameters(
					update.Relationship.OptionalCaveat.Context.AsMap(),
					caveat.ParameterTypes,
					caveats.ErrorForUnknownParameters,
				)
				if err != nil {
					return rewriteError(ctx, err)
				}
			}
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			// One request per precondition and one request for the actual writes.
			DispatchCount: uint32(len(req.OptionalPreconditions)) + 1,
		})

		if err := checkPreconditions(ctx, rwt, req.OptionalPreconditions); err != nil {
			return err
		}

		return rwt.WriteRelationships(ctx, tuple.UpdateFromRelationshipUpdates(req.Updates))
	})
	if err != nil {
		return nil, rewriteError(ctx, err)
	}

	return &v1.WriteRelationshipsResponse{
		WrittenAt: zedtoken.NewFromRevision(revision),
	}, nil
}

func hasNonEmptyCaveatContext(update *v1.RelationshipUpdate) bool {
	return update.Relationship.OptionalCaveat != nil &&
		update.Relationship.OptionalCaveat.CaveatName != "" &&
		update.Relationship.OptionalCaveat.Context != nil &&
		len(update.Relationship.OptionalCaveat.Context.GetFields()) > 0
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
		DeletedAt: zedtoken.NewFromRevision(revision),
	}, nil
}
