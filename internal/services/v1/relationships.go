package v1

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/jzelinskie/stringz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/middleware"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/streamtimeout"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/relationships"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/pagination"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

var writeUpdateCounter = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "v1",
	Name:      "write_relationships_updates",
	Help:      "The update counts for the WriteRelationships calls",
	Buckets:   []float64{0, 1, 2, 5, 10, 15, 25, 50, 100, 250, 500, 1000},
}, []string{"kind"})

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

	// MaxRelationshipContextSize defines the maximum length of a relationship's context in bytes
	MaxRelationshipContextSize int

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
		MaxPreconditionsCount:      defaultIfZero(config.MaxPreconditionsCount, 1000),
		MaxUpdatesPerWrite:         defaultIfZero(config.MaxUpdatesPerWrite, 1000),
		MaximumAPIDepth:            defaultIfZero(config.MaximumAPIDepth, 50),
		StreamingAPITimeout:        defaultIfZero(config.StreamingAPITimeout, 30*time.Second),
		MaxCaveatContextSize:       defaultIfZero(config.MaxCaveatContextSize, 4096),
		MaxRelationshipContextSize: defaultIfZero(config.MaxRelationshipContextSize, 25_000),
		MaxDatastoreReadPageSize:   defaultIfZero(config.MaxDatastoreReadPageSize, 1_000),
	}

	return &permissionServer{
		dispatch: dispatch,
		config:   configWithDefaults,
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary: middleware.ChainUnaryServer(
				grpcvalidate.UnaryServerInterceptor(),
				handwrittenvalidation.UnaryServerInterceptor,
				usagemetrics.UnaryServerInterceptor(),
			),
			Stream: middleware.ChainStreamServer(
				grpcvalidate.StreamServerInterceptor(),
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
		return ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	if err := ps.checkFilterNamespaces(ctx, req.RelationshipFilter, ds); err != nil {
		return ps.rewriteError(ctx, err)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: 1,
	})

	limit := 0
	var startCursor options.Cursor

	rrRequestHash, err := computeReadRelationshipsRequestHash(req)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	if req.OptionalCursor != nil {
		decodedCursor, err := cursor.DecodeToDispatchCursor(req.OptionalCursor, rrRequestHash)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		if len(decodedCursor.Sections) != 1 {
			return ps.rewriteError(ctx, NewInvalidCursorErr("did not find expected resume relationship"))
		}

		parsed := tuple.Parse(decodedCursor.Sections[0])
		if parsed == nil {
			return ps.rewriteError(ctx, NewInvalidCursorErr("could not parse resume relationship"))
		}

		startCursor = options.Cursor(parsed)
	}

	pageSize := ps.config.MaxDatastoreReadPageSize
	if req.OptionalLimit > 0 {
		limit = int(req.OptionalLimit)
		if uint64(limit) < pageSize {
			pageSize = uint64(limit)
		}
	}

	tupleIterator, err := pagination.NewPaginatedIterator(
		ctx,
		ds,
		datastore.RelationshipsFilterFromPublicFilter(req.RelationshipFilter),
		pageSize,
		options.ByResource,
		startCursor,
	)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}
	defer tupleIterator.Close()

	response := &v1.ReadRelationshipsResponse{
		ReadAt: revisionReadAt,
	}
	targetRel := tuple.NewRelationship()
	targetCaveat := &v1.ContextualizedCaveat{}
	returnedCount := 0

	dispatchCursor := &dispatchv1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{""},
	}

	for tpl := tupleIterator.Next(); tpl != nil; tpl = tupleIterator.Next() {
		if limit > 0 && returnedCount >= limit {
			break
		}

		if tupleIterator.Err() != nil {
			return ps.rewriteError(ctx, fmt.Errorf("error when reading tuples: %w", tupleIterator.Err()))
		}

		dispatchCursor.Sections[0] = tuple.StringWithoutCaveat(tpl)
		encodedCursor, err := cursor.EncodeFromDispatchCursor(dispatchCursor, rrRequestHash, atRevision)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		tuple.MustToRelationshipMutating(tpl, targetRel, targetCaveat)
		response.Relationship = targetRel
		response.AfterResultCursor = encodedCursor
		err = resp.Send(response)
		if err != nil {
			return ps.rewriteError(ctx, fmt.Errorf("error when streaming tuple: %w", err))
		}
		returnedCount++
	}

	if tupleIterator.Err() != nil {
		return ps.rewriteError(ctx, fmt.Errorf("error when reading tuples: %w", tupleIterator.Err()))
	}

	tupleIterator.Close()
	return nil
}

func (ps *permissionServer) WriteRelationships(ctx context.Context, req *v1.WriteRelationshipsRequest) (*v1.WriteRelationshipsResponse, error) {
	ds := datastoremw.MustFromContext(ctx)

	// Ensure that the updates and preconditions are not over the configured limits.
	if len(req.Updates) > int(ps.config.MaxUpdatesPerWrite) {
		return nil, ps.rewriteError(
			ctx,
			NewExceedsMaximumUpdatesErr(uint16(len(req.Updates)), ps.config.MaxUpdatesPerWrite),
		)
	}

	if len(req.OptionalPreconditions) > int(ps.config.MaxPreconditionsCount) {
		return nil, ps.rewriteError(
			ctx,
			NewExceedsMaximumPreconditionsErr(uint16(len(req.OptionalPreconditions)), ps.config.MaxPreconditionsCount),
		)
	}

	// Check for duplicate updates and create the set of caveat names to load.
	updateRelationshipSet := mapz.NewSet[string]()
	for _, update := range req.Updates {
		tupleStr := tuple.StringRelationshipWithoutCaveat(update.Relationship)
		if !updateRelationshipSet.Add(tupleStr) {
			return nil, ps.rewriteError(
				ctx,
				NewDuplicateRelationshipErr(update),
			)
		}
		if proto.Size(update.Relationship.OptionalCaveat) > ps.config.MaxRelationshipContextSize {
			return nil, ps.rewriteError(
				ctx,
				NewMaxRelationshipContextError(update, ps.config.MaxRelationshipContextSize),
			)
		}
	}

	// Execute the write operation(s).
	tupleUpdates := tuple.UpdateFromRelationshipUpdates(req.Updates)
	revision, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		// Validate the preconditions.
		for _, precond := range req.OptionalPreconditions {
			if err := ps.checkFilterNamespaces(ctx, precond.Filter, rwt); err != nil {
				return err
			}
		}

		// Validate the updates.
		err := relationships.ValidateRelationshipUpdates(ctx, rwt, tupleUpdates)
		if err != nil {
			return ps.rewriteError(ctx, err)
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
		return nil, ps.rewriteError(ctx, err)
	}

	// Log a metric of the counts of the different kinds of update operations.
	updateCountByOperation := make(map[v1.RelationshipUpdate_Operation]int, 0)
	for _, update := range req.Updates {
		updateCountByOperation[update.Operation]++
	}

	for kind, count := range updateCountByOperation {
		writeUpdateCounter.WithLabelValues(v1.RelationshipUpdate_Operation_name[int32(kind)]).Observe(float64(count))
	}

	return &v1.WriteRelationshipsResponse{
		WrittenAt: zedtoken.MustNewFromRevision(revision),
	}, nil
}

func (ps *permissionServer) DeleteRelationships(ctx context.Context, req *v1.DeleteRelationshipsRequest) (*v1.DeleteRelationshipsResponse, error) {
	if len(req.OptionalPreconditions) > int(ps.config.MaxPreconditionsCount) {
		return nil, ps.rewriteError(
			ctx,
			NewExceedsMaximumPreconditionsErr(uint16(len(req.OptionalPreconditions)), ps.config.MaxPreconditionsCount),
		)
	}

	ds := datastoremw.MustFromContext(ctx)
	deletionProgress := v1.DeleteRelationshipsResponse_DELETION_PROGRESS_COMPLETE

	revision, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		if err := ps.checkFilterNamespaces(ctx, req.RelationshipFilter, rwt); err != nil {
			return err
		}

		var deleteMutations []*core.RelationTupleUpdate

		if req.OptionalLimit > 0 {
			limit := uint64(req.OptionalLimit)
			deleteMutations = make([]*core.RelationTupleUpdate, 0, limit)

			limitPlusOne := limit + 1
			filter := datastore.RelationshipsFilterFromPublicFilter(req.RelationshipFilter)

			iter, err := rwt.QueryRelationships(ctx, filter, options.WithLimit(&limitPlusOne))
			if err != nil {
				return ps.rewriteError(ctx, err)
			}
			defer iter.Close()

			for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
				if iter.Err() != nil {
					return ps.rewriteError(ctx, err)
				}

				if len(deleteMutations) == int(limit) {
					deletionProgress = v1.DeleteRelationshipsResponse_DELETION_PROGRESS_PARTIAL
					if !req.OptionalAllowPartialDeletions {
						return ps.rewriteError(ctx, NewCouldNotTransactionallyDeleteErr(req.RelationshipFilter, req.OptionalLimit))
					}

					break
				}

				deleteMutations = append(deleteMutations, tuple.Delete(tpl))
			}
			iter.Close()
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			// One request per precondition and one request for the actual delete.
			DispatchCount: uint32(len(req.OptionalPreconditions)) + 1,
		})

		if err := checkPreconditions(ctx, rwt, req.OptionalPreconditions); err != nil {
			return err
		}

		if len(deleteMutations) > 0 {
			return rwt.WriteRelationships(ctx, deleteMutations)
		}

		return rwt.DeleteRelationships(ctx, req.RelationshipFilter)
	})
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	return &v1.DeleteRelationshipsResponse{
		DeletedAt:        zedtoken.MustNewFromRevision(revision),
		DeletionProgress: deletionProgress,
	}, nil
}
