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
	"go.opentelemetry.io/otel/trace"
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
	"github.com/authzed/spicedb/pkg/genutil"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
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

	// DispatchChunkSize is the maximum number of elements to dispach in a dispatch call
	DispatchChunkSize uint16

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

	// MaxCheckBulkConcurrency defines the maximum number of concurrent checks that can be
	// made in a single CheckBulkPermissions call.
	MaxCheckBulkConcurrency uint16

	// MaxReadRelationshipsLimit defines the maximum number of relationships that can be read
	// in a single ReadRelationships call.
	MaxReadRelationshipsLimit uint32

	// MaxDeleteRelationshipsLimit defines the maximum number of relationships that can be deleted
	// in a single DeleteRelationships call.
	MaxDeleteRelationshipsLimit uint32

	// MaxLookupResourcesLimit defines the maximum number of resources that can be looked up in a
	// single LookupResources call.
	MaxLookupResourcesLimit uint32

	// MaxBulkExportRelationshipsLimit defines the maximum number of relationships that can be
	// exported in a single BulkExportRelationships call.
	MaxBulkExportRelationshipsLimit uint32

	// UseExperimentalLookupResources2 enables the experimental LookupResources2 API.
	UseExperimentalLookupResources2 bool
}

// NewPermissionsServer creates a PermissionsServiceServer instance.
func NewPermissionsServer(
	dispatch dispatch.Dispatcher,
	config PermissionsServerConfig,
) v1.PermissionsServiceServer {
	configWithDefaults := PermissionsServerConfig{
		MaxPreconditionsCount:           defaultIfZero(config.MaxPreconditionsCount, 1000),
		MaxUpdatesPerWrite:              defaultIfZero(config.MaxUpdatesPerWrite, 1000),
		MaximumAPIDepth:                 defaultIfZero(config.MaximumAPIDepth, 50),
		StreamingAPITimeout:             defaultIfZero(config.StreamingAPITimeout, 30*time.Second),
		MaxCaveatContextSize:            defaultIfZero(config.MaxCaveatContextSize, 4096),
		MaxRelationshipContextSize:      defaultIfZero(config.MaxRelationshipContextSize, 25_000),
		MaxDatastoreReadPageSize:        defaultIfZero(config.MaxDatastoreReadPageSize, 1_000),
		MaxReadRelationshipsLimit:       defaultIfZero(config.MaxReadRelationshipsLimit, 1_000),
		MaxDeleteRelationshipsLimit:     defaultIfZero(config.MaxDeleteRelationshipsLimit, 1_000),
		MaxLookupResourcesLimit:         defaultIfZero(config.MaxLookupResourcesLimit, 1_000),
		MaxBulkExportRelationshipsLimit: defaultIfZero(config.MaxBulkExportRelationshipsLimit, 100_000),
		UseExperimentalLookupResources2: config.UseExperimentalLookupResources2,
		DispatchChunkSize:               defaultIfZero(config.DispatchChunkSize, 100),
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
		bulkChecker: &bulkChecker{
			maxAPIDepth:          configWithDefaults.MaximumAPIDepth,
			maxCaveatContextSize: configWithDefaults.MaxCaveatContextSize,
			maxConcurrency:       configWithDefaults.MaxCheckBulkConcurrency,
			dispatch:             dispatch,
			dispatchChunkSize:    configWithDefaults.DispatchChunkSize,
		},
	}
}

type permissionServer struct {
	v1.UnimplementedPermissionsServiceServer
	shared.WithServiceSpecificInterceptors

	dispatch dispatch.Dispatcher
	config   PermissionsServerConfig

	bulkChecker *bulkChecker
}

func (ps *permissionServer) ReadRelationships(req *v1.ReadRelationshipsRequest, resp v1.PermissionsService_ReadRelationshipsServer) error {
	if req.OptionalLimit > 0 && req.OptionalLimit > ps.config.MaxReadRelationshipsLimit {
		return ps.rewriteError(resp.Context(), NewExceedsMaximumLimitErr(uint64(req.OptionalLimit), uint64(ps.config.MaxReadRelationshipsLimit)))
	}

	ctx := resp.Context()
	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	if err := validateRelationshipsFilter(ctx, req.RelationshipFilter, ds); err != nil {
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

	dsFilter, err := datastore.RelationshipsFilterFromPublicFilter(req.RelationshipFilter)
	if err != nil {
		return ps.rewriteError(ctx, fmt.Errorf("error filtering: %w", err))
	}

	tupleIterator, err := pagination.NewPaginatedIterator(
		ctx,
		ds,
		dsFilter,
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

	span := trace.SpanFromContext(ctx)
	span.AddEvent("validating mutations")
	// Ensure that the updates and preconditions are not over the configured limits.
	if len(req.Updates) > int(ps.config.MaxUpdatesPerWrite) {
		return nil, ps.rewriteError(
			ctx,
			NewExceedsMaximumUpdatesErr(uint64(len(req.Updates)), uint64(ps.config.MaxUpdatesPerWrite)),
		)
	}

	if len(req.OptionalPreconditions) > int(ps.config.MaxPreconditionsCount) {
		return nil, ps.rewriteError(
			ctx,
			NewExceedsMaximumPreconditionsErr(uint64(len(req.OptionalPreconditions)), uint64(ps.config.MaxPreconditionsCount)),
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
	span.AddEvent("read write transaction")
	tupleUpdates := tuple.UpdateFromRelationshipUpdates(req.Updates)
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		span.AddEvent("preconditions")

		// Validate the preconditions.
		for _, precond := range req.OptionalPreconditions {
			if err := validatePrecondition(ctx, precond, rwt); err != nil {
				return err
			}
		}

		// Validate the updates.
		span.AddEvent("validate updates")
		err := relationships.ValidateRelationshipUpdates(ctx, rwt, tupleUpdates)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		dispatchCount, err := genutil.EnsureUInt32(len(req.OptionalPreconditions) + 1)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			// One request per precondition and one request for the actual writes.
			DispatchCount: dispatchCount,
		})

		span.AddEvent("preconditions")
		if err := checkPreconditions(ctx, rwt, req.OptionalPreconditions); err != nil {
			return err
		}

		span.AddEvent("write relationships")
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
			NewExceedsMaximumPreconditionsErr(uint64(len(req.OptionalPreconditions)), uint64(ps.config.MaxPreconditionsCount)),
		)
	}

	if req.OptionalLimit > 0 && req.OptionalLimit > ps.config.MaxDeleteRelationshipsLimit {
		return nil, ps.rewriteError(ctx, NewExceedsMaximumLimitErr(uint64(req.OptionalLimit), uint64(ps.config.MaxDeleteRelationshipsLimit)))
	}

	ds := datastoremw.MustFromContext(ctx)
	deletionProgress := v1.DeleteRelationshipsResponse_DELETION_PROGRESS_COMPLETE

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		if err := validateRelationshipsFilter(ctx, req.RelationshipFilter, rwt); err != nil {
			return err
		}

		dispatchCount, err := genutil.EnsureUInt32(len(req.OptionalPreconditions) + 1)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			// One request per precondition and one request for the actual delete.
			DispatchCount: dispatchCount,
		})

		for _, precond := range req.OptionalPreconditions {
			if err := validatePrecondition(ctx, precond, rwt); err != nil {
				return err
			}
		}

		if err := checkPreconditions(ctx, rwt, req.OptionalPreconditions); err != nil {
			return err
		}

		// If a limit was specified but partial deletion is not allowed, we need to check if the
		// number of relationships to be deleted exceeds the limit.
		if req.OptionalLimit > 0 && !req.OptionalAllowPartialDeletions {
			limit := uint64(req.OptionalLimit)
			limitPlusOne := limit + 1
			filter, err := datastore.RelationshipsFilterFromPublicFilter(req.RelationshipFilter)
			if err != nil {
				return ps.rewriteError(ctx, err)
			}

			iter, err := rwt.QueryRelationships(ctx, filter, options.WithLimit(&limitPlusOne))
			if err != nil {
				return ps.rewriteError(ctx, err)
			}
			defer iter.Close()

			counter := 0
			for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
				if iter.Err() != nil {
					return ps.rewriteError(ctx, err)
				}

				if counter == int(limit) {
					return ps.rewriteError(ctx, NewCouldNotTransactionallyDeleteErr(req.RelationshipFilter, req.OptionalLimit))
				}

				counter++
			}
			iter.Close()
		}

		// Delete with the specified limit.
		if req.OptionalLimit > 0 {
			deleteLimit := uint64(req.OptionalLimit)
			reachedLimit, err := rwt.DeleteRelationships(ctx, req.RelationshipFilter, options.WithDeleteLimit(&deleteLimit))
			if err != nil {
				return err
			}

			if reachedLimit {
				deletionProgress = v1.DeleteRelationshipsResponse_DELETION_PROGRESS_PARTIAL
			}

			return nil
		}

		// Otherwise, kick off an unlimited deletion.
		_, err = rwt.DeleteRelationships(ctx, req.RelationshipFilter)
		return err
	})
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	return &v1.DeleteRelationshipsResponse{
		DeletedAt:        zedtoken.MustNewFromRevision(revision),
		DeletionProgress: deletionProgress,
	}, nil
}

var emptyPrecondition = &v1.Precondition{}

func validatePrecondition(ctx context.Context, precond *v1.Precondition, reader datastore.Reader) error {
	if precond.EqualVT(emptyPrecondition) || precond.Filter == nil {
		return NewEmptyPreconditionErr()
	}

	return validateRelationshipsFilter(ctx, precond.Filter, reader)
}

func checkFilterComponent(ctx context.Context, objectType, optionalRelation string, ds datastore.Reader) error {
	if objectType == "" {
		return nil
	}

	relationToTest := stringz.DefaultEmpty(optionalRelation, datastore.Ellipsis)
	allowEllipsis := optionalRelation == ""
	return namespace.CheckNamespaceAndRelation(ctx, objectType, relationToTest, allowEllipsis, ds)
}

func validateRelationshipsFilter(ctx context.Context, filter *v1.RelationshipFilter, ds datastore.Reader) error {
	// ResourceType is optional, so only check the relation if it is specified.
	if filter.ResourceType != "" {
		if err := checkFilterComponent(ctx, filter.ResourceType, filter.OptionalRelation, ds); err != nil {
			return err
		}
	}

	// SubjectFilter is optional, so only check if it is specified.
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		subjectRelation := ""
		if subjectFilter.OptionalRelation != nil {
			subjectRelation = subjectFilter.OptionalRelation.Relation
		}
		if err := checkFilterComponent(ctx, subjectFilter.SubjectType, subjectRelation, ds); err != nil {
			return err
		}
	}

	// Ensure the resource ID and the resource ID prefix are not set at the same time.
	if filter.OptionalResourceId != "" && filter.OptionalResourceIdPrefix != "" {
		return NewInvalidFilterErr("resource_id and resource_id_prefix cannot be set at the same time", filter.String())
	}

	// Ensure that at least one field is set.
	return checkIfFilterIsEmpty(filter)
}

func checkIfFilterIsEmpty(filter *v1.RelationshipFilter) error {
	if filter.ResourceType == "" &&
		filter.OptionalResourceId == "" &&
		filter.OptionalResourceIdPrefix == "" &&
		filter.OptionalRelation == "" &&
		filter.OptionalSubjectFilter == nil {
		return NewInvalidFilterErr("at least one field must be set", filter.String())
	}

	return nil
}
