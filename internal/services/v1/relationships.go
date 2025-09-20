package v1

import (
	"cmp"
	"context"
	"fmt"
	"time"

	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/middleware"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/perfinsights"
	"github.com/authzed/spicedb/internal/middleware/streamtimeout"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/relationships"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/telemetry/otelconv"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/pagination"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
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

const MaximumTransactionMetadataSize = 65000 // bytes. Limited by the BLOB size used in MySQL driver

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

	// ExpiringRelationshipsEnabled defines whether or not expiring relationships are enabled.
	ExpiringRelationshipsEnabled bool

	// CaveatTypeSet is the set of caveat types to use for caveats. If not specified,
	// the default type set is used.
	CaveatTypeSet *caveattypes.TypeSet

	// PerformanceInsightMetricsEnabled defines whether or not performance insight metrics are enabled.
	PerformanceInsightMetricsEnabled bool

	// EnableExperimentalLookupResources3 is used to enable LookupResources v3 for testing.
	EnableExperimentalLookupResources3 bool // TODO: remove when LookupResources v3 is fully enabled
}

// NewPermissionsServer creates a PermissionsServiceServer instance.
func NewPermissionsServer(
	dispatch dispatch.Dispatcher,
	config PermissionsServerConfig,
) v1.PermissionsServiceServer {
	configWithDefaults := PermissionsServerConfig{
		MaxPreconditionsCount:              defaultIfZero(config.MaxPreconditionsCount, 1000),
		MaxUpdatesPerWrite:                 defaultIfZero(config.MaxUpdatesPerWrite, 1000),
		MaximumAPIDepth:                    defaultIfZero(config.MaximumAPIDepth, 50),
		StreamingAPITimeout:                defaultIfZero(config.StreamingAPITimeout, 30*time.Second),
		MaxCaveatContextSize:               defaultIfZero(config.MaxCaveatContextSize, 4096),
		MaxRelationshipContextSize:         defaultIfZero(config.MaxRelationshipContextSize, 25_000),
		MaxDatastoreReadPageSize:           defaultIfZero(config.MaxDatastoreReadPageSize, 1_000),
		MaxReadRelationshipsLimit:          defaultIfZero(config.MaxReadRelationshipsLimit, 1_000),
		MaxDeleteRelationshipsLimit:        defaultIfZero(config.MaxDeleteRelationshipsLimit, 1_000),
		MaxLookupResourcesLimit:            defaultIfZero(config.MaxLookupResourcesLimit, 1_000),
		MaxBulkExportRelationshipsLimit:    defaultIfZero(config.MaxBulkExportRelationshipsLimit, 100_000),
		DispatchChunkSize:                  defaultIfZero(config.DispatchChunkSize, 100),
		MaxCheckBulkConcurrency:            defaultIfZero(config.MaxCheckBulkConcurrency, 50),
		CaveatTypeSet:                      caveattypes.TypeSetOrDefault(config.CaveatTypeSet),
		ExpiringRelationshipsEnabled:       config.ExpiringRelationshipsEnabled,
		PerformanceInsightMetricsEnabled:   config.PerformanceInsightMetricsEnabled,
		EnableExperimentalLookupResources3: config.EnableExperimentalLookupResources3,
	}

	return &permissionServer{
		dispatch: dispatch,
		config:   configWithDefaults,
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary: middleware.ChainUnaryServer(
				grpcvalidate.UnaryServerInterceptor(),
				handwrittenvalidation.UnaryServerInterceptor,
				usagemetrics.UnaryServerInterceptor(),
				perfinsights.UnaryServerInterceptor(configWithDefaults.PerformanceInsightMetricsEnabled),
			),
			Stream: middleware.ChainStreamServer(
				grpcvalidate.StreamServerInterceptor(),
				handwrittenvalidation.StreamServerInterceptor,
				usagemetrics.StreamServerInterceptor(),
				streamtimeout.MustStreamServerInterceptor(configWithDefaults.StreamingAPITimeout),
				perfinsights.StreamServerInterceptor(configWithDefaults.PerformanceInsightMetricsEnabled),
			),
		},
		bulkChecker: &bulkChecker{
			maxAPIDepth:          configWithDefaults.MaximumAPIDepth,
			maxCaveatContextSize: configWithDefaults.MaxCaveatContextSize,
			maxConcurrency:       configWithDefaults.MaxCheckBulkConcurrency,
			dispatch:             dispatch,
			dispatchChunkSize:    configWithDefaults.DispatchChunkSize,
			caveatTypeSet:        configWithDefaults.CaveatTypeSet,
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
	perfinsights.SetInContext(resp.Context(), func() perfinsights.APIShapeLabels {
		return labelsForFilter(req.RelationshipFilter)
	})

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

	limit := uint64(0)
	var startCursor options.Cursor

	rrRequestHash, err := computeReadRelationshipsRequestHash(req)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	if req.OptionalCursor != nil {
		decodedCursor, _, err := cursor.DecodeToDispatchCursor(req.OptionalCursor, rrRequestHash)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		if len(decodedCursor.Sections) != 1 {
			return ps.rewriteError(ctx, NewInvalidCursorErr("did not find expected resume relationship"))
		}

		parsed, err := tuple.Parse(decodedCursor.Sections[0])
		if err != nil {
			return ps.rewriteError(ctx, NewInvalidCursorErr("could not parse resume relationship"))
		}

		startCursor = options.ToCursor(parsed)
	}

	pageSize := ps.config.MaxDatastoreReadPageSize
	if req.OptionalLimit > 0 {
		limit = uint64(req.OptionalLimit)
		if limit < pageSize {
			pageSize = limit
		}
	}

	dsFilter, err := datastore.RelationshipsFilterFromPublicFilter(req.RelationshipFilter)
	if err != nil {
		return ps.rewriteError(ctx, fmt.Errorf("error filtering: %w", err))
	}

	it, err := pagination.NewPaginatedIterator(
		ctx,
		ds,
		dsFilter,
		pageSize,
		options.ByResource,
		startCursor,
		queryshape.Varying,
	)
	if err != nil {
		return ps.rewriteError(ctx, err)
	}

	response := &v1.ReadRelationshipsResponse{
		ReadAt: revisionReadAt,
		Relationship: &v1.Relationship{
			Resource: &v1.ObjectReference{},
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{},
			},
		},
	}

	dispatchCursor := &dispatchv1.Cursor{
		DispatchVersion: 1,
		Sections:        []string{""},
	}

	var returnedCount uint64
	for rel, err := range it {
		if err != nil {
			return ps.rewriteError(ctx, fmt.Errorf("error when reading tuples: %w", err))
		}

		if limit > 0 && returnedCount >= limit {
			break
		}

		dispatchCursor.Sections[0] = tuple.StringWithoutCaveatOrExpiration(rel)
		encodedCursor, err := cursor.EncodeFromDispatchCursor(dispatchCursor, rrRequestHash, atRevision, nil)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		tuple.CopyToV1Relationship(rel, response.Relationship)
		response.AfterResultCursor = encodedCursor

		err = resp.Send(response)
		if err != nil {
			return ps.rewriteError(ctx, fmt.Errorf("error when streaming tuple: %w", err))
		}
		returnedCount++
	}
	return nil
}

func (ps *permissionServer) WriteRelationships(ctx context.Context, req *v1.WriteRelationshipsRequest) (*v1.WriteRelationshipsResponse, error) {
	perfinsights.SetInContext(ctx, perfinsights.NoLabels)

	if err := ps.validateTransactionMetadata(req.OptionalTransactionMetadata); err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx)

	span := trace.SpanFromContext(ctx)
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
		// TODO(jschorr): Change to struct-based keys.
		tupleStr := tuple.V1StringRelationshipWithoutCaveatOrExpiration(update.Relationship)
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

		if !ps.config.ExpiringRelationshipsEnabled && update.Relationship.OptionalExpiresAt != nil {
			return nil, ps.rewriteError(
				ctx,
				fmt.Errorf("support for expiring relationships is not enabled"),
			)
		}
	}
	span.AddEvent(otelconv.EventRelationshipsMutationsValidated)

	// Execute the write operation(s).
	relUpdates, err := tuple.UpdatesFromV1RelationshipUpdates(req.Updates)
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// Validate the preconditions.
		for _, precond := range req.OptionalPreconditions {
			if err := validatePrecondition(ctx, precond, rwt); err != nil {
				return err
			}
		}
		span.AddEvent(otelconv.EventRelationshipsPreconditionsValidated)

		// Validate the updates.
		err := relationships.ValidateRelationshipUpdates(ctx, rwt, ps.config.CaveatTypeSet, relUpdates)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}
		span.AddEvent(otelconv.EventRelationshipsUpdatesValidated)

		dispatchCount, err := genutil.EnsureUInt32(len(req.OptionalPreconditions) + 1)
		if err != nil {
			return ps.rewriteError(ctx, err)
		}

		usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
			// One request per precondition and one request for the actual writes.
			DispatchCount: dispatchCount,
		})

		span.AddEvent(otelconv.EventRelationshipsPreconditionsValidated)
		if err := checkPreconditions(ctx, rwt, req.OptionalPreconditions); err != nil {
			return err
		}

		errWrite := rwt.WriteRelationships(ctx, relUpdates)
		span.AddEvent(otelconv.EventRelationshipsWritten)
		return errWrite
	}, options.WithMetadata(req.OptionalTransactionMetadata))
	span.AddEvent(otelconv.EventRelationshipsReadWriteExecuted)
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

func (ps *permissionServer) validateTransactionMetadata(metadata *structpb.Struct) error {
	if metadata == nil {
		return nil
	}

	b, err := metadata.MarshalJSON()
	if err != nil {
		return err
	}

	if len(b) > MaximumTransactionMetadataSize {
		return NewTransactionMetadataTooLargeErr(len(b), MaximumTransactionMetadataSize)
	}

	return nil
}

func (ps *permissionServer) DeleteRelationships(ctx context.Context, req *v1.DeleteRelationshipsRequest) (*v1.DeleteRelationshipsResponse, error) {
	perfinsights.SetInContext(ctx, func() perfinsights.APIShapeLabels {
		return labelsForFilter(req.RelationshipFilter)
	})

	if err := ps.validateTransactionMetadata(req.OptionalTransactionMetadata); err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

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

	var deletedRelationshipCount uint64
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

			it, err := rwt.QueryRelationships(ctx, filter, options.WithLimit(&limitPlusOne), options.WithQueryShape(queryshape.Varying))
			if err != nil {
				return ps.rewriteError(ctx, err)
			}

			counter := uint64(0)
			for _, err := range it {
				if err != nil {
					return ps.rewriteError(ctx, err)
				}

				if counter == limit {
					return ps.rewriteError(ctx, NewCouldNotTransactionallyDeleteErr(req.RelationshipFilter, req.OptionalLimit))
				}

				counter++
			}
		}

		// Delete with the specified limit.
		if req.OptionalLimit > 0 {
			deleteLimit := uint64(req.OptionalLimit)
			drc, reachedLimit, err := rwt.DeleteRelationships(ctx, req.RelationshipFilter, options.WithDeleteLimit(&deleteLimit))
			if err != nil {
				return err
			}

			if reachedLimit {
				deletionProgress = v1.DeleteRelationshipsResponse_DELETION_PROGRESS_PARTIAL
			}

			deletedRelationshipCount = drc
			return nil
		}

		// Otherwise, kick off an unlimited deletion.
		deletedRelationshipCount, _, err = rwt.DeleteRelationships(ctx, req.RelationshipFilter)
		return err
	}, options.WithMetadata(req.OptionalTransactionMetadata))
	if err != nil {
		return nil, ps.rewriteError(ctx, err)
	}

	return &v1.DeleteRelationshipsResponse{
		DeletedAt:                 zedtoken.MustNewFromRevision(revision),
		DeletionProgress:          deletionProgress,
		RelationshipsDeletedCount: deletedRelationshipCount,
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

	relationToTest := cmp.Or(optionalRelation, datastore.Ellipsis)
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

	// Use counter-based validation for mutual exclusion of resource ID fields
	resourceIdFieldsCount := 0
	if filter.OptionalResourceId != "" {
		resourceIdFieldsCount++
	}
	if filter.OptionalResourceIdPrefix != "" {
		resourceIdFieldsCount++
	}
	if len(filter.OptionalResourceIds) > 0 {
		resourceIdFieldsCount++
	}

	// Ensure only one resource ID field type is set at a time
	if resourceIdFieldsCount > 1 {
		return NewInvalidFilterErr("only one of resource_id, resource_id_prefix, or resource_ids can be set at the same time", filter.String())
	}

	// Ensure that at least one field is set.
	return checkIfFilterIsEmpty(filter)
}

func checkIfFilterIsEmpty(filter *v1.RelationshipFilter) error {
	if filter.ResourceType == "" &&
		filter.OptionalResourceId == "" &&
		filter.OptionalResourceIdPrefix == "" &&
		len(filter.OptionalResourceIds) == 0 &&
		filter.OptionalRelation == "" &&
		filter.OptionalSubjectFilter == nil {
		return NewInvalidFilterErr("at least one field must be set", filter.String())
	}

	return nil
}

func labelsForFilter(filter *v1.RelationshipFilter) perfinsights.APIShapeLabels {
	if filter == nil {
		return perfinsights.NoLabels()
	}

	if filter.OptionalSubjectFilter == nil {
		return perfinsights.APIShapeLabels{
			perfinsights.ResourceTypeLabel:     filter.ResourceType,
			perfinsights.ResourceRelationLabel: filter.OptionalRelation,
		}
	}

	if filter.OptionalSubjectFilter.OptionalRelation == nil {
		return perfinsights.APIShapeLabels{
			perfinsights.ResourceTypeLabel:     filter.ResourceType,
			perfinsights.ResourceRelationLabel: filter.OptionalRelation,
			perfinsights.SubjectTypeLabel:      filter.OptionalSubjectFilter.SubjectType,
		}
	}

	return perfinsights.APIShapeLabels{
		perfinsights.ResourceTypeLabel:     filter.ResourceType,
		perfinsights.ResourceRelationLabel: filter.OptionalRelation,
		perfinsights.SubjectTypeLabel:      filter.OptionalSubjectFilter.SubjectType,
		perfinsights.SubjectRelationLabel:  filter.OptionalSubjectFilter.OptionalRelation.Relation,
	}
}
