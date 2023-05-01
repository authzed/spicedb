package v1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/jzelinskie/stringz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/samber/lo"

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
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/util"
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
		return shared.RewriteError(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	if err := ps.checkFilterNamespaces(ctx, req.RelationshipFilter, ds); err != nil {
		return shared.RewriteError(ctx, err)
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
		return shared.RewriteError(ctx, err)
	}
	defer tupleIterator.Close()

	response := &v1.ReadRelationshipsResponse{
		ReadAt: revisionReadAt,
	}
	targetRel := tuple.NewRelationship()
	targetCaveat := &v1.ContextualizedCaveat{}
	for tpl := tupleIterator.Next(); tpl != nil; tpl = tupleIterator.Next() {
		if tupleIterator.Err() != nil {
			return shared.RewriteError(ctx, fmt.Errorf("error when reading tuples: %w", tupleIterator.Err()))
		}

		tuple.MustToRelationshipMutating(tpl, targetRel, targetCaveat)
		response.Relationship = targetRel
		err := resp.Send(response)
		if err != nil {
			return shared.RewriteError(ctx, fmt.Errorf("error when streaming tuple: %w", err))
		}
	}

	if tupleIterator.Err() != nil {
		return shared.RewriteError(ctx, fmt.Errorf("error when reading tuples: %w", tupleIterator.Err()))
	}

	tupleIterator.Close()
	return nil
}

func (ps *permissionServer) WriteRelationships(ctx context.Context, req *v1.WriteRelationshipsRequest) (*v1.WriteRelationshipsResponse, error) {
	ds := datastoremw.MustFromContext(ctx)

	// Ensure that the updates and preconditions are not over the configured limits.
	if len(req.Updates) > int(ps.config.MaxUpdatesPerWrite) {
		return nil, shared.RewriteError(
			ctx,
			NewExceedsMaximumUpdatesErr(uint16(len(req.Updates)), ps.config.MaxUpdatesPerWrite),
		)
	}

	if len(req.OptionalPreconditions) > int(ps.config.MaxPreconditionsCount) {
		return nil, shared.RewriteError(
			ctx,
			NewExceedsMaximumPreconditionsErr(uint16(len(req.OptionalPreconditions)), ps.config.MaxPreconditionsCount),
		)
	}

	// Check for duplicate updates and create the set of caveat names to load.
	updateRelationshipSet := util.NewSet[string]()
	for _, update := range req.Updates {
		tupleStr := tuple.StringRelationshipWithoutCaveat(update.Relationship)
		if !updateRelationshipSet.Add(tupleStr) {
			return nil, shared.RewriteError(
				ctx,
				NewDuplicateRelationshipErr(update),
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

		tuples := lo.Map(tupleUpdates, func(item *core.RelationTupleUpdate, _ int) *core.RelationTuple {
			return item.Tuple
		})

		// Validate the updates.
		err := relationships.ValidateRelationships(ctx, rwt, tuples)
		if err != nil {
			return shared.RewriteError(ctx, err)
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
		return nil, shared.RewriteError(ctx, err)
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

type bulkLoadAdapter struct {
	stream                 v1.PermissionsService_BulkLoadRelationshipsServer
	referencedNamespaceMap map[string]*namespace.TypeSystem
	referencedCaveatMap    map[string]*core.CaveatDefinition
	current                core.RelationTuple
	caveat                 core.ContextualizedCaveat

	awaitingNamespaces []string
	awaitingCaveats    []string

	currentBatch []*v1.Relationship
	numSent      int
	err          error
}

func (a *bulkLoadAdapter) Next(_ context.Context) (*core.RelationTuple, error) {
	for a.err == nil && a.numSent == len(a.currentBatch) {
		// Load a new batch
		batch, err := a.stream.Recv()
		if err != nil {
			a.err = err
			if errors.Is(a.err, io.EOF) {
				return nil, nil
			}
			return nil, a.err
		}

		a.currentBatch = batch.Relationships
		a.numSent = 0

		a.awaitingNamespaces, a.awaitingCaveats = extractBatchNewReferencedNamespacesAndCaveats(
			a.currentBatch,
			a.referencedNamespaceMap,
			a.referencedCaveatMap,
		)
	}

	if len(a.awaitingNamespaces) > 0 || len(a.awaitingCaveats) > 0 {
		// Shut down the stream to give our caller a chance to fill in this information
		return nil, nil
	}

	a.current.Caveat = &a.caveat
	tuple.CopyRelationshipToRelationTuple(a.currentBatch[a.numSent], &a.current)

	if err := relationships.ValidateOneRelationship(
		a.referencedNamespaceMap,
		a.referencedCaveatMap,
		&a.current,
	); err != nil {
		return nil, err
	}

	a.numSent++
	return &a.current, nil
}

func extractBatchNewReferencedNamespacesAndCaveats(
	batch []*v1.Relationship,
	existingNamespaces map[string]*namespace.TypeSystem,
	existingCaveats map[string]*core.CaveatDefinition,
) ([]string, []string) {
	newNamespaces := make(map[string]struct{})
	newCaveats := make(map[string]struct{})
	for _, rel := range batch {
		if _, ok := existingNamespaces[rel.Resource.ObjectType]; !ok {
			newNamespaces[rel.Resource.ObjectType] = struct{}{}
		}
		if _, ok := existingNamespaces[rel.Subject.Object.ObjectType]; !ok {
			newNamespaces[rel.Subject.Object.ObjectType] = struct{}{}
		}
		if rel.OptionalCaveat != nil {
			if _, ok := existingCaveats[rel.OptionalCaveat.CaveatName]; !ok {
				newCaveats[rel.OptionalCaveat.CaveatName] = struct{}{}
			}
		}
	}

	return lo.Keys(newNamespaces), lo.Keys(newCaveats)
}

func (ps *permissionServer) BulkLoadRelationships(stream v1.PermissionsService_BulkLoadRelationshipsServer) error {
	ds := datastoremw.MustFromContext(stream.Context())

	var numWritten uint64
	if _, err := ds.ReadWriteTx(stream.Context(), func(rwt datastore.ReadWriteTransaction) error {
		loadedNamespaces := make(map[string]*namespace.TypeSystem)
		loadedCaveats := make(map[string]*core.CaveatDefinition)

		adapter := &bulkLoadAdapter{
			stream:                 stream,
			referencedNamespaceMap: loadedNamespaces,
			referencedCaveatMap:    loadedCaveats,
			current: core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{},
				Subject:             &core.ObjectAndRelation{},
			},
			caveat: core.ContextualizedCaveat{},
		}

		var streamWritten uint64
		var err error
		for ; adapter.err == nil && err == nil; streamWritten, err = rwt.BulkLoad(stream.Context(), adapter) {
			numWritten += streamWritten

			// The stream has terminated because we're awaiting namespace and caveat information
			if len(adapter.awaitingNamespaces) > 0 {
				nsDefs, err := rwt.LookupNamespacesWithNames(stream.Context(), adapter.awaitingNamespaces)
				if err != nil {
					return err
				}

				for _, nsDef := range nsDefs {
					nts, err := namespace.NewNamespaceTypeSystem(nsDef.Definition, namespace.ResolverForDatastoreReader(rwt))
					if err != nil {
						return err
					}

					loadedNamespaces[nsDef.Definition.Name] = nts
				}
				adapter.awaitingNamespaces = nil
			}

			if len(adapter.awaitingCaveats) > 0 {
				caveats, err := rwt.LookupCaveatsWithNames(stream.Context(), adapter.awaitingCaveats)
				if err != nil {
					return err
				}

				for _, caveat := range caveats {
					loadedCaveats[caveat.Definition.Name] = caveat.Definition
				}
				adapter.awaitingCaveats = nil
			}
		}
		numWritten += streamWritten

		return err
	}); err != nil {
		return rewriteError(stream.Context(), err)
	}

	usagemetrics.SetInContext(stream.Context(), &dispatchv1.ResponseMeta{
		// One request for the whole load
		DispatchCount: 1,
	})

	return stream.SendAndClose(&v1.BulkLoadRelationshipsResponse{
		NumLoaded: numWritten,
	})
}

func (ps *permissionServer) DeleteRelationships(ctx context.Context, req *v1.DeleteRelationshipsRequest) (*v1.DeleteRelationshipsResponse, error) {
	if len(req.OptionalPreconditions) > int(ps.config.MaxPreconditionsCount) {
		return nil, shared.RewriteError(
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
		return nil, shared.RewriteError(ctx, err)
	}

	return &v1.DeleteRelationshipsResponse{
		DeletedAt: zedtoken.MustNewFromRevision(revision),
	}, nil
}
