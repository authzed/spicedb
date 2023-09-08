package v1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/internal/middleware"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/streamtimeout"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/relationships"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/services/v1/options"
	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	implv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	defaultExportBatchSizeFallback   = 1_000
	maxExportBatchSizeFallback       = 1_000
	streamReadTimeoutFallbackSeconds = 600
)

// NewExperimentalServer creates a ExperimentalServiceServer instance.
func NewExperimentalServer(dispatch dispatch.Dispatcher, permServerConfig PermissionsServerConfig, opts ...options.ExperimentalServerOptionsOption) v1.ExperimentalServiceServer {
	config := options.NewExperimentalServerOptionsWithOptionsAndDefaults(opts...)

	if config.DefaultExportBatchSize == 0 {
		log.
			Warn().
			Uint32("specified", config.DefaultExportBatchSize).
			Uint32("fallback", defaultExportBatchSizeFallback).
			Msg("experimental server config specified invalid DefaultExportBatchSize, setting to fallback")
		config.DefaultExportBatchSize = defaultExportBatchSizeFallback
	}
	if config.MaxExportBatchSize == 0 {
		log.
			Warn().
			Uint32("specified", config.MaxExportBatchSize).
			Uint32("fallback", maxExportBatchSizeFallback).
			Msg("experimental server config specified invalid MaxExportBatchSize, setting to fallback")
		config.MaxExportBatchSize = maxExportBatchSizeFallback
	}
	if config.StreamReadTimeout == 0 {
		log.
			Warn().
			Stringer("specified", config.StreamReadTimeout).
			Stringer("fallback", streamReadTimeoutFallbackSeconds*time.Second).
			Msg("experimental server config specified invalid StreamReadTimeout, setting to fallback")
		config.StreamReadTimeout = streamReadTimeoutFallbackSeconds * time.Second
	}

	return &experimentalServer{
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
				streamtimeout.MustStreamServerInterceptor(config.StreamReadTimeout),
			),
		},
		defaultBatchSize:        uint64(config.DefaultExportBatchSize),
		maxBatchSize:            uint64(config.MaxExportBatchSize),
		dispatch:                dispatch,
		maximumAPIDepth:         permServerConfig.MaximumAPIDepth,
		maxCaveatContextSize:    permServerConfig.MaxCaveatContextSize,
		bulkCheckMaxConcurrency: config.BulkCheckMaxConcurrency,
	}
}

type experimentalServer struct {
	v1.UnimplementedExperimentalServiceServer
	shared.WithServiceSpecificInterceptors

	defaultBatchSize uint64
	maxBatchSize     uint64

	// PermissionServer config specific
	dispatch                dispatch.Dispatcher
	maximumAPIDepth         uint32
	maxCaveatContextSize    int
	bulkCheckMaxConcurrency uint16
}

type bulkLoadAdapter struct {
	stream                 v1.ExperimentalService_BulkImportRelationshipsServer
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
	tuple.CopyRelationshipToRelationTuple[
		*v1.ObjectReference,
		*v1.SubjectReference,
		*v1.ContextualizedCaveat,
	](a.currentBatch[a.numSent], &a.current)

	if err := relationships.ValidateOneRelationship(
		a.referencedNamespaceMap,
		a.referencedCaveatMap,
		&a.current,
		relationships.ValidateRelationshipForCreateOrTouch,
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

func (es *experimentalServer) rewriteError(ctx context.Context, err error) error {
	return shared.RewriteError(ctx, err, nil)
}

func (es *experimentalServer) BulkImportRelationships(stream v1.ExperimentalService_BulkImportRelationshipsServer) error {
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
	}, dsoptions.WithDisableRetries(true)); err != nil {
		return es.rewriteError(stream.Context(), err)
	}

	usagemetrics.SetInContext(stream.Context(), &dispatchv1.ResponseMeta{
		// One request for the whole load
		DispatchCount: 1,
	})

	return stream.SendAndClose(&v1.BulkImportRelationshipsResponse{
		NumLoaded: numWritten,
	})
}

func (es *experimentalServer) BulkExportRelationships(
	req *v1.BulkExportRelationshipsRequest,
	resp v1.ExperimentalService_BulkExportRelationshipsServer,
) error {
	ctx := resp.Context()
	ds := datastoremw.MustFromContext(ctx)

	var atRevision datastore.Revision
	var cur dsoptions.Cursor

	if req.OptionalCursor != nil {
		var err error
		atRevision, cur, err = decodeCursor(ds, req.OptionalCursor)
		if err != nil {
			return es.rewriteError(ctx, err)
		}
	} else {
		var err error
		atRevision, _, err = consistency.RevisionFromContext(ctx)
		if err != nil {
			return es.rewriteError(ctx, err)
		}
	}

	reader := ds.SnapshotReader(atRevision)

	namespaces, err := reader.ListAllNamespaces(ctx)
	if err != nil {
		return es.rewriteError(ctx, err)
	}

	// Make sure the namespaces are always in a stable order
	slices.SortFunc(namespaces, func(
		lhs datastore.RevisionedDefinition[*core.NamespaceDefinition],
		rhs datastore.RevisionedDefinition[*core.NamespaceDefinition],
	) int {
		return strings.Compare(lhs.Definition.Name, rhs.Definition.Name)
	})

	// Skip the namespaces that are already fully returned
	for cur != nil && len(namespaces) > 0 && namespaces[0].Definition.Name < cur.ResourceAndRelation.Namespace {
		namespaces = namespaces[1:]
	}

	limit := es.defaultBatchSize
	if req.OptionalLimit > 0 {
		limit = uint64(req.OptionalLimit)
	}

	if limit > es.maxBatchSize {
		limit = es.maxBatchSize
	}

	// Pre-allocate all of the relationships that we might need in order to
	// make export easier and faster for the garbage collector.
	relsArray := make([]v1.Relationship, limit)
	objArray := make([]v1.ObjectReference, limit)
	subArray := make([]v1.SubjectReference, limit)
	subObjArray := make([]v1.ObjectReference, limit)
	caveatArray := make([]v1.ContextualizedCaveat, limit)
	for i := range relsArray {
		relsArray[i].Resource = &objArray[i]
		relsArray[i].Subject = &subArray[i]
		relsArray[i].Subject.Object = &subObjArray[i]
	}

	emptyRels := make([]*v1.Relationship, limit)

	for _, ns := range namespaces {
		rels := emptyRels

		// We want to keep iterating as long as we're sending full batches.
		// To bootstrap this loop, we enter the first time with a full rels
		// slice of dummy rels that were never sent.
		for uint64(len(rels)) == limit {
			// Lop off any rels we've already sent
			rels = rels[:0]

			tplFn := func(tpl *core.RelationTuple) {
				offset := len(rels)
				rels = append(rels, &relsArray[offset]) // nozero
				tuple.CopyRelationTupleToRelationship(tpl, &relsArray[offset], &caveatArray[offset])
			}

			cur, err = queryForEach(
				ctx,
				reader,
				datastore.RelationshipsFilter{ResourceType: ns.Definition.Name},
				tplFn,
				dsoptions.WithLimit(&limit),
				dsoptions.WithAfter(cur),
				dsoptions.WithSort(dsoptions.ByResource),
			)
			if err != nil {
				return es.rewriteError(ctx, err)
			}

			if len(rels) == 0 {
				continue
			}

			encoded, err := cursor.Encode(&implv1.DecodedCursor{
				VersionOneof: &implv1.DecodedCursor_V1{
					V1: &implv1.V1Cursor{
						Revision: atRevision.String(),
						Sections: []string{
							tuple.MustString(cur),
						},
					},
				},
			})
			if err != nil {
				return es.rewriteError(ctx, err)
			}

			if err := resp.Send(&v1.BulkExportRelationshipsResponse{
				AfterResultCursor: encoded,
				Relationships:     rels,
			}); err != nil {
				return es.rewriteError(ctx, err)
			}
		}

		// Datastore namespace order might not be exactly the same as go namespace order
		// so we shouldn't assume cursors are valid across namespaces
		cur = nil
	}

	return nil
}

func (es *experimentalServer) BulkCheckPermission(ctx context.Context, req *v1.BulkCheckPermissionRequest) (*v1.BulkCheckPermissionResponse, error) {
	atRevision, checkedAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, es.rewriteError(ctx, err)
	}

	// Identify checks with same permission+subject over different resources and group them. This is doable because
	// the dispatching system already internally supports this kind of batching for performance.
	groupedItems, err := groupItems(ctx, groupingParameters{
		atRevision:           atRevision,
		maxCaveatContextSize: es.maxCaveatContextSize,
		maximumAPIDepth:      es.maximumAPIDepth,
	}, req.Items)
	if err != nil {
		return nil, es.rewriteError(ctx, err)
	}

	bulkResponseMutex := sync.Mutex{}
	resp := &v1.BulkCheckPermissionResponse{CheckedAt: checkedAt}
	tr := taskrunner.NewPreloadedTaskRunner(ctx, es.bulkCheckMaxConcurrency, len(groupedItems))

	respMetadata := &dispatchv1.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 0,
		DepthRequired:       1,
		DebugInfo:           nil,
	}
	usagemetrics.SetInContext(ctx, respMetadata)

	appendResultsForError := func(params computed.CheckParameters, resourceIDs []string, err error) error {
		rewritten := es.rewriteError(ctx, err)
		statusResp, ok := status.FromError(rewritten)
		if !ok {
			// If error is not a gRPC Status, fail the entire bulk check request.
			return err
		}

		bulkResponseMutex.Lock()
		defer bulkResponseMutex.Unlock()

		for _, resourceID := range resourceIDs {
			reqItem, err := requestItemFromResourceAndParameters(params, resourceID)
			if err != nil {
				return es.rewriteError(ctx, err)
			}

			resp.Pairs = append(resp.Pairs, &v1.BulkCheckPermissionPair{
				Request: reqItem,
				Response: &v1.BulkCheckPermissionPair_Error{
					Error: statusResp.Proto(),
				},
			})
		}

		return nil
	}

	appendResultsForCheck := func(params computed.CheckParameters, resourceIDs []string, metadata *dispatchv1.ResponseMeta, results map[string]*dispatchv1.ResourceCheckResult) error {
		bulkResponseMutex.Lock()
		defer bulkResponseMutex.Unlock()

		for _, resourceID := range resourceIDs {
			reqItem, err := requestItemFromResourceAndParameters(params, resourceID)
			if err != nil {
				return es.rewriteError(ctx, err)
			}

			resp.Pairs = append(resp.Pairs, &v1.BulkCheckPermissionPair{
				Request:  reqItem,
				Response: pairItemFromCheckResult(results[resourceID]),
			})
		}

		respMetadata.DispatchCount += metadata.DispatchCount
		respMetadata.CachedDispatchCount += metadata.CachedDispatchCount
		return nil
	}

	for _, group := range groupedItems {
		group := group

		slicez.ForEachChunk(group.resourceIDs, MaxBulkCheckDispatchChunkSize, func(resourceIDs []string) {
			tr.Add(func(ctx context.Context) error {
				ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

				// Ensure the check namespaces and relations are valid.
				err := namespace.CheckNamespaceAndRelations(ctx,
					[]namespace.TypeAndRelationToCheck{
						{
							NamespaceName: group.params.ResourceType.Namespace,
							RelationName:  group.params.ResourceType.Relation,
							AllowEllipsis: false,
						},
						{
							NamespaceName: group.params.Subject.Namespace,
							RelationName:  stringz.DefaultEmpty(group.params.Subject.Relation, graph.Ellipsis),
							AllowEllipsis: true,
						},
					}, ds)
				if err != nil {
					return appendResultsForError(group.params, resourceIDs, err)
				}

				// Call bulk check to compute the check result(s) for the resource ID(s).
				rcr, metadata, err := computed.ComputeBulkCheck(ctx, es.dispatch, group.params, resourceIDs)
				if err != nil {
					return appendResultsForError(group.params, resourceIDs, err)
				}

				return appendResultsForCheck(group.params, resourceIDs, metadata, rcr)
			})
		})
	}

	if err := tr.StartAndWait(); err != nil {
		return nil, es.rewriteError(ctx, err)
	}

	return resp, nil
}

func pairItemFromCheckResult(checkResult *dispatchv1.ResourceCheckResult) *v1.BulkCheckPermissionPair_Item {
	permissionship, partialCaveat := checkResultToAPITypes(checkResult)
	return &v1.BulkCheckPermissionPair_Item{
		Item: &v1.BulkCheckPermissionResponseItem{
			Permissionship:    permissionship,
			PartialCaveatInfo: partialCaveat,
		},
	}
}

func requestItemFromResourceAndParameters(params computed.CheckParameters, resourceID string) (*v1.BulkCheckPermissionRequestItem, error) {
	item := &v1.BulkCheckPermissionRequestItem{
		Resource: &v1.ObjectReference{
			ObjectType: params.ResourceType.Namespace,
			ObjectId:   resourceID,
		},
		Permission: params.ResourceType.Relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: params.Subject.Namespace,
				ObjectId:   params.Subject.ObjectId,
			},
			OptionalRelation: denormalizeSubjectRelation(params.Subject.Relation),
		},
	}
	if len(params.CaveatContext) > 0 {
		var err error
		item.Context, err = structpb.NewStruct(params.CaveatContext)
		if err != nil {
			return nil, fmt.Errorf("caveat context wasn't properly validated: %w", err)
		}
	}
	return item, nil
}

func queryForEach(
	ctx context.Context,
	reader datastore.Reader,
	filter datastore.RelationshipsFilter,
	fn func(tpl *core.RelationTuple),
	opts ...dsoptions.QueryOptionsOption,
) (*core.RelationTuple, error) {
	iter, err := reader.QueryRelationships(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var hadTuples bool
	for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
		fn(tpl)
		hadTuples = true
	}
	if iter.Err() != nil {
		return nil, err
	}

	var cur *core.RelationTuple
	if hadTuples {
		cur, err = iter.Cursor()
		iter.Close()
		if err != nil {
			return nil, err
		}
	}

	return cur, nil
}

func decodeCursor(ds datastore.Datastore, encoded *v1.Cursor) (datastore.Revision, *core.RelationTuple, error) {
	decoded, err := cursor.Decode(encoded)
	if err != nil {
		return datastore.NoRevision, nil, err
	}

	if decoded.GetV1() == nil {
		return datastore.NoRevision, nil, errors.New("malformed cursor: no V1 in OneOf")
	}

	if len(decoded.GetV1().Sections) != 1 {
		return datastore.NoRevision, nil, errors.New("malformed cursor: wrong number of components")
	}

	atRevision, err := ds.RevisionFromString(decoded.GetV1().Revision)
	if err != nil {
		return datastore.NoRevision, nil, err
	}

	cur := tuple.Parse(decoded.GetV1().GetSections()[0])
	if cur == nil {
		return datastore.NoRevision, nil, errors.New("malformed cursor: invalid encoded relation tuple")
	}

	return atRevision, cur, nil
}
