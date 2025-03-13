package v1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ccoveille/go-safecast"
	"github.com/jzelinskie/stringz"

	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/middleware"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/streamtimeout"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/relationships"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/services/v1/options"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	implv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/samber/lo"
)

const (
	defaultExportBatchSizeFallback   = 1_000
	maxExportBatchSizeFallback       = 10_000
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
		fallback := permServerConfig.MaxBulkExportRelationshipsLimit
		if fallback == 0 {
			fallback = maxExportBatchSizeFallback
		}

		log.
			Warn().
			Uint32("specified", config.MaxExportBatchSize).
			Uint32("fallback", fallback).
			Msg("experimental server config specified invalid MaxExportBatchSize, setting to fallback")
		config.MaxExportBatchSize = fallback
	}
	if config.StreamReadTimeout == 0 {
		log.
			Warn().
			Stringer("specified", config.StreamReadTimeout).
			Stringer("fallback", streamReadTimeoutFallbackSeconds*time.Second).
			Msg("experimental server config specified invalid StreamReadTimeout, setting to fallback")
		config.StreamReadTimeout = streamReadTimeoutFallbackSeconds * time.Second
	}

	chunkSize := permServerConfig.DispatchChunkSize
	if chunkSize == 0 {
		log.
			Warn().
			Msg("experimental server config specified invalid DispatchChunkSize, defaulting to 100")
		chunkSize = 100
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
		maxBatchSize: uint64(config.MaxExportBatchSize),
		bulkChecker: &bulkChecker{
			maxAPIDepth:          permServerConfig.MaximumAPIDepth,
			maxCaveatContextSize: permServerConfig.MaxCaveatContextSize,
			maxConcurrency:       config.BulkCheckMaxConcurrency,
			dispatch:             dispatch,
			dispatchChunkSize:    chunkSize,
		},
	}
}

type experimentalServer struct {
	v1.UnimplementedExperimentalServiceServer
	shared.WithServiceSpecificInterceptors

	maxBatchSize uint64

	bulkChecker *bulkChecker
}

type bulkLoadAdapter struct {
	stream                 v1.ExperimentalService_BulkImportRelationshipsServer
	referencedNamespaceMap map[string]*schema.Definition
	referencedCaveatMap    map[string]*core.CaveatDefinition
	current                tuple.Relationship
	caveat                 core.ContextualizedCaveat

	awaitingNamespaces []string
	awaitingCaveats    []string

	currentBatch []*v1.Relationship
	numSent      int
	err          error
}

func (a *bulkLoadAdapter) Next(_ context.Context) (*tuple.Relationship, error) {
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

	a.current.RelationshipReference.Resource.ObjectType = a.currentBatch[a.numSent].Resource.ObjectType
	a.current.RelationshipReference.Resource.ObjectID = a.currentBatch[a.numSent].Resource.ObjectId
	a.current.RelationshipReference.Resource.Relation = a.currentBatch[a.numSent].Relation
	a.current.Subject.ObjectType = a.currentBatch[a.numSent].Subject.Object.ObjectType
	a.current.Subject.ObjectID = a.currentBatch[a.numSent].Subject.Object.ObjectId
	a.current.Subject.Relation = stringz.DefaultEmpty(a.currentBatch[a.numSent].Subject.OptionalRelation, tuple.Ellipsis)

	if a.currentBatch[a.numSent].OptionalCaveat != nil {
		a.caveat.CaveatName = a.currentBatch[a.numSent].OptionalCaveat.CaveatName
		a.caveat.Context = a.currentBatch[a.numSent].OptionalCaveat.Context
		a.current.OptionalCaveat = &a.caveat
	} else {
		a.current.OptionalCaveat = nil
	}

	if a.currentBatch[a.numSent].OptionalExpiresAt != nil {
		t := a.currentBatch[a.numSent].OptionalExpiresAt.AsTime()
		a.current.OptionalExpiration = &t
	} else {
		a.current.OptionalExpiration = nil
	}

	a.current.OptionalIntegrity = nil

	if err := relationships.ValidateOneRelationship(
		a.referencedNamespaceMap,
		a.referencedCaveatMap,
		a.current,
		relationships.ValidateRelationshipForCreateOrTouch,
	); err != nil {
		return nil, err
	}

	a.numSent++
	return &a.current, nil
}

func extractBatchNewReferencedNamespacesAndCaveats(
	batch []*v1.Relationship,
	existingNamespaces map[string]*schema.Definition,
	existingCaveats map[string]*core.CaveatDefinition,
) ([]string, []string) {
	newNamespaces := make(map[string]struct{}, 2)
	newCaveats := make(map[string]struct{}, 0)
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

// TODO: this is now duplicate code with ImportBulkRelationships
func (es *experimentalServer) BulkImportRelationships(stream v1.ExperimentalService_BulkImportRelationshipsServer) error {
	ds := datastoremw.MustFromContext(stream.Context())

	var numWritten uint64
	if _, err := ds.ReadWriteTx(stream.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		loadedNamespaces := make(map[string]*schema.Definition, 2)
		loadedCaveats := make(map[string]*core.CaveatDefinition, 0)

		adapter := &bulkLoadAdapter{
			stream:                 stream,
			referencedNamespaceMap: loadedNamespaces,
			referencedCaveatMap:    loadedCaveats,
			current:                tuple.Relationship{},
			caveat:                 core.ContextualizedCaveat{},
		}
		resolver := schema.ResolverForDatastoreReader(rwt)
		ts := schema.NewTypeSystem(resolver)

		var streamWritten uint64
		var err error
		for ; adapter.err == nil && err == nil; streamWritten, err = rwt.BulkLoad(stream.Context(), adapter) {
			numWritten += streamWritten

			// The stream has terminated because we're awaiting namespace and/or caveat information
			if len(adapter.awaitingNamespaces) > 0 {
				nsDefs, err := rwt.LookupNamespacesWithNames(stream.Context(), adapter.awaitingNamespaces)
				if err != nil {
					return err
				}

				for _, nsDef := range nsDefs {
					newDef, err := schema.NewDefinition(ts, nsDef.Definition)
					if err != nil {
						return err
					}

					loadedNamespaces[nsDef.Definition.Name] = newDef
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
		return shared.RewriteErrorWithoutConfig(stream.Context(), err)
	}

	usagemetrics.SetInContext(stream.Context(), &dispatchv1.ResponseMeta{
		// One request for the whole load
		DispatchCount: 1,
	})

	return stream.SendAndClose(&v1.BulkImportRelationshipsResponse{
		NumLoaded: numWritten,
	})
}

// TODO: this is now duplicate code with ExportBulkRelationships
func (es *experimentalServer) BulkExportRelationships(
	req *v1.BulkExportRelationshipsRequest,
	resp grpc.ServerStreamingServer[v1.BulkExportRelationshipsResponse],
) error {
	ctx := resp.Context()
	atRevision, _, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return shared.RewriteErrorWithoutConfig(ctx, err)
	}

	return BulkExport(ctx, datastoremw.MustFromContext(ctx), es.maxBatchSize, req, atRevision, resp.Send)
}

// BulkExport implements the BulkExportRelationships API functionality. Given a datastore.Datastore, it will
// export stream via the sender all relationships matched by the incoming request.
// If no cursor is provided, it will fallback to the provided revision.
func BulkExport(ctx context.Context, ds datastore.ReadOnlyDatastore, batchSize uint64, req *v1.BulkExportRelationshipsRequest, fallbackRevision datastore.Revision, sender func(response *v1.BulkExportRelationshipsResponse) error) error {
	if req.OptionalLimit > 0 && uint64(req.OptionalLimit) > batchSize {
		return shared.RewriteErrorWithoutConfig(ctx, NewExceedsMaximumLimitErr(uint64(req.OptionalLimit), batchSize))
	}

	atRevision := fallbackRevision
	var curNamespace string
	var cur dsoptions.Cursor
	if req.OptionalCursor != nil {
		var err error
		atRevision, curNamespace, cur, err = decodeCursor(ds, req.OptionalCursor)
		if err != nil {
			return shared.RewriteErrorWithoutConfig(ctx, err)
		}
	}

	reader := ds.SnapshotReader(atRevision)

	namespaces, err := reader.ListAllNamespaces(ctx)
	if err != nil {
		return shared.RewriteErrorWithoutConfig(ctx, err)
	}

	// Make sure the namespaces are always in a stable order
	slices.SortFunc(namespaces, func(
		lhs datastore.RevisionedDefinition[*core.NamespaceDefinition],
		rhs datastore.RevisionedDefinition[*core.NamespaceDefinition],
	) int {
		return strings.Compare(lhs.Definition.Name, rhs.Definition.Name)
	})

	// Skip the namespaces that are already fully returned
	for cur != nil && len(namespaces) > 0 && namespaces[0].Definition.Name < curNamespace {
		namespaces = namespaces[1:]
	}

	limit := batchSize
	if req.OptionalLimit > 0 {
		limit = uint64(req.OptionalLimit)
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

		// Reset the cursor between namespaces.
		if ns.Definition.Name != curNamespace {
			cur = nil
		}

		// Skip this namespace if a resource type filter was specified.
		if req.OptionalRelationshipFilter != nil && req.OptionalRelationshipFilter.ResourceType != "" {
			if ns.Definition.Name != req.OptionalRelationshipFilter.ResourceType {
				continue
			}
		}

		// Setup the filter to use for the relationships.
		relationshipFilter := datastore.RelationshipsFilter{OptionalResourceType: ns.Definition.Name}
		if req.OptionalRelationshipFilter != nil {
			rf, err := datastore.RelationshipsFilterFromPublicFilter(req.OptionalRelationshipFilter)
			if err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}

			// Overload the namespace name with the one from the request, because each iteration is for a different namespace.
			rf.OptionalResourceType = ns.Definition.Name
			relationshipFilter = rf
		}

		// We want to keep iterating as long as we're sending full batches.
		// To bootstrap this loop, we enter the first time with a full rels
		// slice of dummy rels that were never sent.
		for uint64(len(rels)) == limit {
			// Lop off any rels we've already sent
			rels = rels[:0]

			relFn := func(rel tuple.Relationship) {
				offset := len(rels)
				rels = append(rels, &relsArray[offset]) // nozero

				v1Rel := &relsArray[offset]
				v1Rel.Resource.ObjectType = rel.RelationshipReference.Resource.ObjectType
				v1Rel.Resource.ObjectId = rel.RelationshipReference.Resource.ObjectID
				v1Rel.Relation = rel.RelationshipReference.Resource.Relation
				v1Rel.Subject.Object.ObjectType = rel.RelationshipReference.Subject.ObjectType
				v1Rel.Subject.Object.ObjectId = rel.RelationshipReference.Subject.ObjectID
				v1Rel.Subject.OptionalRelation = denormalizeSubjectRelation(rel.RelationshipReference.Subject.Relation)

				if rel.OptionalCaveat != nil {
					caveatArray[offset].CaveatName = rel.OptionalCaveat.CaveatName
					caveatArray[offset].Context = rel.OptionalCaveat.Context
					v1Rel.OptionalCaveat = &caveatArray[offset]
				} else {
					v1Rel.OptionalCaveat = nil
				}

				if rel.OptionalExpiration != nil {
					v1Rel.OptionalExpiresAt = timestamppb.New(*rel.OptionalExpiration)
				} else {
					v1Rel.OptionalExpiresAt = nil
				}
			}

			cur, err = queryForEach(
				ctx,
				reader,
				relationshipFilter,
				relFn,
				dsoptions.WithLimit(&limit),
				dsoptions.WithAfter(cur),
				dsoptions.WithSort(dsoptions.ByResource),
			)
			if err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}

			if len(rels) == 0 {
				continue
			}

			encoded, err := cursor.Encode(&implv1.DecodedCursor{
				VersionOneof: &implv1.DecodedCursor_V1{
					V1: &implv1.V1Cursor{
						Revision: atRevision.String(),
						Sections: []string{
							ns.Definition.Name,
							tuple.MustString(*dsoptions.ToRelationship(cur)),
						},
					},
				},
			})
			if err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}

			if err := sender(&v1.BulkExportRelationshipsResponse{
				AfterResultCursor: encoded,
				Relationships:     rels,
			}); err != nil {
				return shared.RewriteErrorWithoutConfig(ctx, err)
			}
		}
	}
	return nil
}

func (es *experimentalServer) BulkCheckPermission(ctx context.Context, req *v1.BulkCheckPermissionRequest) (*v1.BulkCheckPermissionResponse, error) {
	convertedReq := toCheckBulkPermissionsRequest(req)
	res, err := es.bulkChecker.checkBulkPermissions(ctx, convertedReq)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	return toBulkCheckPermissionResponse(res), nil
}

func (es *experimentalServer) ExperimentalReflectSchema(ctx context.Context, req *v1.ExperimentalReflectSchemaRequest) (*v1.ExperimentalReflectSchemaResponse, error) {
	// Get the current schema.
	schema, atRevision, err := loadCurrentSchema(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	filters, err := newexpSchemaFilters(req.OptionalFilters)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	definitions := make([]*v1.ExpDefinition, 0, len(schema.ObjectDefinitions))
	if filters.HasNamespaces() {
		for _, ns := range schema.ObjectDefinitions {
			def, err := expNamespaceAPIRepr(ns, filters)
			if err != nil {
				return nil, shared.RewriteErrorWithoutConfig(ctx, err)
			}

			if def != nil {
				definitions = append(definitions, def)
			}
		}
	}

	caveats := make([]*v1.ExpCaveat, 0, len(schema.CaveatDefinitions))
	if filters.HasCaveats() {
		for _, cd := range schema.CaveatDefinitions {
			caveat, err := expCaveatAPIRepr(cd, filters)
			if err != nil {
				return nil, shared.RewriteErrorWithoutConfig(ctx, err)
			}

			if caveat != nil {
				caveats = append(caveats, caveat)
			}
		}
	}

	return &v1.ExperimentalReflectSchemaResponse{
		Definitions: definitions,
		Caveats:     caveats,
		ReadAt:      zedtoken.MustNewFromRevision(atRevision),
	}, nil
}

func (es *experimentalServer) ExperimentalDiffSchema(ctx context.Context, req *v1.ExperimentalDiffSchemaRequest) (*v1.ExperimentalDiffSchemaResponse, error) {
	atRevision, _, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	diff, existingSchema, comparisonSchema, err := schemaDiff(ctx, req.ComparisonSchema)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	resp, err := expConvertDiff(diff, existingSchema, comparisonSchema, atRevision)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	return resp, nil
}

func (es *experimentalServer) ExperimentalComputablePermissions(ctx context.Context, req *v1.ExperimentalComputablePermissionsRequest) (*v1.ExperimentalComputablePermissionsResponse, error) {
	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)
	ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(ds))
	vdef, err := ts.GetValidatedDefinition(ctx, req.DefinitionName)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	relationName := req.RelationName
	if relationName == "" {
		relationName = tuple.Ellipsis
	} else {
		if _, ok := vdef.GetRelation(relationName); !ok {
			return nil, shared.RewriteErrorWithoutConfig(ctx, schema.NewRelationNotFoundErr(req.DefinitionName, relationName))
		}
	}

	allNamespaces, err := ds.ListAllNamespaces(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	allDefinitions := make([]*core.NamespaceDefinition, 0, len(allNamespaces))
	for _, ns := range allNamespaces {
		allDefinitions = append(allDefinitions, ns.Definition)
	}

	rg := vdef.Reachability()
	rr, err := rg.RelationsEncounteredForSubject(ctx, allDefinitions, &core.RelationReference{
		Namespace: req.DefinitionName,
		Relation:  relationName,
	})
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	relations := make([]*v1.ExpRelationReference, 0, len(rr))
	for _, r := range rr {
		if r.Namespace == req.DefinitionName && r.Relation == req.RelationName {
			continue
		}

		if req.OptionalDefinitionNameFilter != "" && !strings.HasPrefix(r.Namespace, req.OptionalDefinitionNameFilter) {
			continue
		}

		def, err := ts.GetValidatedDefinition(ctx, r.Namespace)
		if err != nil {
			return nil, shared.RewriteErrorWithoutConfig(ctx, err)
		}

		relations = append(relations, &v1.ExpRelationReference{
			DefinitionName: r.Namespace,
			RelationName:   r.Relation,
			IsPermission:   def.IsPermission(r.Relation),
		})
	}

	sort.Slice(relations, func(i, j int) bool {
		if relations[i].DefinitionName == relations[j].DefinitionName {
			return relations[i].RelationName < relations[j].RelationName
		}
		return relations[i].DefinitionName < relations[j].DefinitionName
	})

	return &v1.ExperimentalComputablePermissionsResponse{
		Permissions: relations,
		ReadAt:      revisionReadAt,
	}, nil
}

func (es *experimentalServer) ExperimentalDependentRelations(ctx context.Context, req *v1.ExperimentalDependentRelationsRequest) (*v1.ExperimentalDependentRelationsResponse, error) {
	atRevision, revisionReadAt, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)
	ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(ds))
	vdef, err := ts.GetValidatedDefinition(ctx, req.DefinitionName)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	_, ok := vdef.GetRelation(req.PermissionName)
	if !ok {
		return nil, shared.RewriteErrorWithoutConfig(ctx, schema.NewRelationNotFoundErr(req.DefinitionName, req.PermissionName))
	}

	if !vdef.IsPermission(req.PermissionName) {
		return nil, shared.RewriteErrorWithoutConfig(ctx, NewNotAPermissionError(req.PermissionName))
	}

	rg := vdef.Reachability()
	rr, err := rg.RelationsEncounteredForResource(ctx, &core.RelationReference{
		Namespace: req.DefinitionName,
		Relation:  req.PermissionName,
	})
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	relations := make([]*v1.ExpRelationReference, 0, len(rr))
	for _, r := range rr {
		if r.Namespace == req.DefinitionName && r.Relation == req.PermissionName {
			continue
		}

		ts, err := ts.GetDefinition(ctx, r.Namespace)
		if err != nil {
			return nil, shared.RewriteErrorWithoutConfig(ctx, err)
		}

		relations = append(relations, &v1.ExpRelationReference{
			DefinitionName: r.Namespace,
			RelationName:   r.Relation,
			IsPermission:   ts.IsPermission(r.Relation),
		})
	}

	sort.Slice(relations, func(i, j int) bool {
		if relations[i].DefinitionName == relations[j].DefinitionName {
			return relations[i].RelationName < relations[j].RelationName
		}

		return relations[i].DefinitionName < relations[j].DefinitionName
	})

	return &v1.ExperimentalDependentRelationsResponse{
		Relations: relations,
		ReadAt:    revisionReadAt,
	}, nil
}

func (es *experimentalServer) ExperimentalRegisterRelationshipCounter(ctx context.Context, req *v1.ExperimentalRegisterRelationshipCounterRequest) (*v1.ExperimentalRegisterRelationshipCounterResponse, error) {
	ds := datastoremw.MustFromContext(ctx)

	if req.Name == "" {
		return nil, shared.RewriteErrorWithoutConfig(ctx, spiceerrors.WithCodeAndReason(errors.New("name must be provided"), codes.InvalidArgument, v1.ErrorReason_ERROR_REASON_UNSPECIFIED))
	}

	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		if err := validateRelationshipsFilter(ctx, req.RelationshipFilter, rwt); err != nil {
			return err
		}

		coreFilter := datastore.CoreFilterFromRelationshipFilter(req.RelationshipFilter)
		return rwt.RegisterCounter(ctx, req.Name, coreFilter)
	})
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	return &v1.ExperimentalRegisterRelationshipCounterResponse{}, nil
}

func (es *experimentalServer) ExperimentalUnregisterRelationshipCounter(ctx context.Context, req *v1.ExperimentalUnregisterRelationshipCounterRequest) (*v1.ExperimentalUnregisterRelationshipCounterResponse, error) {
	ds := datastoremw.MustFromContext(ctx)

	if req.Name == "" {
		return nil, shared.RewriteErrorWithoutConfig(ctx, spiceerrors.WithCodeAndReason(errors.New("name must be provided"), codes.InvalidArgument, v1.ErrorReason_ERROR_REASON_UNSPECIFIED))
	}

	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.UnregisterCounter(ctx, req.Name)
	})
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	return &v1.ExperimentalUnregisterRelationshipCounterResponse{}, nil
}

func (es *experimentalServer) ExperimentalCountRelationships(ctx context.Context, req *v1.ExperimentalCountRelationshipsRequest) (*v1.ExperimentalCountRelationshipsResponse, error) {
	if req.Name == "" {
		return nil, shared.RewriteErrorWithoutConfig(ctx, spiceerrors.WithCodeAndReason(errors.New("name must be provided"), codes.InvalidArgument, v1.ErrorReason_ERROR_REASON_UNSPECIFIED))
	}

	ds := datastoremw.MustFromContext(ctx)
	headRev, err := ds.HeadRevision(ctx)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	snapshotReader := ds.SnapshotReader(headRev)
	count, err := snapshotReader.CountRelationships(ctx, req.Name)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	uintCount, err := safecast.ToUint64(count)
	if err != nil {
		return nil, spiceerrors.MustBugf("count should not be negative")
	}

	return &v1.ExperimentalCountRelationshipsResponse{
		CounterResult: &v1.ExperimentalCountRelationshipsResponse_ReadCounterValue{
			ReadCounterValue: &v1.ReadCounterValue{
				RelationshipCount: uintCount,
				ReadAt:            zedtoken.MustNewFromRevision(headRev),
			},
		},
	}, nil
}

func queryForEach(
	ctx context.Context,
	reader datastore.Reader,
	filter datastore.RelationshipsFilter,
	fn func(rel tuple.Relationship),
	opts ...dsoptions.QueryOptionsOption,
) (dsoptions.Cursor, error) {
	iter, err := reader.QueryRelationships(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	var cursor dsoptions.Cursor
	for rel, err := range iter {
		if err != nil {
			return nil, err
		}

		fn(rel)
		cursor = dsoptions.ToCursor(rel)
	}
	return cursor, nil
}

func decodeCursor(ds datastore.ReadOnlyDatastore, encoded *v1.Cursor) (datastore.Revision, string, dsoptions.Cursor, error) {
	decoded, err := cursor.Decode(encoded)
	if err != nil {
		return datastore.NoRevision, "", nil, err
	}

	if decoded.GetV1() == nil {
		return datastore.NoRevision, "", nil, errors.New("malformed cursor: no V1 in OneOf")
	}

	if len(decoded.GetV1().Sections) != 2 {
		return datastore.NoRevision, "", nil, errors.New("malformed cursor: wrong number of components")
	}

	atRevision, err := ds.RevisionFromString(decoded.GetV1().Revision)
	if err != nil {
		return datastore.NoRevision, "", nil, err
	}

	cur, err := tuple.Parse(decoded.GetV1().GetSections()[1])
	if err != nil {
		return datastore.NoRevision, "", nil, fmt.Errorf("malformed cursor: invalid encoded relation tuple: %w", err)
	}

	// Returns the current namespace and the cursor.
	return atRevision, decoded.GetV1().GetSections()[0], dsoptions.ToCursor(cur), nil
}
