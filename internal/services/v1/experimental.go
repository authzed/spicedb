package v1

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/ccoveille/go-safecast"

	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/middleware"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/streamtimeout"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/services/v1/options"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	implv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/typesystem"
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
		bulkChecker: &bulkChecker{
			maxAPIDepth:          permServerConfig.MaximumAPIDepth,
			maxCaveatContextSize: permServerConfig.MaxCaveatContextSize,
			maxConcurrency:       config.BulkCheckMaxConcurrency,
			dispatch:             dispatch,
			dispatchChunkSize:    chunkSize,
		},
		bulkImporter: &bulkImporter{},
		bulkExporter: &bulkExporter{
			maxBatchSize:     uint64(config.MaxExportBatchSize),
		},
	}
}

type experimentalServer struct {
	v1.UnimplementedExperimentalServiceServer
	shared.WithServiceSpecificInterceptors

	bulkChecker *bulkChecker
	bulkImporter *bulkImporter
	bulkExporter *bulkExporter
}

func extractBatchNewReferencedNamespacesAndCaveats(
	batch []*v1.Relationship,
	existingNamespaces map[string]*typesystem.TypeSystem,
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

func (es *experimentalServer) BulkImportRelationships(stream grpc.ClientStreamingServer[v1.BulkImportRelationshipsRequest, v1.BulkImportRelationshipsResponse]) error {
}

func (es *experimentalServer) BulkExportRelationships(
	req *v1.BulkExportRelationshipsRequest,
	resp grpc.ServerStreamingServer[v1.BulkExportRelationshipsResponse],
) error {
	ctx := resp.Context()
	atRevision, _, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return shared.RewriteErrorWithoutConfig(ctx, err)
	}

	return BulkExport(ctx, datastoremw.MustFromContext(ctx), es.bulkExporter.maxBatchSize, req, atRevision, resp.Send)
}

// BulkExport implements the BulkExportRelationships API functionality. Given a datastore.Datastore, it will
// export stream via the sender all relationships matched by the incoming request.
// If no cursor is provided, it will fallback to the provided revision.
func BulkExport(ctx context.Context, ds datastore.Datastore, batchSize uint64, req *v1.ExportBulkRelationshipsRequest, fallbackRevision datastore.Revision, sender func(response *v1.ExportBulkRelationshipsResponse) error) error {
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

			tplFn := func(tpl *core.RelationTuple) {
				offset := len(rels)
				rels = append(rels, &relsArray[offset]) // nozero
				tuple.CopyRelationTupleToRelationship(tpl, &relsArray[offset], &caveatArray[offset])
			}

			cur, err = queryForEach(
				ctx,
				reader,
				relationshipFilter,
				tplFn,
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
							tuple.MustString(cur),
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

func (es *experimentalServer) ImportBulkRelationships(stream v1.PermissionsService_ImportBulkRelationshipsServer) error {
	ds := datastoremw.MustFromContext(stream.Context())

	var numWritten uint64
	if _, err := ds.ReadWriteTx(stream.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		loadedNamespaces := make(map[string]*typesystem.TypeSystem, 2)
		loadedCaveats := make(map[string]*core.CaveatDefinition, 0)

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
		resolver := typesystem.ResolverForDatastoreReader(rwt)

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
					nts, err := typesystem.NewNamespaceTypeSystem(nsDef.Definition, resolver)
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

// BulkExport implements the BulkExportRelationships API functionality. Given a datastore.Datastore, it will
// export stream via the sender all relationships matched by the incoming request.
// If no cursor is provided, it will fallback to the provided revision.
func BulkExport(ctx context.Context, ds datastore.Datastore, batchSize uint64, req *v1.BulkExportRelationshipsRequest, fallbackRevision datastore.Revision, sender func(response *v1.BulkExportRelationshipsResponse) error) error {
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

			tplFn := func(tpl *core.RelationTuple) {
				offset := len(rels)
				rels = append(rels, &relsArray[offset]) // nozero
				tuple.CopyRelationTupleToRelationship(tpl, &relsArray[offset], &caveatArray[offset])
			}

			cur, err = queryForEach(
				ctx,
				reader,
				relationshipFilter,
				tplFn,
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
							tuple.MustString(cur),
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

const maxBulkCheckCount = 10000

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

	filters, err := newSchemaFilters(req.OptionalFilters)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	definitions := make([]*v1.ExpDefinition, 0, len(schema.ObjectDefinitions))
	if filters.HasNamespaces() {
		for _, ns := range schema.ObjectDefinitions {
			def, err := namespaceAPIRepr(ns, filters)
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
			caveat, err := caveatAPIRepr(cd, filters)
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

	resp, err := convertDiff(diff, existingSchema, comparisonSchema, atRevision)
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
	_, vts, err := typesystem.ReadNamespaceAndTypes(ctx, req.DefinitionName, ds)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	relationName := req.RelationName
	if relationName == "" {
		relationName = tuple.Ellipsis
	} else {
		if _, ok := vts.GetRelation(relationName); !ok {
			return nil, shared.RewriteErrorWithoutConfig(ctx, typesystem.NewRelationNotFoundErr(req.DefinitionName, relationName))
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

	rg := typesystem.ReachabilityGraphFor(vts)
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

		ts, err := vts.TypeSystemForNamespace(ctx, r.Namespace)
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
	_, vts, err := typesystem.ReadNamespaceAndTypes(ctx, req.DefinitionName, ds)
	if err != nil {
		return nil, shared.RewriteErrorWithoutConfig(ctx, err)
	}

	_, ok := vts.GetRelation(req.PermissionName)
	if !ok {
		return nil, shared.RewriteErrorWithoutConfig(ctx, typesystem.NewRelationNotFoundErr(req.DefinitionName, req.PermissionName))
	}

	if !vts.IsPermission(req.PermissionName) {
		return nil, shared.RewriteErrorWithoutConfig(ctx, NewNotAPermissionError(req.PermissionName))
	}

	rg := typesystem.ReachabilityGraphFor(vts)
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

		ts, err := vts.TypeSystemForNamespace(ctx, r.Namespace)
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

func decodeCursor(ds datastore.Datastore, encoded *v1.Cursor) (datastore.Revision, string, *core.RelationTuple, error) {
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

	cur := tuple.Parse(decoded.GetV1().GetSections()[1])
	if cur == nil {
		return datastore.NoRevision, "", nil, errors.New("malformed cursor: invalid encoded relation tuple")
	}

	// Returns the current namespace and the cursor.
	return atRevision, decoded.GetV1().GetSections()[0], cur, nil
}
