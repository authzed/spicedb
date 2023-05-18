package v1

import (
	"context"
	"errors"
	"io"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/samber/lo"
	"golang.org/x/exp/slices"

	"github.com/authzed/spicedb/internal/middleware"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/streamtimeout"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/relationships"
	"github.com/authzed/spicedb/internal/services/shared"
	options "github.com/authzed/spicedb/internal/services/v1/options"
	"github.com/authzed/spicedb/pkg/cursor"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	implv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewExperimentalServer creates a ExperimentalServiceServer instance.
func NewExperimentalServer(opts ...options.ExperimentalServerOptionsOption) v1.ExperimentalServiceServer {
	config := options.NewExperimentalServerOptionsWithOptionsAndDefaults(opts...)

	return &experimentalServer{
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
				streamtimeout.MustStreamServerInterceptor(config.StreamReadTimeount),
			),
		},
	}
}

type experimentalServer struct {
	v1.UnimplementedExperimentalServiceServer
	shared.WithServiceSpecificInterceptors
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
		return shared.RewriteError(stream.Context(), err)
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
			return shared.RewriteError(ctx, err)
		}
	} else {
		var err error
		atRevision, _, err = consistency.RevisionFromContext(ctx)
		if err != nil {
			return shared.RewriteError(ctx, err)
		}
	}

	reader := ds.SnapshotReader(atRevision)

	namespaces, err := reader.ListAllNamespaces(ctx)
	if err != nil {
		return shared.RewriteError(ctx, err)
	}

	// Make sure the namespaces are always in a stable order
	slices.SortFunc(namespaces, func(
		lhs datastore.RevisionedDefinition[*core.NamespaceDefinition],
		rhs datastore.RevisionedDefinition[*core.NamespaceDefinition],
	) bool {
		return lhs.Definition.Name < rhs.Definition.Name
	})

	// Skip the namespaces that are already fully returned
	for cur != nil && len(namespaces) > 0 && namespaces[0].Definition.Name < cur.ResourceAndRelation.Namespace {
		namespaces = namespaces[1:]
	}

	limit := uint64(1_000)
	if req.OptionalLimit > 0 {
		limit = uint64(req.OptionalLimit)
	}

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

		for uint64(len(rels)) == limit {
			// Lop off any rels we've already sent
			rels = rels[:0]

			iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType: ns.Definition.Name,
			}, dsoptions.WithLimit(&limit), dsoptions.WithAfter(cur), dsoptions.WithSort(dsoptions.ByResource))
			if err != nil {
				return shared.RewriteError(ctx, err)
			}

			for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
				offset := len(rels)
				rels = append(rels, &relsArray[offset]) // nozero
				tuple.CopyRelationTupleToRelationship(tpl, &relsArray[offset], &caveatArray[offset])
			}
			if iter.Err() != nil {
				return shared.RewriteError(ctx, iter.Err())
			}

			if len(rels) == 0 {
				iter.Close()
				continue
			}

			cur, err = iter.Cursor()
			if err != nil {
				return shared.RewriteError(ctx, err)
			}
			iter.Close()

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
				return shared.RewriteError(ctx, err)
			}

			if err := resp.Send(&v1.BulkExportRelationshipsResponse{
				AfterResultCursor: encoded,
				Relationships:     rels,
			}); err != nil {
				return shared.RewriteError(ctx, err)
			}
		}

		// Datastore namespace order might not be exactly the same as go namespace order
		// so we shouldn't assume cursors are valid across namespaces
		cur = nil
	}

	return nil
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
