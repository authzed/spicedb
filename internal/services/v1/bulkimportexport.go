package v1

import (
	"context"
	"errors"
	"io"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/grpc"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/relationships"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/typesystem"
)


type bulkLoadAdapter struct {
	stream                 grpc.ClientStreamingServer[v1.ImportBulkRelationshipsRequest, v1.ImportBulkRelationshipsResponse]
	referencedNamespaceMap map[string]*typesystem.TypeSystem
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
	a.current.Integrity = nil
	tuple.CopyRelationshipToRelationTuple(a.currentBatch[a.numSent], &a.current)

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


// bulkImporter contains the logic to allow ExperimentalService/BulkImportRelationships and
// PermissionsService/ImportBulkRelationships to share the same implementation.
// TODO: this may not be necessary. if the bulkExporter is similarly empty let's remove it.
type bulkImporter struct {
	maxBatchSize     uint64
}

func (bi *bulkImporter) bulkImportRelationships(streamCtx context.Context, adapter *bulkLoadAdapter) (*v1.ImportBulkRelationshipsResponse, error) {
	ds := datastoremw.MustFromContext(streamCtx)

	var numWritten uint64
	if _, err := ds.ReadWriteTx(streamCtx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		loadedNamespaces := make(map[string]*typesystem.TypeSystem, 2)
		loadedCaveats := make(map[string]*core.CaveatDefinition, 0)

		// NOTE: these can't be lifted to the adapter declaration in the calling code because they
		// depend on local context.
		adapter.referencedNamespaceMap = loadedNamespaces
		adapter.referencedCaveatMap = loadedCaveats
		// NOTE: these fields are common to both callsites, so we push them down here.
		adapter.current = core.RelationTuple{
			ResourceAndRelation: &core.ObjectAndRelation{},
			Subject:             &core.ObjectAndRelation{},
		}
		adapter.caveat = core.ContextualizedCaveat{}
		resolver := typesystem.ResolverForDatastoreReader(rwt)

		var streamWritten uint64
		var err error
		for ; adapter.err == nil && err == nil; streamWritten, err = rwt.BulkLoad(streamCtx, adapter) {
			numWritten += streamWritten

			// The stream has terminated because we're awaiting namespace and/or caveat information
			if len(adapter.awaitingNamespaces) > 0 {
				nsDefs, err := rwt.LookupNamespacesWithNames(streamCtx, adapter.awaitingNamespaces)
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
				caveats, err := rwt.LookupCaveatsWithNames(streamCtx, adapter.awaitingCaveats)
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
		return nil, shared.RewriteErrorWithoutConfig(streamCtx, err)
	}

	usagemetrics.SetInContext(streamCtx, &dispatchv1.ResponseMeta{
		// One request for the whole load
		DispatchCount: 1,
	})

	return &v1.ImportBulkRelationshipsResponse{
		NumLoaded: numWritten,
	}, nil
}

type bulkExporter struct {
	maxBatchSize uint64
}

func (be *bulkExporter) BulkExportRelationships ()
