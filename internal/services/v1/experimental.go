package v1

import (
	"context"
	"errors"
	"io"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/samber/lo"

	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/middleware"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/streamtimeout"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/relationships"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/services/v1/options"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	servicesv1 "github.com/authzed/spicedb/pkg/services/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/typesystem"
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
		defaultBatchSize: uint64(config.DefaultExportBatchSize),
		maxBatchSize:     uint64(config.MaxExportBatchSize),
		bulkChecker: &bulkChecker{
			maxAPIDepth:          permServerConfig.MaximumAPIDepth,
			maxCaveatContextSize: permServerConfig.MaxCaveatContextSize,
			maxConcurrency:       config.BulkCheckMaxConcurrency,
			dispatch:             dispatch,
		},
	}
}

type experimentalServer struct {
	v1.UnimplementedExperimentalServiceServer
	shared.WithServiceSpecificInterceptors

	defaultBatchSize uint64
	maxBatchSize     uint64

	bulkChecker *bulkChecker
}

type bulkLoadAdapter struct {
	stream                 v1.ExperimentalService_BulkImportRelationshipsServer
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

func (es *experimentalServer) BulkImportRelationships(stream v1.ExperimentalService_BulkImportRelationshipsServer) error {
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

func (es *experimentalServer) BulkExportRelationships(
	req *v1.BulkExportRelationshipsRequest,
	resp v1.ExperimentalService_BulkExportRelationshipsServer,
) error {
	if req.OptionalLimit > 0 && uint64(req.OptionalLimit) > es.maxBatchSize {
		return shared.RewriteErrorWithoutConfig(resp.Context(), NewExceedsMaximumLimitErr(uint64(req.OptionalLimit), es.maxBatchSize))
	}

	ctx := resp.Context()
	atRevision, _, err := consistency.RevisionFromContext(ctx)
	if err != nil {
		return shared.RewriteErrorWithoutConfig(ctx, err)
	}

	return servicesv1.BulkExport(ctx, datastoremw.MustFromContext(ctx), es.maxBatchSize, req, atRevision, resp.Send)
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
