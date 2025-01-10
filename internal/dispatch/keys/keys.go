package keys

import (
	"context"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// Handler is an interface defining how keys are computed for dispatching and caching.
type Handler interface {
	// CheckCacheKey computes the caching key for a Check operation.
	CheckCacheKey(ctx context.Context, req *v1.DispatchCheckRequest) (DispatchCacheKey, error)

	// LookupResources2CacheKey computes the caching key for a LookupResources2 operation.
	LookupResources2CacheKey(ctx context.Context, req *v1.DispatchLookupResources2Request) (DispatchCacheKey, error)

	// LookupSubjectsCacheKey computes the caching key for a LookupSubjects operation.
	LookupSubjectsCacheKey(ctx context.Context, req *v1.DispatchLookupSubjectsRequest) (DispatchCacheKey, error)

	// ExpandCacheKey computes the caching key for an Expand operation.
	ExpandCacheKey(ctx context.Context, req *v1.DispatchExpandRequest) (DispatchCacheKey, error)

	// CheckDispatchKey computes the dispatch key for a Check operation.
	CheckDispatchKey(ctx context.Context, req *v1.DispatchCheckRequest) ([]byte, error)

	// LookupResources2DispatchKey computes the dispatch key for a LookupResources2 operation.
	LookupResources2DispatchKey(ctx context.Context, req *v1.DispatchLookupResources2Request) ([]byte, error)

	// LookupSubjectsDispatchKey computes the key for a LookupSubjects operation.
	LookupSubjectsDispatchKey(ctx context.Context, req *v1.DispatchLookupSubjectsRequest) ([]byte, error)

	// ExpandDispatchKey computes the dispatch key for an Expand operation.
	ExpandDispatchKey(ctx context.Context, req *v1.DispatchExpandRequest) ([]byte, error)
}

type baseKeyHandler struct{}

func (b baseKeyHandler) LookupResources2CacheKey(_ context.Context, req *v1.DispatchLookupResources2Request) (DispatchCacheKey, error) {
	return lookupResourcesRequest2ToKey(req, computeBothHashes), nil
}

func (b baseKeyHandler) LookupSubjectsCacheKey(_ context.Context, req *v1.DispatchLookupSubjectsRequest) (DispatchCacheKey, error) {
	return lookupSubjectsRequestToKey(req, computeBothHashes), nil
}

func (b baseKeyHandler) ExpandCacheKey(_ context.Context, req *v1.DispatchExpandRequest) (DispatchCacheKey, error) {
	return expandRequestToKey(req, computeBothHashes), nil
}

func (b baseKeyHandler) CheckDispatchKey(_ context.Context, req *v1.DispatchCheckRequest) ([]byte, error) {
	return checkRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

func (b baseKeyHandler) LookupResources2DispatchKey(_ context.Context, req *v1.DispatchLookupResources2Request) ([]byte, error) {
	return lookupResourcesRequest2ToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

func (b baseKeyHandler) LookupSubjectsDispatchKey(_ context.Context, req *v1.DispatchLookupSubjectsRequest) ([]byte, error) {
	return lookupSubjectsRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

func (b baseKeyHandler) ExpandDispatchKey(_ context.Context, req *v1.DispatchExpandRequest) ([]byte, error) {
	return expandRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

// DirectKeyHandler is a key handler that uses the relation name itself as the key.
type DirectKeyHandler struct {
	baseKeyHandler
}

func (d *DirectKeyHandler) CheckCacheKey(_ context.Context, req *v1.DispatchCheckRequest) (DispatchCacheKey, error) {
	return checkRequestToKey(req, computeBothHashes), nil
}

// CanonicalKeyHandler is a key handler which makes use of the canonical key for relations for
// dispatching.
type CanonicalKeyHandler struct {
	baseKeyHandler
}

func (c *CanonicalKeyHandler) CheckCacheKey(ctx context.Context, req *v1.DispatchCheckRequest) (DispatchCacheKey, error) {
	// NOTE: We do not use the canonicalized cache key when checking within the same namespace, as
	// we may get different results if the subject being checked matches the resource exactly, e.g.
	// a check for `somenamespace:someobject#somerel@somenamespace:someobject#somerel`.
	if req.ResourceRelation.Namespace != req.Subject.Namespace {
		// Load the relation to get its computed cache key, if any.
		ds := datastoremw.MustFromContext(ctx)

		revision, err := ds.RevisionFromString(req.Metadata.AtRevision)
		if err != nil {
			return emptyDispatchCacheKey, err
		}
		r := ds.SnapshotReader(revision)

		_, relation, err := namespace.ReadNamespaceAndRelation(
			ctx,
			req.ResourceRelation.Namespace,
			req.ResourceRelation.Relation,
			r,
		)
		if err != nil {
			return emptyDispatchCacheKey, err
		}

		if relation.CanonicalCacheKey != "" {
			return checkRequestToKeyWithCanonical(req, relation.CanonicalCacheKey)
		}
	}

	return checkRequestToKey(req, computeBothHashes), nil
}
