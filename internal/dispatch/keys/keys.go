package keys

import (
	"context"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// Handler is an interface defining how keys are computed for dispatching and caching.
type Handler interface {
	// CheckCachekKey computes the caching key for a Check operation.
	CheckCacheKey(ctx context.Context, req *v1.DispatchCheckRequest) (DispatchCacheKey, error)

	// LookupResourcesCacheKey computes the caching key for a LookupResources operation.
	LookupResourcesCacheKey(ctx context.Context, req *v1.DispatchLookupResourcesRequest) (DispatchCacheKey, error)

	// LookupSubjectsCacheKey computes the caching key for a LookupSubjects operation.
	LookupSubjectsCacheKey(ctx context.Context, req *v1.DispatchLookupSubjectsRequest) (DispatchCacheKey, error)

	// ExpandCacheKey computes the caching key for an Expand operation.
	ExpandCacheKey(ctx context.Context, req *v1.DispatchExpandRequest) (DispatchCacheKey, error)

	// ReachableResourcesCacheKey computes the caching key for a ReachableResources operation.
	ReachableResourcesCacheKey(ctx context.Context, req *v1.DispatchReachableResourcesRequest) (DispatchCacheKey, error)

	// CheckDispatchKey computes the dispatch key for a Check operation.
	CheckDispatchKey(ctx context.Context, req *v1.DispatchCheckRequest) ([]byte, error)

	// LookupResourcesDispatchKey computes the dispatch key for a LookupResources operation.
	LookupResourcesDispatchKey(ctx context.Context, req *v1.DispatchLookupResourcesRequest) ([]byte, error)

	// LookupSubjectsDispatchKey computes the key for a LookupSubjects operation.
	LookupSubjectsDispatchKey(ctx context.Context, req *v1.DispatchLookupSubjectsRequest) ([]byte, error)

	// ExpandDispatchKey computes the dispatch key for an Expand operation.
	ExpandDispatchKey(ctx context.Context, req *v1.DispatchExpandRequest) ([]byte, error)

	// ReachableResourcesDispatchKey computes the key for a ReachableResources operation.
	ReachableResourcesDispatchKey(ctx context.Context, req *v1.DispatchReachableResourcesRequest) ([]byte, error)
}

type baseKeyHandler struct{}

func (b baseKeyHandler) LookupResourcesCacheKey(_ context.Context, req *v1.DispatchLookupResourcesRequest) (DispatchCacheKey, error) {
	return lookupResourcesRequestToKey(req, computeBothHashes), nil
}

func (b baseKeyHandler) LookupSubjectsCacheKey(_ context.Context, req *v1.DispatchLookupSubjectsRequest) (DispatchCacheKey, error) {
	return lookupSubjectsRequestToKey(req, computeBothHashes), nil
}

func (b baseKeyHandler) ExpandCacheKey(_ context.Context, req *v1.DispatchExpandRequest) (DispatchCacheKey, error) {
	return expandRequestToKey(req, computeBothHashes), nil
}

func (b baseKeyHandler) ReachableResourcesCacheKey(_ context.Context, req *v1.DispatchReachableResourcesRequest) (DispatchCacheKey, error) {
	return reachableResourcesRequestToKey(req, computeBothHashes), nil
}

func (b baseKeyHandler) CheckDispatchKey(_ context.Context, req *v1.DispatchCheckRequest) ([]byte, error) {
	return checkRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

func (b baseKeyHandler) LookupResourcesDispatchKey(_ context.Context, req *v1.DispatchLookupResourcesRequest) ([]byte, error) {
	return lookupResourcesRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

func (b baseKeyHandler) LookupSubjectsDispatchKey(_ context.Context, req *v1.DispatchLookupSubjectsRequest) ([]byte, error) {
	return lookupSubjectsRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

func (b baseKeyHandler) ExpandDispatchKey(_ context.Context, req *v1.DispatchExpandRequest) ([]byte, error) {
	return expandRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

func (b baseKeyHandler) ReachableResourcesDispatchKey(_ context.Context, req *v1.DispatchReachableResourcesRequest) ([]byte, error) {
	return reachableResourcesRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
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

		// TODO(jschorr): Remove this conditional once we have a verified migration ordering system that ensures a backfill migration has
		// run after the namespace annotation code has been fully deployed by users.
		if relation.CanonicalCacheKey != "" {
			return checkRequestToKeyWithCanonical(req, relation.CanonicalCacheKey)
		}
	}

	return checkRequestToKey(req, computeBothHashes), nil
}
