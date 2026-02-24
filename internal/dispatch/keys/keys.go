package keys

import (
	"context"

	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datalayer"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// Handler is an interface defining how keys are computed for dispatching and caching.
type Handler interface {
	// CheckCacheKey computes the caching key for a Check operation.
	CheckCacheKey(ctx context.Context, req *v1.DispatchCheckRequest) (DispatchCacheKey, error)

	// LookupResources2CacheKey computes the caching key for a LookupResources2 operation.
	LookupResources2CacheKey(ctx context.Context, req *v1.DispatchLookupResources2Request) (DispatchCacheKey, error)

	// LookupResources3CacheKey computes the caching key for a LookupResources3 operation.
	LookupResources3CacheKey(ctx context.Context, req *v1.DispatchLookupResources3Request) (DispatchCacheKey, error)

	// LookupSubjectsCacheKey computes the caching key for a LookupSubjects operation.
	LookupSubjectsCacheKey(ctx context.Context, req *v1.DispatchLookupSubjectsRequest) (DispatchCacheKey, error)

	// ExpandCacheKey computes the caching key for an Expand operation.
	ExpandCacheKey(ctx context.Context, req *v1.DispatchExpandRequest) (DispatchCacheKey, error)

	// CheckDispatchKey computes the dispatch key for a Check operation.
	CheckDispatchKey(ctx context.Context, req *v1.DispatchCheckRequest) ([]byte, error)

	// LookupResources2DispatchKey computes the dispatch key for a LookupResources2 operation.
	LookupResources2DispatchKey(ctx context.Context, req *v1.DispatchLookupResources2Request) ([]byte, error)

	// LookupResources3DispatchKey computes the dispatch key for a LookupResources3 operation.
	LookupResources3DispatchKey(ctx context.Context, req *v1.DispatchLookupResources3Request) ([]byte, error)

	// LookupSubjectsDispatchKey computes the key for a LookupSubjects operation.
	LookupSubjectsDispatchKey(ctx context.Context, req *v1.DispatchLookupSubjectsRequest) ([]byte, error)

	// ExpandDispatchKey computes the dispatch key for an Expand operation.
	ExpandDispatchKey(ctx context.Context, req *v1.DispatchExpandRequest) ([]byte, error)

	// PlanCheckCacheKey computes the caching key for a plan Check operation.
	PlanCheckCacheKey(ctx context.Context, req *v1.DispatchQueryPlanRequest) (DispatchCacheKey, error)

	// PlanCheckDispatchKey computes the dispatch key for a plan Check operation.
	PlanCheckDispatchKey(ctx context.Context, req *v1.DispatchQueryPlanRequest) ([]byte, error)

	// PlanLookupResourcesCacheKey computes the caching key for a plan LookupResources operation.
	PlanLookupResourcesCacheKey(ctx context.Context, req *v1.DispatchQueryPlanRequest) (DispatchCacheKey, error)

	// PlanLookupResourcesDispatchKey computes the dispatch key for a plan LookupResources operation.
	PlanLookupResourcesDispatchKey(ctx context.Context, req *v1.DispatchQueryPlanRequest) ([]byte, error)

	// PlanLookupSubjectsCacheKey computes the caching key for a plan LookupSubjects operation.
	PlanLookupSubjectsCacheKey(ctx context.Context, req *v1.DispatchQueryPlanRequest) (DispatchCacheKey, error)

	// PlanLookupSubjectsDispatchKey computes the dispatch key for a plan LookupSubjects operation.
	PlanLookupSubjectsDispatchKey(ctx context.Context, req *v1.DispatchQueryPlanRequest) ([]byte, error)
}

type baseKeyHandler struct{}

func (b baseKeyHandler) LookupResources2CacheKey(_ context.Context, req *v1.DispatchLookupResources2Request) (DispatchCacheKey, error) {
	return lookupResourcesRequest2ToKey(req, computeBothHashes)
}

func (b baseKeyHandler) LookupResources3CacheKey(_ context.Context, req *v1.DispatchLookupResources3Request) (DispatchCacheKey, error) {
	return lookupResourcesRequest3ToKey(req, computeBothHashes)
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
	hash, err := lookupResourcesRequest2ToKey(req, computeOnlyStableHash)
	if err != nil {
		return nil, err
	}
	return hash.StableSumAsBytes(), nil
}

func (b baseKeyHandler) LookupResources3DispatchKey(_ context.Context, req *v1.DispatchLookupResources3Request) ([]byte, error) {
	hash, err := lookupResourcesRequest3ToKey(req, computeOnlyStableHash)
	if err != nil {
		return nil, err
	}
	return hash.StableSumAsBytes(), nil
}

func (b baseKeyHandler) LookupSubjectsDispatchKey(_ context.Context, req *v1.DispatchLookupSubjectsRequest) ([]byte, error) {
	return lookupSubjectsRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

func (b baseKeyHandler) ExpandDispatchKey(_ context.Context, req *v1.DispatchExpandRequest) ([]byte, error) {
	return expandRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

func (b baseKeyHandler) PlanCheckCacheKey(_ context.Context, req *v1.DispatchQueryPlanRequest) (DispatchCacheKey, error) {
	return planCheckRequestToKey(req, computeBothHashes), nil
}

func (b baseKeyHandler) PlanCheckDispatchKey(_ context.Context, req *v1.DispatchQueryPlanRequest) ([]byte, error) {
	return planCheckRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

func (b baseKeyHandler) PlanLookupResourcesCacheKey(_ context.Context, req *v1.DispatchQueryPlanRequest) (DispatchCacheKey, error) {
	return planLookupResourcesRequestToKey(req, computeBothHashes), nil
}

func (b baseKeyHandler) PlanLookupResourcesDispatchKey(_ context.Context, req *v1.DispatchQueryPlanRequest) ([]byte, error) {
	return planLookupResourcesRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
}

func (b baseKeyHandler) PlanLookupSubjectsCacheKey(_ context.Context, req *v1.DispatchQueryPlanRequest) (DispatchCacheKey, error) {
	return planLookupSubjectsRequestToKey(req, computeBothHashes), nil
}

func (b baseKeyHandler) PlanLookupSubjectsDispatchKey(_ context.Context, req *v1.DispatchQueryPlanRequest) ([]byte, error) {
	return planLookupSubjectsRequestToKey(req, computeOnlyStableHash).StableSumAsBytes(), nil
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
		dl := datalayer.MustFromContext(ctx)

		revision, err := dl.RevisionFromString(req.Metadata.AtRevision)
		if err != nil {
			return emptyDispatchCacheKey, err
		}
		r := dl.SnapshotReader(revision, datalayer.SchemaHash(req.Metadata.GetSchemaHash()))

		sr, err := r.ReadSchema(ctx)
		if err != nil {
			return emptyDispatchCacheKey, err
		}

		_, relation, err := namespace.ReadNamespaceAndRelation(
			ctx,
			req.ResourceRelation.Namespace,
			req.ResourceRelation.Relation,
			sr,
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
