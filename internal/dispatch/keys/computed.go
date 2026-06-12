package keys

import (
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// cachePrefix defines a unique prefix for a type of cache key.
type cachePrefix string

// Define the various prefixes for the cache entries. These must *all* be unique and must *all*
// also be placed into the cachePrefixes slice below.
const (
	checkViaRelationPrefix    cachePrefix = "cr"
	checkViaCanonicalPrefix   cachePrefix = "cc"
	lookupPrefix              cachePrefix = "l"
	expandPrefix              cachePrefix = "e"
	lookupSubjectsPrefix      cachePrefix = "ls"
	planCheckPrefix           cachePrefix = "pc"
	planLookupResourcesPrefix cachePrefix = "pr"
	planLookupSubjectsPrefix  cachePrefix = "ps"
)

var cachePrefixes = []cachePrefix{
	checkViaRelationPrefix,
	checkViaCanonicalPrefix,
	lookupPrefix,
	expandPrefix,
	lookupSubjectsPrefix,
	planCheckPrefix,
	planLookupResourcesPrefix,
	planLookupSubjectsPrefix,
}

// checkRequestToKey converts a check request into a cache key based on the relation
func checkRequestToKey(req *v1.DispatchCheckRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(checkViaRelationPrefix, req.Metadata.AtRevision,
		hashableRelationReference{req.ResourceRelation},
		hashableIds(req.ResourceIds),
		hashableOnr{req.Subject},
		hashableResultSetting(req.ResultsSetting),
	)
}

// checkRequestToKeyWithCanonical converts a check request into a cache key based
// on the canonical key.
func checkRequestToKeyWithCanonical(req *v1.DispatchCheckRequest, canonicalKey string) (DispatchCacheKey, error) {
	// NOTE: canonical cache keys are only unique *within* a version of a namespace.
	cacheKey := dispatchCacheKeyHash(checkViaCanonicalPrefix, req.Metadata.AtRevision,
		hashableString(req.ResourceRelation.Namespace),
		hashableString(canonicalKey),
		hashableIds(req.ResourceIds),
		hashableOnr{req.Subject},
		hashableResultSetting(req.ResultsSetting),
	)

	if canonicalKey == "" {
		return cacheKey, spiceerrors.MustBugf("given empty canonical key for request: %s => %s", req.ResourceRelation, tuple.StringCoreONR(req.Subject))
	}

	return cacheKey, nil
}

// expandRequestToKey converts an expand request into a cache key
func expandRequestToKey(req *v1.DispatchExpandRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(expandPrefix, req.Metadata.AtRevision,
		hashableOnr{req.ResourceAndRelation},
	)
}

// lookupResourcesRequest2ToKey converts a lookup request into a cache key
func lookupResourcesRequest2ToKey(req *v1.DispatchLookupResources2Request) (DispatchCacheKey, error) {
	stableContextString, err := caveats.StableContextStringForHashing(req.Context)
	if err != nil {
		return emptyDispatchCacheKey, err
	}
	return dispatchCacheKeyHash(lookupPrefix, req.Metadata.AtRevision,
		hashableRelationReference{req.ResourceRelation},
		hashableRelationReference{req.SubjectRelation},
		hashableIds(req.SubjectIds),
		hashableOnr{req.TerminalSubject},
		hashableContextString(stableContextString),
		hashableCursor{req.OptionalCursor},
		hashableLimit(req.OptionalLimit),
	), nil
}

// lookupResourcesRequest3ToKey converts a lookup request into a cache key
func lookupResourcesRequest3ToKey(req *v1.DispatchLookupResources3Request) (DispatchCacheKey, error) {
	stableContextString, err := caveats.StableContextStringForHashing(req.Context)
	if err != nil {
		return emptyDispatchCacheKey, err
	}
	return dispatchCacheKeyHash(lookupPrefix, req.Metadata.AtRevision,
		hashableRelationReference{req.ResourceRelation},
		hashableRelationReference{req.SubjectRelation},
		hashableIds(req.SubjectIds),
		hashableOnr{req.TerminalSubject},
		hashableContextString(stableContextString), // NOTE: context is included here because lookup does a single dispatch
		hashableCursorSections{req.OptionalCursor},
		hashableLimit(req.OptionalLimit),
	), nil
}

// lookupSubjectsRequestToKey converts a lookup subjects request into a cache key
func lookupSubjectsRequestToKey(req *v1.DispatchLookupSubjectsRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(lookupSubjectsPrefix, req.Metadata.AtRevision,
		hashableRelationReference{req.ResourceRelation},
		hashableRelationReference{req.SubjectRelation},
		hashableIds(req.ResourceIds),
	)
}

// planCheckRequestToKey converts a plan check request into a cache key
func planCheckRequestToKey(req *v1.DispatchQueryPlanRequest) DispatchCacheKey {
	return PlanCheckLookupKey(
		req.PlanContext.Revision,
		req.Plan,
		req.Resource,
		req.Subject,
		req.PlanContext.CaveatContext,
	)
}

// PlanCheckLookupKey computes the Plan-Check cache key from the serialized
// iterator bytes plus the request inputs. Same shape as planCheckRequestToKey
// so the cache-hit fast path (LookupPlanCheck) and the DispatchQueryPlan slow
// path produce identical keys for identical inputs.
//
// The plan bytes are the dispatch identity — content-addressed; two dispatches
// with byte-identical plans collide into the same cache slot, and any
// structural difference (advisor reorderings, optimizer variations) lands in
// a separate slot.
func PlanCheckLookupKey(revision string, plan []byte, resource, subject *core.ObjectAndRelation, caveatContext *structpb.Struct) DispatchCacheKey {
	return dispatchCacheKeyHash(planCheckPrefix, revision,
		hashableBytes(plan),
		hashableOnr{resource},
		hashableOnr{subject},
		hashableContext{Struct: caveatContext},
	)
}

// planLookupResourcesRequestToKey converts a plan lookup resources request into a cache key
func planLookupResourcesRequestToKey(req *v1.DispatchQueryPlanRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(planLookupResourcesPrefix, req.PlanContext.Revision,
		hashableBytes(req.Plan),
		hashableOnr{req.Subject},
		hashableContext{Struct: req.PlanContext.CaveatContext},
	)
}

// planLookupSubjectsRequestToKey converts a plan lookup subjects request into a cache key
func planLookupSubjectsRequestToKey(req *v1.DispatchQueryPlanRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(planLookupSubjectsPrefix, req.PlanContext.Revision,
		hashableBytes(req.Plan),
		hashableOnr{req.Resource},
		hashableContext{Struct: req.PlanContext.CaveatContext},
	)
}
