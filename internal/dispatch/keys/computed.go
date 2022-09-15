package keys

import (
	"fmt"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// cachePrefix defines a unique prefix for a type of cache key.
type cachePrefix string

// Define the various prefixes for the cache entries. These must *all* be unique and must *all*
// also be placed into the cachePrefixes slice below.
const (
	checkViaRelationPrefix   cachePrefix = "cr"
	checkViaCanonicalPrefix  cachePrefix = "cc"
	lookupPrefix             cachePrefix = "l"
	expandPrefix             cachePrefix = "e"
	reachableResourcesPrefix cachePrefix = "rr"
	lookupSubjectsPrefix     cachePrefix = "ls"
)

var cachePrefixes = []cachePrefix{
	checkViaRelationPrefix,
	checkViaCanonicalPrefix,
	lookupPrefix,
	expandPrefix,
	reachableResourcesPrefix,
	lookupSubjectsPrefix,
}

// CheckRequestToKey converts a check request into a cache key based on the relation
func CheckRequestToKey(req *v1.DispatchCheckRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(checkViaRelationPrefix, req.Metadata.AtRevision,
		hashableRelationReference{req.ResourceRelation},
		hashableIds(req.ResourceIds),
		hashableOnr{req.Subject},
		hashableResultSetting(req.ResultsSetting),
	)
}

// CheckRequestToKeyWithCanonical converts a check request into a cache key based
// on the canonical key.
func CheckRequestToKeyWithCanonical(req *v1.DispatchCheckRequest, canonicalKey string) DispatchCacheKey {
	if canonicalKey == "" {
		panic(fmt.Sprintf("given empty canonical key for request: %s => %s", req.ResourceRelation, tuple.StringONR(req.Subject)))
	}

	// NOTE: canonical cache keys are only unique *within* a version of a namespace.
	return dispatchCacheKeyHash(checkViaCanonicalPrefix, req.Metadata.AtRevision,
		hashableString(req.ResourceRelation.Namespace),
		hashableString(canonicalKey),
		hashableIds(req.ResourceIds),
		hashableOnr{req.Subject},
		hashableResultSetting(req.ResultsSetting),
	)
}

// LookupRequestToKey converts a lookup request into a cache key
func LookupRequestToKey(req *v1.DispatchLookupRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(lookupPrefix, req.Metadata.AtRevision,
		hashableRelationReference{req.ObjectRelation},
		hashableOnr{req.Subject},
	)
}

// ExpandRequestToKey converts an expand request into a cache key
func ExpandRequestToKey(req *v1.DispatchExpandRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(expandPrefix, req.Metadata.AtRevision,
		hashableOnr{req.ResourceAndRelation},
	)
}

// ReachableResourcesRequestToKey converts a reachable resources request into a cache key
func ReachableResourcesRequestToKey(req *v1.DispatchReachableResourcesRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(reachableResourcesPrefix, req.Metadata.AtRevision,
		hashableRelationReference{req.ResourceRelation},
		hashableRelationReference{req.SubjectRelation},
		hashableIds(req.SubjectIds),
	)
}

// LookupSubjectsRequestToKey converts a lookup subjects request into a cache key
func LookupSubjectsRequestToKey(req *v1.DispatchLookupSubjectsRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(lookupSubjectsPrefix, req.Metadata.AtRevision,
		hashableRelationReference{req.ResourceRelation},
		hashableRelationReference{req.SubjectRelation},
		hashableIds(req.ResourceIds),
	)
}
