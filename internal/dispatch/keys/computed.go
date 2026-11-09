package keys

import (
	"strconv"

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
	planCheckBatchPrefix      cachePrefix = "pcb"
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
	planCheckBatchPrefix,
	planLookupResourcesPrefix,
	planLookupSubjectsPrefix,
}

// checkRequestToKey converts a check request into a cache key based on the relation
func checkRequestToKey(req *v1.DispatchCheckRequest) DispatchCacheKey {
	args := []hashableValue{
		hashableRelationReference{req.ResourceRelation},
		hashableIds(req.ResourceIds),
		hashableOnr{req.Subject},
		hashableResultSetting(req.ResultsSetting),
	}

	if len(req.CheckHints) > 0 {
		args = append(args, hashableCheckHints(req.CheckHints))
	}

	return dispatchCacheKeyHash(checkViaRelationPrefix, req.Metadata.AtRevision, args...)
}

// checkRequestToKeyWithCanonical converts a check request into a cache key based
// on the canonical key.
func checkRequestToKeyWithCanonical(req *v1.DispatchCheckRequest, canonicalKey string) (DispatchCacheKey, error) {
	// NOTE: canonical cache keys are only unique *within* a version of a namespace.
	args := []hashableValue{
		hashableString(req.ResourceRelation.Namespace),
		hashableString(canonicalKey),
		hashableIds(req.ResourceIds),
		hashableOnr{req.Subject},
		hashableResultSetting(req.ResultsSetting),
	}

	if len(req.CheckHints) > 0 {
		args = append(args, hashableCheckHints(req.CheckHints))
	}

	cacheKey := dispatchCacheKeyHash(checkViaCanonicalPrefix, req.Metadata.AtRevision, args...)

	if canonicalKey == "" {
		return cacheKey, spiceerrors.MustBugf(
			"given empty canonical key for request: %s => %s",
			req.ResourceRelation,
			tuple.StringCoreONR(req.Subject),
		)
	}

	return cacheKey, nil
}

// expandRequestToKey converts an expand request into a cache key
func expandRequestToKey(req *v1.DispatchExpandRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(
		expandPrefix, req.Metadata.AtRevision,
		hashableOnr{req.ResourceAndRelation},
	)
}

// lookupResourcesRequest2ToKey converts a lookup request into a cache key
func lookupResourcesRequest2ToKey(req *v1.DispatchLookupResources2Request) (DispatchCacheKey, error) {
	stableContextString, err := caveats.StableContextStringForHashing(req.Context)
	if err != nil {
		return emptyDispatchCacheKey, err
	}

	return dispatchCacheKeyHash(
		lookupPrefix, req.Metadata.AtRevision,
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

	return dispatchCacheKeyHash(
		lookupPrefix, req.Metadata.AtRevision,
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
	return dispatchCacheKeyHash(
		lookupSubjectsPrefix, req.Metadata.AtRevision,
		hashableRelationReference{req.ResourceRelation},
		hashableRelationReference{req.SubjectRelation},
		hashableIds(req.ResourceIds),
	)
}

// planCheckRequestToKey converts a plan check request into a cache key
func planCheckRequestToKey(req *v1.DispatchQueryPlanRequest) DispatchCacheKey {
	if req.Operation == v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES || req.Operation == v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS {
		return planCheckBatchRequestToKey(req)
	}

	return PlanCheckLookupKey(
		req.PlanContext.Revision,
		currentDispatchKey(req.PlanContext),
		req.Resource,
		req.Subject,
		req.PlanContext.CaveatContext,
		req.PlanContext.MaxRecursionDepth,
	)
}

// Finite batch keys preserve the full target vector and result requirement.
// An existential response can omit unresolved targets, so it must never satisfy
// an exhaustive batch. A dedicated prefix also isolates the batch's response
// sequence encoding from the scalar path/denial encoding.
func planCheckBatchRequestToKey(req *v1.DispatchQueryPlanRequest) DispatchCacheKey {
	pc := req.GetPlanContext()
	args := make([]hashableValue, 0, 7+len(req.Many))
	args = append(
		args,
		hashableString(currentDispatchKey(pc)),
		hashableString(strconv.FormatInt(int64(req.Operation), 10)),
		hashableOnr{req.Resource},
		hashableOnr{req.Subject},
		hashableContext{Struct: pc.GetCaveatContext()},
		hashableString(strconv.FormatInt(int64(pc.GetMaxRecursionDepth()), 10)),
		hashableResultSetting(req.ResultsSetting),
	)

	for _, target := range req.Many {
		args = append(args, hashableOnr{target})
	}

	return dispatchCacheKeyHash(planCheckBatchPrefix, pc.GetRevision(), args...)
}

// currentDispatchKey returns the canonical "def#rel" key for the dispatch
// currently being processed. By contract — see planContextForDispatch in
// internal/dispatch/executor.go — the sender appends the current dispatch's
// key to PlanContext.InProgressKeys before sending, so the last entry is
// always the current dispatch's key. Returns "" if InProgressKeys is empty,
// which is invalid for a Plan-dispatched request but is tolerated to keep
// this helper safe to call on partial test fixtures.
func currentDispatchKey(pc *v1.PlanContext) string {
	if pc == nil || len(pc.InProgressKeys) == 0 {
		return ""
	}
	return pc.InProgressKeys[len(pc.InProgressKeys)-1]
}

// PlanCheckLookupKey computes the same Plan-Check cache key as
// planCheckRequestToKey, but takes the inputs directly so callers can probe the
// cache without first having to construct a DispatchQueryPlanRequest (which
// forces iterator serialization). The hashed component set must stay identical
// to planCheckRequestToKey or the cache-hit fast path and the DispatchQueryPlan
// slow path will end up on different cache entries. The recursion budget is
// included because a smaller budget can stop traversal before a cached result.
func PlanCheckLookupKey(
	revision string,
	canonicalKey string,
	resource, subject *core.ObjectAndRelation,
	caveatContext *structpb.Struct,
	maxRecursionDepth int32,
) DispatchCacheKey {
	return dispatchCacheKeyHash(
		planCheckPrefix, revision,
		hashableString(canonicalKey),
		hashableOnr{resource},
		hashableOnr{subject},
		hashableContext{Struct: caveatContext},
		hashableString(strconv.FormatInt(int64(maxRecursionDepth), 10)),
	)
}

// planLookupResourcesRequestToKey converts a plan lookup resources request into a cache key
func planLookupResourcesRequestToKey(req *v1.DispatchQueryPlanRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(
		planLookupResourcesPrefix, req.PlanContext.Revision,
		hashableString(currentDispatchKey(req.PlanContext)),
		hashableOnr{req.Subject},
		hashableContext{Struct: req.PlanContext.CaveatContext},
	)
}

// planLookupSubjectsRequestToKey converts a plan lookup subjects request into a cache key
func planLookupSubjectsRequestToKey(req *v1.DispatchQueryPlanRequest) DispatchCacheKey {
	return dispatchCacheKeyHash(
		planLookupSubjectsPrefix, req.PlanContext.Revision,
		hashableString(currentDispatchKey(req.PlanContext)),
		hashableOnr{req.Resource},
		hashableContext{Struct: req.PlanContext.CaveatContext},
	)
}
