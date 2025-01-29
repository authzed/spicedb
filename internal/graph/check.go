package graph

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph/hints"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/middleware/nodeid"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

var tracer = otel.Tracer("spicedb/internal/graph/check")

var dispatchChunkCountHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "spicedb_check_dispatch_chunk_count",
	Help:    "number of chunks when dispatching in check",
	Buckets: []float64{1, 2, 3, 5, 10, 25, 100, 250},
})

var directDispatchQueryHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "spicedb_check_direct_dispatch_query_count",
	Help:    "number of queries made per direct dispatch",
	Buckets: []float64{1, 2},
})

const noOriginalRelation = ""

func init() {
	prometheus.MustRegister(directDispatchQueryHistogram)
	prometheus.MustRegister(dispatchChunkCountHistogram)
}

// NewConcurrentChecker creates an instance of ConcurrentChecker.
func NewConcurrentChecker(d dispatch.Check, concurrencyLimit uint16, dispatchChunkSize uint16) *ConcurrentChecker {
	return &ConcurrentChecker{d, concurrencyLimit, dispatchChunkSize}
}

// ConcurrentChecker exposes a method to perform Check requests, and delegates subproblems to the
// provided dispatch.Check instance.
type ConcurrentChecker struct {
	d                 dispatch.Check
	concurrencyLimit  uint16
	dispatchChunkSize uint16
}

// ValidatedCheckRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedCheckRequest struct {
	*v1.DispatchCheckRequest
	Revision datastore.Revision

	// OriginalRelationName is the original relation/permission name that was used in the request,
	// before being changed due to aliasing.
	OriginalRelationName string
}

// currentRequestContext holds context information for the current request being
// processed.
type currentRequestContext struct {
	// parentReq is the parent request being processed.
	parentReq ValidatedCheckRequest

	// filteredResourceIDs are those resource IDs to be checked after filtering for
	// any resource IDs found directly matching the incoming subject.
	//
	// For example, a check of resources `user:{tom,sarah,fred}` and subject `user:sarah` will
	// result in this slice containing `tom` and `fred`, but not `sarah`, as she was found as a
	// match.
	//
	// This check and filter occurs via the filterForFoundMemberResource function in the
	// checkInternal function before the rest of the checking logic is run. This slice should never
	// be empty.
	filteredResourceIDs []string

	// resultsSetting is the results setting to use for this request and all subsequent
	// requests.
	resultsSetting v1.DispatchCheckRequest_ResultsSetting

	// dispatchChunkSize is the maximum number of resource IDs that can be specified in each dispatch.
	dispatchChunkSize uint16
}

// Check performs a check request with the provided request and context
func (cc *ConcurrentChecker) Check(ctx context.Context, req ValidatedCheckRequest, relation *core.Relation) (*v1.DispatchCheckResponse, error) {
	var startTime *time.Time
	if req.Debug != v1.DispatchCheckRequest_NO_DEBUG {
		now := time.Now()
		startTime = &now
	}

	resolved := cc.checkInternal(ctx, req, relation)
	resolved.Resp.Metadata = addCallToResponseMetadata(resolved.Resp.Metadata)
	if req.Debug == v1.DispatchCheckRequest_NO_DEBUG {
		return resolved.Resp, resolved.Err
	}

	nodeID, err := nodeid.FromContext(ctx)
	if err != nil {
		// NOTE: we ignore this error here as if the node ID is missing, the debug
		// trace is still valid.
		log.Err(err).Msg("failed to get node ID")
	}

	// Add debug information if requested.
	debugInfo := resolved.Resp.Metadata.DebugInfo
	if debugInfo == nil {
		debugInfo = &v1.DebugInformation{
			Check: &v1.CheckDebugTrace{
				TraceId:  NewTraceID(),
				SourceId: nodeID,
			},
		}
	} else if debugInfo.Check != nil && debugInfo.Check.SourceId == "" {
		debugInfo.Check.SourceId = nodeID
	}

	// Remove the traversal bloom from the debug request to save some data over the
	// wire.
	clonedRequest := req.DispatchCheckRequest.CloneVT()
	clonedRequest.Metadata.TraversalBloom = nil

	debugInfo.Check.Request = clonedRequest
	debugInfo.Check.Duration = durationpb.New(time.Since(*startTime))

	if nspkg.GetRelationKind(relation) == iv1.RelationMetadata_PERMISSION {
		debugInfo.Check.ResourceRelationType = v1.CheckDebugTrace_PERMISSION
	} else if nspkg.GetRelationKind(relation) == iv1.RelationMetadata_RELATION {
		debugInfo.Check.ResourceRelationType = v1.CheckDebugTrace_RELATION
	}

	// Build the results for the debug trace.
	results := make(map[string]*v1.ResourceCheckResult, len(req.DispatchCheckRequest.ResourceIds))
	for _, resourceID := range req.DispatchCheckRequest.ResourceIds {
		if found, ok := resolved.Resp.ResultsByResourceId[resourceID]; ok {
			results[resourceID] = found
		}
	}
	debugInfo.Check.Results = results

	// If there is existing debug information in the error, then place it as the subproblem of the current
	// debug information.
	if existingDebugInfo, ok := spiceerrors.GetDetails[*v1.DebugInformation](resolved.Err); ok {
		debugInfo.Check.SubProblems = []*v1.CheckDebugTrace{existingDebugInfo.Check}
	}

	resolved.Resp.Metadata.DebugInfo = debugInfo

	// If there is an error and it is already a gRPC error, add the debug information
	// into the details portion of the payload. This allows the client to see the debug
	// information, as gRPC will only return the error.
	updatedErr := spiceerrors.WithReplacedDetails(resolved.Err, debugInfo)
	return resolved.Resp, updatedErr
}

func (cc *ConcurrentChecker) checkInternal(ctx context.Context, req ValidatedCheckRequest, relation *core.Relation) CheckResult {
	spiceerrors.DebugAssert(func() bool {
		return relation.GetUsersetRewrite() != nil || relation.GetTypeInformation() != nil
	}, "found relation without type information")

	// Ensure that we have at least one resource ID for which to execute the check.
	if len(req.ResourceIds) == 0 {
		return checkResultError(
			spiceerrors.MustBugf("empty resource IDs given to dispatched check"),
			emptyMetadata,
		)
	}

	// Ensure that we are not performing a check for a wildcard as the subject.
	if req.Subject.ObjectId == tuple.PublicWildcard {
		return checkResultError(NewWildcardNotAllowedErr("cannot perform check on wildcard subject", "subject.object_id"), emptyMetadata)
	}

	// Deduplicate any incoming resource IDs.
	resourceIds := lo.Uniq(req.ResourceIds)

	// Filter the incoming resource IDs for any which match the subject directly. For example, if we receive
	// a check for resource `user:{tom, fred, sarah}#...` and a subject of `user:sarah#...`, then we know
	// that `user:sarah#...` is a valid "member" of the resource, as it matches exactly.
	//
	// If the filtering results in no further resource IDs to check, or a result is found and a single
	// result is allowed, we terminate early.
	membershipSet, filteredResourcesIds := filterForFoundMemberResource(req.ResourceRelation, resourceIds, req.Subject)
	if membershipSet.HasDeterminedMember() && req.DispatchCheckRequest.ResultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT {
		return checkResultsForMembership(membershipSet, emptyMetadata)
	}

	// Filter for check hints, if any.
	if len(req.CheckHints) > 0 {
		subject := tuple.FromCoreObjectAndRelation(req.Subject)
		filteredResourcesIdsSet := mapz.NewSet(filteredResourcesIds...)
		for _, checkHint := range req.CheckHints {
			resourceID, ok := hints.AsCheckHintForComputedUserset(checkHint, req.ResourceRelation.Namespace, req.ResourceRelation.Relation, subject)
			if ok {
				filteredResourcesIdsSet.Delete(resourceID)
				continue
			}

			if req.OriginalRelationName != "" {
				resourceID, ok = hints.AsCheckHintForComputedUserset(checkHint, req.ResourceRelation.Namespace, req.OriginalRelationName, subject)
				if ok {
					filteredResourcesIdsSet.Delete(resourceID)
				}
			}
		}
		filteredResourcesIds = filteredResourcesIdsSet.AsSlice()
	}

	if len(filteredResourcesIds) == 0 {
		return combineWithCheckHints(combineResultWithFoundResources(noMembers(), membershipSet), req)
	}

	// NOTE: We can always allow a single result if we're only trying to find the results for a
	// single resource ID. This "reset" allows for short circuiting of downstream dispatched calls.
	resultsSetting := req.ResultsSetting
	if len(filteredResourcesIds) == 1 {
		resultsSetting = v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT
	}

	crc := currentRequestContext{
		parentReq:           req,
		filteredResourceIDs: filteredResourcesIds,
		resultsSetting:      resultsSetting,
		dispatchChunkSize:   cc.dispatchChunkSize,
	}

	if req.Debug == v1.DispatchCheckRequest_ENABLE_TRACE_DEBUGGING {
		crc.dispatchChunkSize = 1
	}

	if relation.UsersetRewrite == nil {
		return combineWithCheckHints(combineResultWithFoundResources(cc.checkDirect(ctx, crc, relation), membershipSet), req)
	}

	return combineWithCheckHints(combineResultWithFoundResources(cc.checkUsersetRewrite(ctx, crc, relation.UsersetRewrite), membershipSet), req)
}

func combineWithComputedHints(result CheckResult, hints map[string]*v1.ResourceCheckResult) CheckResult {
	if len(hints) == 0 {
		return result
	}

	for resourceID, hint := range hints {
		if _, ok := result.Resp.ResultsByResourceId[resourceID]; ok {
			return checkResultError(
				spiceerrors.MustBugf("check hint for resource ID %q, which already exists", resourceID),
				emptyMetadata,
			)
		}

		if result.Resp.ResultsByResourceId == nil {
			result.Resp.ResultsByResourceId = make(map[string]*v1.ResourceCheckResult)
		}
		result.Resp.ResultsByResourceId[resourceID] = hint
	}

	return result
}

func combineWithCheckHints(result CheckResult, req ValidatedCheckRequest) CheckResult {
	if len(req.CheckHints) == 0 {
		return result
	}

	subject := tuple.FromCoreObjectAndRelation(req.Subject)
	for _, checkHint := range req.CheckHints {
		resourceID, ok := hints.AsCheckHintForComputedUserset(checkHint, req.ResourceRelation.Namespace, req.ResourceRelation.Relation, subject)
		if !ok {
			if req.OriginalRelationName != "" {
				resourceID, ok = hints.AsCheckHintForComputedUserset(checkHint, req.ResourceRelation.Namespace, req.OriginalRelationName, subject)
			}

			if !ok {
				continue
			}
		}

		if result.Resp.ResultsByResourceId == nil {
			result.Resp.ResultsByResourceId = make(map[string]*v1.ResourceCheckResult)
		}

		if _, ok := result.Resp.ResultsByResourceId[resourceID]; ok {
			return checkResultError(
				spiceerrors.MustBugf("check hint for resource ID %q, which already exists", resourceID),
				emptyMetadata,
			)
		}

		result.Resp.ResultsByResourceId[resourceID] = checkHint.Result
	}

	return result
}

func (cc *ConcurrentChecker) checkDirect(ctx context.Context, crc currentRequestContext, relation *core.Relation) CheckResult {
	ctx, span := tracer.Start(ctx, "checkDirect")
	defer span.End()

	// Build a filter for finding the direct relationships for the check. There are three
	// classes of relationships to be found:
	// 1) the target subject itself, if allowed on this relation
	// 2) the wildcard form of the target subject, if a wildcard is allowed on this relation
	// 3) Otherwise, any non-terminal (non-`...`) subjects, if allowed on this relation, to be
	//    redispatched outward
	totalNonTerminals := 0
	totalDirectSubjects := 0
	totalWildcardSubjects := 0

	defer func() {
		if totalNonTerminals > 0 {
			span.SetName("non terminal")
		} else if totalDirectSubjects > 0 {
			span.SetName("terminal")
		} else {
			span.SetName("wildcard subject")
		}
	}()
	log.Ctx(ctx).Trace().Object("direct", crc.parentReq).Send()
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(crc.parentReq.Revision)

	directSubjectsAndWildcardsWithoutCaveats := 0
	directSubjectsAndWildcardsWithoutExpiration := 0
	nonTerminalsWithoutCaveats := 0
	nonTerminalsWithoutExpiration := 0

	for _, allowedDirectRelation := range relation.GetTypeInformation().GetAllowedDirectRelations() {
		// If the namespace of the allowed direct relation matches the subject type, there are two
		// cases to optimize:
		// 1) Finding the target subject itself, as a direct lookup
		// 2) Finding a wildcard for the subject type+relation
		if allowedDirectRelation.GetNamespace() == crc.parentReq.Subject.Namespace {
			if allowedDirectRelation.GetPublicWildcard() != nil {
				totalWildcardSubjects++
			} else if allowedDirectRelation.GetRelation() == crc.parentReq.Subject.Relation {
				totalDirectSubjects++
			}

			if allowedDirectRelation.RequiredCaveat == nil {
				directSubjectsAndWildcardsWithoutCaveats++
			}

			if allowedDirectRelation.RequiredExpiration == nil {
				directSubjectsAndWildcardsWithoutExpiration++
			}
		}

		// If the relation found is not an ellipsis, then this is a nested relation that
		// might need to be followed, so indicate that such relationships should be returned
		//
		// TODO(jschorr): Use type information to *further* optimize this query around which nested
		// relations can reach the target subject type.
		if allowedDirectRelation.GetRelation() != tuple.Ellipsis {
			totalNonTerminals++
			if allowedDirectRelation.RequiredCaveat == nil {
				nonTerminalsWithoutCaveats++
			}
			if allowedDirectRelation.RequiredExpiration == nil {
				nonTerminalsWithoutExpiration++
			}
		}
	}

	nonTerminalsCanHaveCaveats := totalNonTerminals != nonTerminalsWithoutCaveats
	nonTerminalsCanHaveExpiration := totalNonTerminals != nonTerminalsWithoutExpiration
	hasNonTerminals := totalNonTerminals > 0

	foundResources := NewMembershipSet()

	// If the direct subject or a wildcard form can be found, issue a query for just that
	// subject.
	var queryCount float64
	defer func() {
		directDispatchQueryHistogram.Observe(queryCount)
	}()

	hasDirectSubject := totalDirectSubjects > 0
	hasWildcardSubject := totalWildcardSubjects > 0
	if hasDirectSubject || hasWildcardSubject {
		directSubjectOrWildcardCanHaveCaveats := directSubjectsAndWildcardsWithoutCaveats != (totalDirectSubjects + totalWildcardSubjects)
		directSubjectOrWildcardCanHaveExpiration := directSubjectsAndWildcardsWithoutExpiration != (totalDirectSubjects + totalWildcardSubjects)

		subjectSelectors := []datastore.SubjectsSelector{}

		if hasDirectSubject {
			subjectSelectors = append(subjectSelectors, datastore.SubjectsSelector{
				OptionalSubjectType: crc.parentReq.Subject.Namespace,
				OptionalSubjectIds:  []string{crc.parentReq.Subject.ObjectId},
				RelationFilter:      datastore.SubjectRelationFilter{}.WithRelation(crc.parentReq.Subject.Relation),
			})
		}

		if hasWildcardSubject {
			subjectSelectors = append(subjectSelectors, datastore.SubjectsSelector{
				OptionalSubjectType: crc.parentReq.Subject.Namespace,
				OptionalSubjectIds:  []string{tuple.PublicWildcard},
				RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
			})
		}

		filter := datastore.RelationshipsFilter{
			OptionalResourceType:      crc.parentReq.ResourceRelation.Namespace,
			OptionalResourceIds:       crc.filteredResourceIDs,
			OptionalResourceRelation:  crc.parentReq.ResourceRelation.Relation,
			OptionalSubjectsSelectors: subjectSelectors,
		}

		it, err := ds.QueryRelationships(ctx, filter,
			options.WithSkipCaveats(!directSubjectOrWildcardCanHaveCaveats),
			options.WithSkipExpiration(!directSubjectOrWildcardCanHaveExpiration),
			options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects),
		)
		if err != nil {
			return checkResultError(NewCheckFailureErr(err), emptyMetadata)
		}
		queryCount += 1.0

		// Find the matching subject(s).
		for rel, err := range it {
			if err != nil {
				return checkResultError(NewCheckFailureErr(err), emptyMetadata)
			}

			// If the subject of the relationship matches the target subject, then we've found
			// a result.
			foundResources.AddDirectMember(rel.Resource.ObjectID, rel.OptionalCaveat)
			if crc.resultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT && foundResources.HasDeterminedMember() {
				return checkResultsForMembership(foundResources, emptyMetadata)
			}
		}
	}

	// Filter down the resource IDs for further dispatch based on whether they exist as found
	// subjects in the existing membership set.
	furtherFilteredResourceIDs := make([]string, 0, len(crc.filteredResourceIDs)-foundResources.Size())
	for _, resourceID := range crc.filteredResourceIDs {
		if foundResources.HasConcreteResourceID(resourceID) {
			continue
		}

		furtherFilteredResourceIDs = append(furtherFilteredResourceIDs, resourceID)
	}

	// If there are no possible non-terminals, then the check is completed.
	if !hasNonTerminals || len(furtherFilteredResourceIDs) == 0 {
		return checkResultsForMembership(foundResources, emptyMetadata)
	}

	// Otherwise, for any remaining resource IDs, query for redispatch.
	filter := datastore.RelationshipsFilter{
		OptionalResourceType:     crc.parentReq.ResourceRelation.Namespace,
		OptionalResourceIds:      furtherFilteredResourceIDs,
		OptionalResourceRelation: crc.parentReq.ResourceRelation.Relation,
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				RelationFilter: datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
			},
		},
	}

	it, err := ds.QueryRelationships(ctx, filter,
		options.WithSkipCaveats(!nonTerminalsCanHaveCaveats),
		options.WithSkipExpiration(!nonTerminalsCanHaveExpiration),
		options.WithQueryShape(queryshape.CheckPermissionSelectIndirectSubjects),
	)
	if err != nil {
		return checkResultError(NewCheckFailureErr(err), emptyMetadata)
	}
	queryCount += 1.0

	// Build the set of subjects over which to dispatch, along with metadata for
	// mapping over caveats (if any).
	checksToDispatch := newCheckDispatchSet()
	for rel, err := range it {
		if err != nil {
			return checkResultError(NewCheckFailureErr(err), emptyMetadata)
		}
		checksToDispatch.addForRelationship(rel)
	}

	// Dispatch and map to the associated resource ID(s).
	toDispatch := checksToDispatch.dispatchChunks(crc.dispatchChunkSize)
	result := union(ctx, crc, toDispatch, func(ctx context.Context, crc currentRequestContext, dd checkDispatchChunk) CheckResult {
		// If there are caveats on any of the incoming relationships for the subjects to dispatch, then we must require all
		// results to be found, as we need to ensure that all caveats are used for building the final expression.
		resultsSetting := crc.resultsSetting
		if dd.hasIncomingCaveats {
			resultsSetting = v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS
		}

		childResult := cc.dispatch(ctx, crc, ValidatedCheckRequest{
			&v1.DispatchCheckRequest{
				ResourceRelation: dd.resourceType.ToCoreRR(),
				ResourceIds:      dd.resourceIds,
				Subject:          crc.parentReq.Subject,
				ResultsSetting:   resultsSetting,

				Metadata:   decrementDepth(crc.parentReq.Metadata),
				Debug:      crc.parentReq.Debug,
				CheckHints: crc.parentReq.CheckHints,
			},
			crc.parentReq.Revision,
			noOriginalRelation,
		})

		if childResult.Err != nil {
			return childResult
		}

		return mapFoundResources(childResult, dd.resourceType, checksToDispatch)
	}, cc.concurrencyLimit)

	return combineResultWithFoundResources(result, foundResources)
}

func mapFoundResources(result CheckResult, resourceType tuple.RelationReference, checksToDispatch *checkDispatchSet) CheckResult {
	// Map any resources found to the parent resource IDs.
	membershipSet := NewMembershipSet()
	for foundResourceID, result := range result.Resp.ResultsByResourceId {
		resourceIDAndCaveats := checksToDispatch.mappingsForSubject(resourceType.ObjectType, foundResourceID, resourceType.Relation)

		spiceerrors.DebugAssert(func() bool {
			return len(resourceIDAndCaveats) > 0
		}, "found resource ID without associated caveats")

		for _, riac := range resourceIDAndCaveats {
			membershipSet.AddMemberWithParentCaveat(riac.resourceID, result.Expression, riac.caveat)
		}
	}

	if membershipSet.IsEmpty() {
		return noMembersWithMetadata(result.Resp.Metadata)
	}

	return checkResultsForMembership(membershipSet, result.Resp.Metadata)
}

func (cc *ConcurrentChecker) checkUsersetRewrite(ctx context.Context, crc currentRequestContext, rewrite *core.UsersetRewrite) CheckResult {
	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		if len(rw.Union.Child) > 1 {
			var span trace.Span
			ctx, span = tracer.Start(ctx, "+")
			defer span.End()
		}
		return union(ctx, crc, rw.Union.Child, cc.runSetOperation, cc.concurrencyLimit)
	case *core.UsersetRewrite_Intersection:
		ctx, span := tracer.Start(ctx, "&")
		defer span.End()
		return all(ctx, crc, rw.Intersection.Child, cc.runSetOperation, cc.concurrencyLimit)
	case *core.UsersetRewrite_Exclusion:
		ctx, span := tracer.Start(ctx, "-")
		defer span.End()
		return difference(ctx, crc, rw.Exclusion.Child, cc.runSetOperation, cc.concurrencyLimit)
	default:
		return checkResultError(spiceerrors.MustBugf("unknown userset rewrite operator"), emptyMetadata)
	}
}

func (cc *ConcurrentChecker) dispatch(ctx context.Context, _ currentRequestContext, req ValidatedCheckRequest) CheckResult {
	log.Ctx(ctx).Trace().Object("dispatch", req).Send()
	result, err := cc.d.DispatchCheck(ctx, req.DispatchCheckRequest)
	return CheckResult{result, err}
}

func (cc *ConcurrentChecker) runSetOperation(ctx context.Context, crc currentRequestContext, childOneof *core.SetOperation_Child) CheckResult {
	switch child := childOneof.ChildType.(type) {
	case *core.SetOperation_Child_XThis:
		return checkResultError(spiceerrors.MustBugf("use of _this is unsupported; please rewrite your schema"), emptyMetadata)
	case *core.SetOperation_Child_ComputedUserset:
		return cc.checkComputedUserset(ctx, crc, child.ComputedUserset, nil, nil)
	case *core.SetOperation_Child_UsersetRewrite:
		return cc.checkUsersetRewrite(ctx, crc, child.UsersetRewrite)
	case *core.SetOperation_Child_TupleToUserset:
		return checkTupleToUserset(ctx, cc, crc, child.TupleToUserset)
	case *core.SetOperation_Child_FunctionedTupleToUserset:
		switch child.FunctionedTupleToUserset.Function {
		case core.FunctionedTupleToUserset_FUNCTION_ANY:
			return checkTupleToUserset(ctx, cc, crc, child.FunctionedTupleToUserset)

		case core.FunctionedTupleToUserset_FUNCTION_ALL:
			return checkIntersectionTupleToUserset(ctx, cc, crc, child.FunctionedTupleToUserset)

		default:
			return checkResultError(spiceerrors.MustBugf("unknown userset function `%s`", child.FunctionedTupleToUserset.Function), emptyMetadata)
		}

	case *core.SetOperation_Child_XNil:
		return noMembers()
	default:
		return checkResultError(spiceerrors.MustBugf("unknown set operation child `%T` in check", child), emptyMetadata)
	}
}

func (cc *ConcurrentChecker) checkComputedUserset(ctx context.Context, crc currentRequestContext, cu *core.ComputedUserset, rr *tuple.RelationReference, resourceIds []string) CheckResult {
	ctx, span := tracer.Start(ctx, cu.Relation)
	defer span.End()

	var startNamespace string
	var targetResourceIds []string
	if cu.Object == core.ComputedUserset_TUPLE_USERSET_OBJECT {
		if rr == nil || len(resourceIds) == 0 {
			return checkResultError(spiceerrors.MustBugf("computed userset for tupleset without tuples"), emptyMetadata)
		}

		startNamespace = rr.ObjectType
		targetResourceIds = resourceIds
	} else if cu.Object == core.ComputedUserset_TUPLE_OBJECT {
		if rr != nil {
			return checkResultError(spiceerrors.MustBugf("computed userset for tupleset with wrong object type"), emptyMetadata)
		}

		startNamespace = crc.parentReq.ResourceRelation.Namespace
		targetResourceIds = crc.filteredResourceIDs
	}

	targetRR := &core.RelationReference{
		Namespace: startNamespace,
		Relation:  cu.Relation,
	}

	// If we will be dispatching to the goal's ONR, then we know that the ONR is a member.
	membershipSet, updatedTargetResourceIds := filterForFoundMemberResource(targetRR, targetResourceIds, crc.parentReq.Subject)
	if (membershipSet.HasDeterminedMember() && crc.resultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT) || len(updatedTargetResourceIds) == 0 {
		return checkResultsForMembership(membershipSet, emptyMetadata)
	}

	// Check if the target relation exists. If not, return nothing. This is only necessary
	// for TTU-based computed usersets, as directly computed ones reference relations within
	// the same namespace as the caller, and thus must be fully typed checked.
	if cu.Object == core.ComputedUserset_TUPLE_USERSET_OBJECT {
		ds := datastoremw.MustFromContext(ctx).SnapshotReader(crc.parentReq.Revision)
		err := namespace.CheckNamespaceAndRelation(ctx, targetRR.Namespace, targetRR.Relation, true, ds)
		if err != nil {
			if errors.As(err, &namespace.RelationNotFoundError{}) {
				return noMembers()
			}

			return checkResultError(err, emptyMetadata)
		}
	}

	result := cc.dispatch(ctx, crc, ValidatedCheckRequest{
		&v1.DispatchCheckRequest{
			ResourceRelation: targetRR,
			ResourceIds:      updatedTargetResourceIds,
			Subject:          crc.parentReq.Subject,
			ResultsSetting:   crc.resultsSetting,
			Metadata:         decrementDepth(crc.parentReq.Metadata),
			Debug:            crc.parentReq.Debug,
			CheckHints:       crc.parentReq.CheckHints,
		},
		crc.parentReq.Revision,
		noOriginalRelation,
	})
	return combineResultWithFoundResources(result, membershipSet)
}

type Traits struct {
	HasCaveats    bool
	HasExpiration bool
}

// TraitsForArrowRelation returns traits such as HasCaveats and HasExpiration if *any* of the subject
// types of the given relation support caveats or expiration.
func TraitsForArrowRelation(ctx context.Context, reader datastore.Reader, namespaceName string, relationName string) (Traits, error) {
	// TODO(jschorr): Change to use the type system once we wire it through Check dispatch.
	nsDef, _, err := reader.ReadNamespaceByName(ctx, namespaceName)
	if err != nil {
		return Traits{}, err
	}

	var relation *core.Relation
	for _, rel := range nsDef.Relation {
		if rel.Name == relationName {
			relation = rel
			break
		}
	}

	if relation == nil || relation.TypeInformation == nil {
		return Traits{}, fmt.Errorf("relation %q not found", relationName)
	}

	hasCaveats := false
	hasExpiration := false

	for _, allowedDirectRelation := range relation.TypeInformation.GetAllowedDirectRelations() {
		if allowedDirectRelation.RequiredCaveat != nil {
			hasCaveats = true
		}

		if allowedDirectRelation.RequiredExpiration != nil {
			hasExpiration = true
		}
	}

	return Traits{
		HasCaveats:    hasCaveats,
		HasExpiration: hasExpiration,
	}, nil
}

func queryOptionsForArrowRelation(ctx context.Context, ds datastore.Reader, namespaceName string, relationName string) ([]options.QueryOptionsOption, error) {
	traits, err := TraitsForArrowRelation(ctx, ds, namespaceName, relationName)
	if err != nil {
		return nil, err
	}

	opts := []options.QueryOptionsOption{}
	if !traits.HasCaveats {
		opts = append(opts, options.WithSkipCaveats(true))
	}

	if !traits.HasExpiration {
		opts = append(opts, options.WithSkipExpiration(true))
	}

	return opts, nil
}

func filterForFoundMemberResource(resourceRelation *core.RelationReference, resourceIds []string, subject *core.ObjectAndRelation) (*MembershipSet, []string) {
	if resourceRelation.Namespace != subject.Namespace || resourceRelation.Relation != subject.Relation {
		return nil, resourceIds
	}

	for index, resourceID := range resourceIds {
		if subject.ObjectId == resourceID {
			membershipSet := NewMembershipSet()
			membershipSet.AddDirectMember(resourceID, nil)
			return membershipSet, removeIndexFromSlice(resourceIds, index)
		}
	}

	return nil, resourceIds
}

func removeIndexFromSlice[T any](s []T, index int) []T {
	cpy := make([]T, 0, len(s)-1)
	cpy = append(cpy, s[:index]...)
	return append(cpy, s[index+1:]...)
}

type relation interface {
	GetRelation() string
}

type ttu[T relation] interface {
	GetComputedUserset() *core.ComputedUserset
	GetTupleset() T
}

type checkResultWithType struct {
	CheckResult

	relationType tuple.RelationReference
}

func checkIntersectionTupleToUserset(
	ctx context.Context,
	cc *ConcurrentChecker,
	crc currentRequestContext,
	ttu *core.FunctionedTupleToUserset,
) CheckResult {
	// TODO(jschorr): use check hints here
	ctx, span := tracer.Start(ctx, ttu.GetTupleset().GetRelation()+"-(all)->"+ttu.GetComputedUserset().Relation)
	defer span.End()

	// Query for the subjects over which to walk the TTU.
	log.Ctx(ctx).Trace().Object("intersectionttu", crc.parentReq).Send()
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(crc.parentReq.Revision)
	queryOpts, err := queryOptionsForArrowRelation(ctx, ds, crc.parentReq.ResourceRelation.Namespace, ttu.GetTupleset().GetRelation())
	if err != nil {
		return checkResultError(NewCheckFailureErr(err), emptyMetadata)
	}

	it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     crc.parentReq.ResourceRelation.Namespace,
		OptionalResourceIds:      crc.filteredResourceIDs,
		OptionalResourceRelation: ttu.GetTupleset().GetRelation(),
	}, queryOpts...)
	if err != nil {
		return checkResultError(NewCheckFailureErr(err), emptyMetadata)
	}

	checksToDispatch := newCheckDispatchSet()
	subjectsByResourceID := mapz.NewMultiMap[string, tuple.ObjectAndRelation]()
	for rel, err := range it {
		if err != nil {
			return checkResultError(NewCheckFailureErr(err), emptyMetadata)
		}

		checksToDispatch.addForRelationship(rel)
		subjectsByResourceID.Add(rel.Resource.ObjectID, rel.Subject)
	}

	// Convert the subjects into batched requests.
	toDispatch := checksToDispatch.dispatchChunks(crc.dispatchChunkSize)
	if len(toDispatch) == 0 {
		return noMembers()
	}

	// Run the dispatch for all the chunks. Unlike a standard TTU, we do *not* perform mapping here,
	// as we need to access the results on a per subject basis. Instead, we keep each result and map
	// by the relation type of the dispatched subject.
	chunkResults, err := run(
		ctx,
		currentRequestContext{
			parentReq:           crc.parentReq,
			filteredResourceIDs: crc.filteredResourceIDs,
			resultsSetting:      v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
			dispatchChunkSize:   crc.dispatchChunkSize,
		},
		toDispatch,
		func(ctx context.Context, crc currentRequestContext, dd checkDispatchChunk) checkResultWithType {
			resourceType := dd.resourceType
			childResult := cc.checkComputedUserset(ctx, crc, ttu.GetComputedUserset(), &resourceType, dd.resourceIds)
			return checkResultWithType{
				CheckResult:  childResult,
				relationType: dd.resourceType,
			}
		},
		cc.concurrencyLimit,
	)
	if err != nil {
		return checkResultError(err, emptyMetadata)
	}

	// Create a membership set per-subject-type, representing the membership for each of the dispatched subjects.
	resultsByDispatchedSubject := map[tuple.RelationReference]*MembershipSet{}
	combinedMetadata := emptyMetadata
	for _, result := range chunkResults {
		if result.Err != nil {
			return checkResultError(result.Err, emptyMetadata)
		}

		if _, ok := resultsByDispatchedSubject[result.relationType]; !ok {
			resultsByDispatchedSubject[result.relationType] = NewMembershipSet()
		}

		resultsByDispatchedSubject[result.relationType].UnionWith(result.Resp.ResultsByResourceId)
		combinedMetadata = combineResponseMetadata(ctx, combinedMetadata, result.Resp.Metadata)
	}

	// For each resource ID, check that there exist some sort of permission for *each* subject. If not, then the
	// intersection for that resource fails. If all subjects have some sort of permission, then the resource ID is
	// a member, perhaps caveated.
	resourcesFound := NewMembershipSet()
	for _, resourceID := range subjectsByResourceID.Keys() {
		subjects, _ := subjectsByResourceID.Get(resourceID)
		if len(subjects) == 0 {
			return checkResultError(spiceerrors.MustBugf("no subjects found for resource ID %s", resourceID), emptyMetadata)
		}

		hasAllSubjects := true
		caveats := make([]*core.CaveatExpression, 0, len(subjects))

		// Check each of the subjects found for the resource ID and ensure that membership (at least caveated)
		// was found for each. If any are not found, then the resource ID is not a member.
		// We also collect up the caveats for each subject, as they will be added to the final result.
		for _, subject := range subjects {
			subjectTypeKey := subject.RelationReference()
			results, ok := resultsByDispatchedSubject[subjectTypeKey]
			if !ok {
				hasAllSubjects = false
				break
			}

			hasMembership, caveat := results.GetResourceID(subject.ObjectID)
			if !hasMembership {
				hasAllSubjects = false
				break
			}

			if caveat != nil {
				caveats = append(caveats, caveat)
			}

			// Add any caveats on the subject from the starting relationship(s) as well.
			resourceIDAndCaveats := checksToDispatch.mappingsForSubject(subject.ObjectType, subject.ObjectID, subject.Relation)
			for _, riac := range resourceIDAndCaveats {
				if riac.caveat != nil {
					caveats = append(caveats, wrapCaveat(riac.caveat))
				}
			}
		}

		if !hasAllSubjects {
			continue
		}

		// Add the member to the membership set, with the caveats for each (if any).
		resourcesFound.AddMemberWithOptionalCaveats(resourceID, caveats)
	}

	return checkResultsForMembership(resourcesFound, combinedMetadata)
}

func checkTupleToUserset[T relation](
	ctx context.Context,
	cc *ConcurrentChecker,
	crc currentRequestContext,
	ttu ttu[T],
) CheckResult {
	filteredResourceIDs := crc.filteredResourceIDs
	hintsToReturn := make(map[string]*v1.ResourceCheckResult, len(crc.parentReq.CheckHints))
	if len(crc.parentReq.CheckHints) > 0 {
		filteredResourcesIdsSet := mapz.NewSet(crc.filteredResourceIDs...)

		for _, checkHint := range crc.parentReq.CheckHints {
			resourceID, ok := hints.AsCheckHintForArrow(
				checkHint,
				crc.parentReq.ResourceRelation.Namespace,
				ttu.GetTupleset().GetRelation(),
				ttu.GetComputedUserset().Relation,
				tuple.FromCoreObjectAndRelation(crc.parentReq.Subject),
			)
			if !ok {
				continue
			}

			filteredResourcesIdsSet.Delete(resourceID)
			hintsToReturn[resourceID] = checkHint.Result
		}

		filteredResourceIDs = filteredResourcesIdsSet.AsSlice()
	}

	if len(filteredResourceIDs) == 0 {
		return combineWithComputedHints(noMembers(), hintsToReturn)
	}

	ctx, span := tracer.Start(ctx, ttu.GetTupleset().GetRelation()+"->"+ttu.GetComputedUserset().Relation)
	defer span.End()

	log.Ctx(ctx).Trace().Object("ttu", crc.parentReq).Send()
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(crc.parentReq.Revision)

	queryOpts, err := queryOptionsForArrowRelation(ctx, ds, crc.parentReq.ResourceRelation.Namespace, ttu.GetTupleset().GetRelation())
	if err != nil {
		return checkResultError(NewCheckFailureErr(err), emptyMetadata)
	}

	it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     crc.parentReq.ResourceRelation.Namespace,
		OptionalResourceIds:      filteredResourceIDs,
		OptionalResourceRelation: ttu.GetTupleset().GetRelation(),
	}, queryOpts...)
	if err != nil {
		return checkResultError(NewCheckFailureErr(err), emptyMetadata)
	}

	checksToDispatch := newCheckDispatchSet()
	for rel, err := range it {
		if err != nil {
			return checkResultError(NewCheckFailureErr(err), emptyMetadata)
		}
		checksToDispatch.addForRelationship(rel)
	}

	toDispatch := checksToDispatch.dispatchChunks(crc.dispatchChunkSize)
	return combineWithComputedHints(union(
		ctx,
		crc,
		toDispatch,
		func(ctx context.Context, crc currentRequestContext, dd checkDispatchChunk) CheckResult {
			resourceType := dd.resourceType
			childResult := cc.checkComputedUserset(ctx, crc, ttu.GetComputedUserset(), &resourceType, dd.resourceIds)
			if childResult.Err != nil {
				return childResult
			}

			return mapFoundResources(childResult, dd.resourceType, checksToDispatch)
		},
		cc.concurrencyLimit,
	), hintsToReturn)
}

func withDistinctMetadata(ctx context.Context, result CheckResult) CheckResult {
	// NOTE: This is necessary to ensure unique debug information on the request and that debug
	// information from the child metadata is *not* copied over.
	clonedResp := result.Resp.CloneVT()
	clonedResp.Metadata = combineResponseMetadata(ctx, emptyMetadata, clonedResp.Metadata)
	return CheckResult{
		Resp: clonedResp,
		Err:  result.Err,
	}
}

// run runs all the children in parallel and returns the full set of results.
func run[T any, R withError](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) R,
	concurrencyLimit uint16,
) ([]R, error) {
	if len(children) == 0 {
		return nil, nil
	}

	if len(children) == 1 {
		return []R{handler(ctx, crc, children[0])}, nil
	}

	resultChan := make(chan R, len(children))
	childCtx, cancelFn := context.WithCancel(ctx)
	dispatchAllAsync(childCtx, crc, children, handler, resultChan, concurrencyLimit)
	defer cancelFn()

	results := make([]R, 0, len(children))
	for i := 0; i < len(children); i++ {
		select {
		case result := <-resultChan:
			results = append(results, result)

		case <-ctx.Done():
			log.Ctx(ctx).Trace().Msg("anyCanceled")
			return nil, ctx.Err()
		}
	}

	return results, nil
}

// union returns whether any one of the lazy checks pass, and is used for union.
func union[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
	concurrencyLimit uint16,
) CheckResult {
	if len(children) == 0 {
		return noMembers()
	}

	if len(children) == 1 {
		return withDistinctMetadata(ctx, handler(ctx, crc, children[0]))
	}

	resultChan := make(chan CheckResult, len(children))
	childCtx, cancelFn := context.WithCancel(ctx)
	dispatchAllAsync(childCtx, crc, children, handler, resultChan, concurrencyLimit)
	defer cancelFn()

	responseMetadata := emptyMetadata
	membershipSet := NewMembershipSet()

	for i := 0; i < len(children); i++ {
		select {
		case result := <-resultChan:
			log.Ctx(ctx).Trace().Object("anyResult", result.Resp).Send()
			responseMetadata = combineResponseMetadata(ctx, responseMetadata, result.Resp.Metadata)
			if result.Err != nil {
				return checkResultError(result.Err, responseMetadata)
			}

			membershipSet.UnionWith(result.Resp.ResultsByResourceId)
			if membershipSet.HasDeterminedMember() && crc.resultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT {
				return checkResultsForMembership(membershipSet, responseMetadata)
			}

		case <-ctx.Done():
			log.Ctx(ctx).Trace().Msg("anyCanceled")
			return checkResultError(context.Canceled, responseMetadata)
		}
	}

	return checkResultsForMembership(membershipSet, responseMetadata)
}

// all returns whether all of the lazy checks pass, and is used for intersection.
func all[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
	concurrencyLimit uint16,
) CheckResult {
	if len(children) == 0 {
		return noMembers()
	}

	if len(children) == 1 {
		return withDistinctMetadata(ctx, handler(ctx, crc, children[0]))
	}

	responseMetadata := emptyMetadata

	resultChan := make(chan CheckResult, len(children))
	childCtx, cancelFn := context.WithCancel(ctx)
	dispatchAllAsync(childCtx, currentRequestContext{
		parentReq:           crc.parentReq,
		filteredResourceIDs: crc.filteredResourceIDs,
		resultsSetting:      v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
		dispatchChunkSize:   crc.dispatchChunkSize,
	}, children, handler, resultChan, concurrencyLimit)
	defer cancelFn()

	var membershipSet *MembershipSet
	for i := 0; i < len(children); i++ {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(ctx, responseMetadata, result.Resp.Metadata)
			if result.Err != nil {
				return checkResultError(result.Err, responseMetadata)
			}

			if membershipSet == nil {
				membershipSet = NewMembershipSet()
				membershipSet.UnionWith(result.Resp.ResultsByResourceId)
			} else {
				membershipSet.IntersectWith(result.Resp.ResultsByResourceId)
			}

			if membershipSet.IsEmpty() {
				return noMembersWithMetadata(responseMetadata)
			}
		case <-ctx.Done():
			return checkResultError(context.Canceled, responseMetadata)
		}
	}

	return checkResultsForMembership(membershipSet, responseMetadata)
}

// difference returns whether the first lazy check passes and none of the supsequent checks pass.
func difference[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
	concurrencyLimit uint16,
) CheckResult {
	if len(children) == 0 {
		return noMembers()
	}

	if len(children) == 1 {
		return checkResultError(spiceerrors.MustBugf("difference requires more than a single child"), emptyMetadata)
	}

	childCtx, cancelFn := context.WithCancel(ctx)
	baseChan := make(chan CheckResult, 1)
	othersChan := make(chan CheckResult, len(children)-1)

	go func() {
		result := handler(childCtx, currentRequestContext{
			parentReq:           crc.parentReq,
			filteredResourceIDs: crc.filteredResourceIDs,
			resultsSetting:      v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
			dispatchChunkSize:   crc.dispatchChunkSize,
		}, children[0])
		baseChan <- result
	}()

	dispatchAllAsync(childCtx, currentRequestContext{
		parentReq:           crc.parentReq,
		filteredResourceIDs: crc.filteredResourceIDs,
		resultsSetting:      v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
		dispatchChunkSize:   crc.dispatchChunkSize,
	}, children[1:], handler, othersChan, concurrencyLimit-1)
	defer cancelFn()

	responseMetadata := emptyMetadata
	membershipSet := NewMembershipSet()

	// Wait for the base set to return.
	select {
	case base := <-baseChan:
		responseMetadata = combineResponseMetadata(ctx, responseMetadata, base.Resp.Metadata)

		if base.Err != nil {
			return checkResultError(base.Err, responseMetadata)
		}

		membershipSet.UnionWith(base.Resp.ResultsByResourceId)
		if membershipSet.IsEmpty() {
			return noMembersWithMetadata(responseMetadata)
		}

	case <-ctx.Done():
		return checkResultError(context.Canceled, responseMetadata)
	}

	// Subtract the remaining sets.
	for i := 1; i < len(children); i++ {
		select {
		case sub := <-othersChan:
			responseMetadata = combineResponseMetadata(ctx, responseMetadata, sub.Resp.Metadata)

			if sub.Err != nil {
				return checkResultError(sub.Err, responseMetadata)
			}

			membershipSet.Subtract(sub.Resp.ResultsByResourceId)
			if membershipSet.IsEmpty() {
				return noMembersWithMetadata(responseMetadata)
			}

		case <-ctx.Done():
			return checkResultError(context.Canceled, responseMetadata)
		}
	}

	return checkResultsForMembership(membershipSet, responseMetadata)
}

type withError interface {
	ResultError() error
}

func dispatchAllAsync[T any, R withError](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) R,
	resultChan chan<- R,
	concurrencyLimit uint16,
) {
	tr := taskrunner.NewPreloadedTaskRunner(ctx, concurrencyLimit, len(children))
	for _, currentChild := range children {
		currentChild := currentChild
		tr.Add(func(ctx context.Context) error {
			result := handler(ctx, crc, currentChild)
			resultChan <- result
			return result.ResultError()
		})
	}

	tr.Start()
}

func noMembers() CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata: emptyMetadata,
		},
		nil,
	}
}

func noMembersWithMetadata(metadata *v1.ResponseMeta) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata: metadata,
		},
		nil,
	}
}

func checkResultsForMembership(foundMembership *MembershipSet, subProblemMetadata *v1.ResponseMeta) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata:            ensureMetadata(subProblemMetadata),
			ResultsByResourceId: foundMembership.AsCheckResultsMap(),
		},
		nil,
	}
}

func checkResultError(err error, subProblemMetadata *v1.ResponseMeta) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata: ensureMetadata(subProblemMetadata),
		},
		err,
	}
}

func combineResultWithFoundResources(result CheckResult, foundResources *MembershipSet) CheckResult {
	if result.Err != nil {
		return result
	}

	if foundResources.IsEmpty() {
		return result
	}

	foundResources.UnionWith(result.Resp.ResultsByResourceId)
	return CheckResult{
		Resp: &v1.DispatchCheckResponse{
			ResultsByResourceId: foundResources.AsCheckResultsMap(),
			Metadata:            result.Resp.Metadata,
		},
		Err: result.Err,
	}
}

func combineResponseMetadata(ctx context.Context, existing *v1.ResponseMeta, responseMetadata *v1.ResponseMeta) *v1.ResponseMeta {
	combined := &v1.ResponseMeta{
		DispatchCount:       existing.DispatchCount + responseMetadata.DispatchCount,
		DepthRequired:       max(existing.DepthRequired, responseMetadata.DepthRequired),
		CachedDispatchCount: existing.CachedDispatchCount + responseMetadata.CachedDispatchCount,
	}

	if existing.DebugInfo == nil && responseMetadata.DebugInfo == nil {
		return combined
	}

	nodeID, err := nodeid.FromContext(ctx)
	if err != nil {
		log.Err(err).Msg("failed to get nodeID from context")
	}

	debugInfo := &v1.DebugInformation{
		Check: &v1.CheckDebugTrace{
			TraceId:  NewTraceID(),
			SourceId: nodeID,
		},
	}

	if existing.DebugInfo != nil {
		if existing.DebugInfo.Check.Request != nil {
			debugInfo.Check.SubProblems = append(debugInfo.Check.SubProblems, existing.DebugInfo.Check)
		} else {
			debugInfo.Check.SubProblems = append(debugInfo.Check.SubProblems, existing.DebugInfo.Check.SubProblems...)
		}
	}

	if responseMetadata.DebugInfo != nil {
		if responseMetadata.DebugInfo.Check.Request != nil {
			debugInfo.Check.SubProblems = append(debugInfo.Check.SubProblems, responseMetadata.DebugInfo.Check)
		} else {
			debugInfo.Check.SubProblems = append(debugInfo.Check.SubProblems, responseMetadata.DebugInfo.Check.SubProblems...)
		}
	}

	combined.DebugInfo = debugInfo
	return combined
}
