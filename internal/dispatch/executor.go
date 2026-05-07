package dispatch

import (
	"context"
	"slices"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/datalayer"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
)

// DispatchExecutor is an Executor that selectively dispatches sub-operations
// through the dispatch infrastructure at alias iterator boundaries.
// For non-alias iterators, it delegates locally like LocalExecutor.
type DispatchExecutor struct {
	dispatcher        Dispatcher
	planContext       *v1.PlanContext
	dispatchChunkSize uint16
}

var _ query.Executor = &DispatchExecutor{}

// defaultDispatchChunkSize is the fallback batch size for CheckMany RPCs when
// the executor was constructed with chunkSize == 0.
const defaultDispatchChunkSize = 100

// recursiveKey uniquely identifies a recursion pair by definition and relation name.
type recursiveKey struct {
	definitionName string
	relationName   string
}

// containsUnmatchedRecursiveSentinel walks the entire iterator tree and checks whether
// there is a RecursiveSentinelIterator whose (definitionName, relationName) pair has no
// matching RecursiveIterator anywhere in the tree. A sentinel is "managed" (safe to
// dispatch) only when a RecursiveIterator with the same key exists to handle its
// collection context. This handles the double-recursion case where a RecursiveIterator
// for one recursion pair contains a sentinel for a different pair in its subtree.
func containsUnmatchedRecursiveSentinel(it query.Iterator) bool {
	// Collect all keys from RecursiveIterator and RecursiveSentinelIterator nodes.
	var iteratorKeys map[recursiveKey]struct{}
	var sentinelKeys map[recursiveKey]struct{}

	var walk func(query.Iterator)
	walk = func(sub query.Iterator) {
		if ri, ok := sub.(*query.RecursiveIterator); ok {
			if iteratorKeys == nil {
				iteratorKeys = make(map[recursiveKey]struct{})
			}
			iteratorKeys[recursiveKey{ri.DefinitionName(), ri.RelationName()}] = struct{}{}
		}
		if si, ok := sub.(*query.RecursiveSentinelIterator); ok {
			if sentinelKeys == nil {
				sentinelKeys = make(map[recursiveKey]struct{})
			}
			sentinelKeys[recursiveKey{si.DefinitionName(), si.RelationName()}] = struct{}{}
		}
		for _, child := range sub.Subiterators() {
			walk(child)
		}
	}
	walk(it)

	// Every sentinel must have a matching iterator; an unmatched sentinel means
	// its collection context won't be established and dispatch would break.
	for key := range sentinelKeys {
		if _, managed := iteratorKeys[key]; !managed {
			return true
		}
	}
	return false
}

// NewDispatchExecutor creates a new DispatchExecutor that dispatches alias
// iterator operations through the given Dispatcher chain. dispatchChunkSize
// caps the number of inputs per CheckMany RPC; if zero, defaults to 100.
func NewDispatchExecutor(dispatcher Dispatcher, planContext *v1.PlanContext, dispatchChunkSize uint16) *DispatchExecutor {
	if dispatchChunkSize == 0 {
		dispatchChunkSize = defaultDispatchChunkSize
	}
	return &DispatchExecutor{
		dispatcher:        dispatcher,
		planContext:       planContext,
		dispatchChunkSize: dispatchChunkSize,
	}
}

// shouldDispatchAlias reports whether the iterator is an AliasIterator that the
// executor should ship over RPC, vs. running locally. Two reasons to refuse:
//   - the alias contains a sentinel whose matching RecursiveIterator lives
//     outside its own subtree (sentinel-based recursion guard, ancestor case).
//   - the alias's canonical key is already in the active dispatch chain
//     (in-progress guard, cross-relation cycle case). Without this, a standalone
//     plan that re-expands the same key in a sibling subtree produces an
//     infinite dispatch loop.
func (e *DispatchExecutor) shouldDispatchAlias(it query.Iterator) (string, bool) {
	alias, ok := it.(*query.AliasIterator)
	if !ok {
		return "", false
	}
	if containsUnmatchedRecursiveSentinel(it) {
		return "", false
	}
	key := alias.DefinitionName() + "#" + alias.Relation()
	for _, k := range e.planContext.GetInProgressKeys() {
		if k == key {
			return "", false
		}
	}
	return key, true
}

func (e *DispatchExecutor) Check(ctx *query.Context, it query.Iterator, resource query.Object, subject query.ObjectAndRelation) (*query.Path, error) {
	if _, ok := e.shouldDispatchAlias(it); ok {
		return e.dispatchCheck(ctx, it, resource, subject)
	}
	return it.CheckImpl(ctx, resource, subject)
}

func (e *DispatchExecutor) CheckManySubjects(ctx *query.Context, it query.Iterator, resource query.Object, subjects []query.ObjectAndRelation) ([]*query.Path, error) {
	if _, ok := e.shouldDispatchAlias(it); !ok {
		out := make([]*query.Path, len(subjects))
		for i, s := range subjects {
			p, err := it.CheckImpl(ctx, resource, s)
			if err != nil {
				return nil, err
			}
			out[i] = p
		}
		return out, nil
	}
	return e.dispatchCheckManySubjects(ctx, it, resource, subjects)
}

func (e *DispatchExecutor) CheckManyResources(ctx *query.Context, it query.Iterator, resources []query.Object, subject query.ObjectAndRelation) ([]*query.Path, error) {
	if _, ok := e.shouldDispatchAlias(it); !ok {
		out := make([]*query.Path, len(resources))
		for i, r := range resources {
			p, err := it.CheckImpl(ctx, r, subject)
			if err != nil {
				return nil, err
			}
			out[i] = p
		}
		return out, nil
	}
	return e.dispatchCheckManyResources(ctx, it, resources, subject)
}

func (e *DispatchExecutor) IterSubjects(ctx *query.Context, it query.Iterator, resource query.Object, filterSubjectType query.ObjectType) (query.PathSeq, error) {
	if _, ok := e.shouldDispatchAlias(it); ok {
		return e.dispatchIterSubjects(ctx, it, resource, filterSubjectType)
	}
	pathSeq, err := it.IterSubjectsImpl(ctx, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}
	return query.FilterSubjectsByType(pathSeq, filterSubjectType), nil
}

func (e *DispatchExecutor) IterResources(ctx *query.Context, it query.Iterator, subject query.ObjectAndRelation, filterResourceType query.ObjectType) (query.PathSeq, error) {
	if _, ok := e.shouldDispatchAlias(it); ok {
		return e.dispatchIterResources(ctx, it, subject, filterResourceType)
	}
	pathSeq, err := it.IterResourcesImpl(ctx, subject, filterResourceType)
	if err != nil {
		return nil, err
	}
	return query.FilterResourcesByType(pathSeq, filterResourceType), nil
}

func (e *DispatchExecutor) dispatchCheck(ctx *query.Context, it query.Iterator, resource query.Object, subject query.ObjectAndRelation) (*query.Path, error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req := e.buildRequest(ctx, v1.PlanOperation_PLAN_OPERATION_CHECK, it, resource, subject)
	stream := NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](subCtx)
	if err := e.dispatcher.DispatchQueryPlan(req, stream); err != nil {
		return nil, err
	}

	for _, resp := range stream.Results() {
		for _, rp := range resp.Paths {
			path := resultPathToQueryPath(rp)
			return path, nil
		}
	}
	return nil, nil
}

func (e *DispatchExecutor) dispatchIterSubjects(ctx *query.Context, it query.Iterator, resource query.Object, filterSubjectType query.ObjectType) (query.PathSeq, error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// For LookupSubjects the proto Subject field carries the *filter* subject
	// type+relation rather than a real subject ONR — the receiver needs the
	// filter to apply the same reachability/optimizer decisions the sender
	// would have applied. The actual receiver-side iteration uses NoObjectFilter
	// since the sender filters the returned paths itself.
	req := e.buildRequest(ctx, v1.PlanOperation_PLAN_OPERATION_LOOKUP_SUBJECTS, it, resource, query.ObjectAndRelation{
		ObjectType: filterSubjectType.Type,
		Relation:   filterSubjectType.Subrelation,
	})
	stream := NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](subCtx)
	if err := e.dispatcher.DispatchQueryPlan(req, stream); err != nil {
		return nil, err
	}

	paths := collectPathsFromResponses(stream.Results())
	return query.FilterSubjectsByType(query.PathSeqFromSlice(paths), filterSubjectType), nil
}

func (e *DispatchExecutor) dispatchIterResources(ctx *query.Context, it query.Iterator, subject query.ObjectAndRelation, filterResourceType query.ObjectType) (query.PathSeq, error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req := e.buildRequest(ctx, v1.PlanOperation_PLAN_OPERATION_LOOKUP_RESOURCES, it, query.Object{
		ObjectType: subject.ObjectType,
		ObjectID:   subject.ObjectID,
	}, subject)
	stream := NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](subCtx)
	if err := e.dispatcher.DispatchQueryPlan(req, stream); err != nil {
		return nil, err
	}

	paths := collectPathsFromResponses(stream.Results())
	return query.FilterResourcesByType(query.PathSeqFromSlice(paths), filterResourceType), nil
}

func (e *DispatchExecutor) dispatchCheckManySubjects(ctx *query.Context, it query.Iterator, resource query.Object, subjects []query.ObjectAndRelation) ([]*query.Path, error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	out := make([]*query.Path, len(subjects))
	idx := 0
	for chunk := range slices.Chunk(subjects, int(e.dispatchChunkSize)) {
		req := e.buildManyRequest(ctx, v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS, it, resource, query.ObjectAndRelation{}, nil, chunk)
		stream := NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](subCtx)
		if err := e.dispatcher.DispatchQueryPlan(req, stream); err != nil {
			return nil, err
		}
		// Index responses by subject ONR for back-mapping.
		bySubject := make(map[query.ObjectAndRelation]*query.Path)
		for _, resp := range stream.Results() {
			for _, rp := range resp.Paths {
				p := resultPathToQueryPath(rp)
				bySubject[p.Subject] = p
			}
		}
		for _, subject := range chunk {
			out[idx] = bySubject[subject]
			idx++
		}
	}
	return out, nil
}

func (e *DispatchExecutor) dispatchCheckManyResources(ctx *query.Context, it query.Iterator, resources []query.Object, subject query.ObjectAndRelation) ([]*query.Path, error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	out := make([]*query.Path, len(resources))
	idx := 0
	for chunk := range slices.Chunk(resources, int(e.dispatchChunkSize)) {
		req := e.buildManyRequest(ctx, v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES, it, query.Object{}, subject, chunk, nil)
		stream := NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](subCtx)
		if err := e.dispatcher.DispatchQueryPlan(req, stream); err != nil {
			return nil, err
		}
		// Index responses by resource Object for back-mapping.
		byResource := make(map[query.Object]*query.Path)
		for _, resp := range stream.Results() {
			for _, rp := range resp.Paths {
				p := resultPathToQueryPath(rp)
				byResource[p.Resource] = p
			}
		}
		for _, resource := range chunk {
			out[idx] = byResource[resource]
			idx++
		}
	}
	return out, nil
}

// buildManyRequest constructs a DispatchQueryPlanRequest for CheckMany operations.
// Exactly one of (manyResources, manySubjects) is non-nil; the corresponding
// singular field (resource for CHECK_MANY_RESOURCES, subject for CHECK_MANY_SUBJECTS)
// must be left zero — the receiver inspects `many` for those operations.
func (e *DispatchExecutor) buildManyRequest(ctx *query.Context, op v1.PlanOperation, it query.Iterator, resource query.Object, subject query.ObjectAndRelation, manyResources []query.Object, manySubjects []query.ObjectAndRelation) *v1.DispatchQueryPlanRequest {
	alias := it.(*query.AliasIterator)
	key := alias.DefinitionName() + "#" + alias.Relation()
	req := &v1.DispatchQueryPlanRequest{
		Operation:    op,
		CanonicalKey: key,
		Resource: &core.ObjectAndRelation{
			Namespace: resource.ObjectType,
			ObjectId:  resource.ObjectID,
		},
		Subject: &core.ObjectAndRelation{
			Namespace: subject.ObjectType,
			ObjectId:  subject.ObjectID,
			Relation:  subject.Relation,
		},
		PlanContext: planContextForDispatch(e.planContext, key, ctx.TopLevelOperation),
	}
	if len(manyResources) > 0 {
		req.Many = make([]*core.ObjectAndRelation, len(manyResources))
		for i, r := range manyResources {
			req.Many[i] = &core.ObjectAndRelation{
				Namespace: r.ObjectType,
				ObjectId:  r.ObjectID,
			}
		}
	}
	if len(manySubjects) > 0 {
		req.Many = make([]*core.ObjectAndRelation, len(manySubjects))
		for i, s := range manySubjects {
			req.Many[i] = &core.ObjectAndRelation{
				Namespace: s.ObjectType,
				ObjectId:  s.ObjectID,
				Relation:  s.Relation,
			}
		}
	}
	return req
}

func (e *DispatchExecutor) buildRequest(ctx *query.Context, op v1.PlanOperation, it query.Iterator, resource query.Object, subject query.ObjectAndRelation) *v1.DispatchQueryPlanRequest {
	// dispatch{Check,IterSubjects,IterResources} only enter buildRequest when
	// it is a *query.AliasIterator, so the type assertion is safe. The dispatch
	// boundary is always the (definition, relation) the alias represents; we
	// encode that as "definition#relation" in the canonical_key field.
	alias := it.(*query.AliasIterator)
	key := alias.DefinitionName() + "#" + alias.Relation()
	return &v1.DispatchQueryPlanRequest{
		Operation:    op,
		CanonicalKey: key,
		Resource: &core.ObjectAndRelation{
			Namespace: resource.ObjectType,
			ObjectId:  resource.ObjectID,
		},
		Subject: &core.ObjectAndRelation{
			Namespace: subject.ObjectType,
			ObjectId:  subject.ObjectID,
			Relation:  subject.Relation,
		},
		PlanContext: planContextForDispatch(e.planContext, key, ctx.TopLevelOperation),
	}
}

// planContextForDispatch returns a shallow copy of pc with `key` appended to
// the in-progress dispatch keys and `topLevelOp` recorded as the user-facing
// operation. Two receiver-side guards depend on these:
//   - InProgressKeys breaks cross-relation cycles produced by standalone plan
//     rebuilds at each receiver hop.
//   - TopLevelOperation propagates the original API operation across hops so
//     iterators that consult TopLevelOperation see consistent user intent.
func planContextForDispatch(pc *v1.PlanContext, key string, topLevelOp query.Operation) *v1.PlanContext {
	if pc == nil {
		return &v1.PlanContext{
			InProgressKeys:    []string{key},
			TopLevelOperation: queryOpToPlanOperation(topLevelOp),
		}
	}
	// Preserve any TopLevelOperation already on pc (set higher in the chain);
	// only fill in when the chain hasn't recorded a user-facing op yet.
	// PlanOperation's zero value is CHECK, which is also the natural fallback
	// for Check-rooted chains, so detecting "unset" here is by-design lossy.
	tlo := pc.TopLevelOperation
	if tlo == v1.PlanOperation_PLAN_OPERATION_CHECK {
		tlo = queryOpToPlanOperation(topLevelOp)
	}

	// Duplicate and append the in progress keys
	inProgress := make([]string, len(pc.InProgressKeys)+1)
	copy(inProgress, pc.InProgressKeys)
	inProgress[len(pc.InProgressKeys)] = key

	return &v1.PlanContext{
		Revision:               pc.Revision,
		CaveatContext:          pc.CaveatContext,
		MaxRecursionDepth:      pc.MaxRecursionDepth,
		OptionalDatastoreLimit: pc.OptionalDatastoreLimit,
		SchemaHash:             pc.SchemaHash,
		InProgressKeys:         inProgress,
		TopLevelOperation:      tlo,
	}
}

// queryOpToPlanOperation maps the user-facing query.Operation (Check /
// IterSubjects / IterResources) to its PlanOperation equivalent so the
// outgoing PlanContext can carry the original API operation across hops.
// CheckMany variants and OperationUnset both map to PLAN_OPERATION_CHECK
// since the field only describes user-facing intent.
func queryOpToPlanOperation(op query.Operation) v1.PlanOperation {
	switch op {
	case query.OperationIterSubjects:
		return v1.PlanOperation_PLAN_OPERATION_LOOKUP_SUBJECTS
	case query.OperationIterResources:
		return v1.PlanOperation_PLAN_OPERATION_LOOKUP_RESOURCES
	default:
		return v1.PlanOperation_PLAN_OPERATION_CHECK
	}
}

func collectPathsFromResponses(responses []*v1.DispatchQueryPlanResponse) []*query.Path {
	var paths []*query.Path
	for _, resp := range responses {
		for _, rp := range resp.Paths {
			paths = append(paths, resultPathToQueryPath(rp))
		}
	}
	return paths
}

// resultPathToQueryPath converts a proto ResultPath to a query.Path.
func resultPathToQueryPath(rp *v1.ResultPath) *query.Path {
	p := &query.Path{
		Resource: query.Object{
			ObjectType: rp.ResourceType,
			ObjectID:   rp.ResourceId,
		},
		Relation: rp.Relation,
		Subject: query.ObjectAndRelation{
			ObjectType: rp.SubjectType,
			ObjectID:   rp.SubjectId,
			Relation:   rp.SubjectRelation,
		},
		Caveat:    rp.Caveat,
		Integrity: rp.Integrity,
	}
	if rp.Expiration != nil {
		t := rp.Expiration.AsTime()
		p.Expiration = &t
	}
	if rp.Metadata != nil {
		p.Metadata = rp.Metadata.AsMap()
	}
	if len(rp.ExcludedSubjects) > 0 {
		p.ExcludedSubjects = make([]*query.Path, len(rp.ExcludedSubjects))
		for i, esp := range rp.ExcludedSubjects {
			p.ExcludedSubjects[i] = resultPathToQueryPath(esp)
		}
	}
	return p
}

// QueryPathToResultPath converts a query.Path to a proto ResultPath.
func QueryPathToResultPath(p *query.Path) *v1.ResultPath {
	rp := &v1.ResultPath{
		ResourceType:    p.Resource.ObjectType,
		ResourceId:      p.Resource.ObjectID,
		Relation:        p.Relation,
		SubjectType:     p.Subject.ObjectType,
		SubjectId:       p.Subject.ObjectID,
		SubjectRelation: p.Subject.Relation,
		Caveat:          p.Caveat,
		Integrity:       p.Integrity,
	}
	if p.Expiration != nil {
		rp.Expiration = timestamppb.New(*p.Expiration)
	}
	if p.Metadata != nil {
		md, err := toProtoStruct(p.Metadata)
		if err == nil {
			rp.Metadata = md
		}
	}
	if len(p.ExcludedSubjects) > 0 {
		rp.ExcludedSubjects = make([]*v1.ResultPath, len(p.ExcludedSubjects))
		for i, ep := range p.ExcludedSubjects {
			rp.ExcludedSubjects[i] = QueryPathToResultPath(ep)
		}
	}
	return rp
}

func toProtoStruct(m map[string]any) (*structpb.Struct, error) {
	if len(m) == 0 {
		return nil, nil
	}
	return structpb.NewStruct(m)
}

// NewQueryContext builds a query.Context whose Executor is a DispatchExecutor
// driven by the given PlanContext, so that dispatched sub-requests carry the
// same plan state. Plan-derived options (caveat context, recursion depth,
// datastore limit) are applied first; extra opts are appended after.
// dispatchChunkSize caps inputs-per-RPC for batched CheckMany operations.
func NewQueryContext(
	stdContext context.Context,
	dispatcher Dispatcher,
	planContext *v1.PlanContext,
	reader query.QueryDatastoreReader,
	caveatRunner *caveats.CaveatRunner,
	dispatchChunkSize uint16,
	opts ...query.ContextOption,
) *query.Context {
	executor := NewDispatchExecutor(dispatcher, planContext, dispatchChunkSize)

	baseOpts := []query.ContextOption{
		query.WithReader(reader),
		query.WithCaveatContext(CaveatContextFromPlanContext(planContext)),
		query.WithCaveatRunner(caveatRunner),
		query.WithBatchedArrows(true),
	}
	if d := planContext.GetMaxRecursionDepth(); d > 0 {
		baseOpts = append(baseOpts, query.WithMaxRecursionDepth(int(d)))
	}
	if l := planContext.GetOptionalDatastoreLimit(); l > 0 {
		baseOpts = append(baseOpts, query.WithPaginationLimit(l))
	}
	baseOpts = append(baseOpts, opts...)

	return query.NewQueryContext(stdContext, executor, baseOpts...)
}

// NewPlanContext builds a PlanContext proto from query.Context fields.
func NewPlanContext(revision string, schemaHash datalayer.SchemaHash, caveatContext map[string]any, maxRecursionDepth int, datastoreLimit uint64) *v1.PlanContext {
	pc := &v1.PlanContext{
		Revision:               revision,
		MaxRecursionDepth:      int32(maxRecursionDepth),
		OptionalDatastoreLimit: datastoreLimit,
		SchemaHash:             []byte(schemaHash),
	}
	if caveatContext != nil {
		cc, err := structpb.NewStruct(caveatContext)
		if err == nil {
			pc.CaveatContext = cc
		}
	}
	return pc
}

// CaveatContextFromPlanContext extracts the caveat context map from a PlanContext.
func CaveatContextFromPlanContext(pc *v1.PlanContext) map[string]any {
	if pc == nil || pc.CaveatContext == nil {
		return nil
	}
	return pc.CaveatContext.AsMap()
}

// PaginationLimitFromPlanContext returns the datastore limit from a PlanContext, or nil if unset.
func PaginationLimitFromPlanContext(pc *v1.PlanContext) *uint64 {
	if pc == nil || pc.OptionalDatastoreLimit == 0 {
		return nil
	}
	limit := pc.OptionalDatastoreLimit
	return &limit
}
