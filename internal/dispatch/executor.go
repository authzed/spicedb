package dispatch

import (
	"bytes"
	"context"
	"fmt"
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

// shouldDispatch reports whether to ship this iterator over RPC vs run it
// locally. Two conditions must hold:
//
//  1. The iterator is a DispatchIterator — the optimizer's authoritative
//     dispatch boundary (see internal/dispatch/dispatch_optimizer.go).
//
//  2. currentOp matches ctx.TopLevelOperation — the request type at this
//     wrap matches the user-facing request type that started the work. The
//     receiver-side executor only knows how to drive one operation per
//     dispatch (the request's op == top-level op by construction at the
//     edge), so dispatching a wrap reached via a *different* sub-operation
//     (e.g. Arrow(RTL) drives IterResources on its right subtree while the
//     user request is Check) would launch an RPC whose results we then
//     re-process locally — pure overhead. Stay local; let CheckImpl drive.
//
// No runtime cycle-break: the compiler emits RecursiveIterator +
// RecursiveSentinelIterator for every cyclic schema, so the compiled tree is
// finite by construction and a dispatch chain can't loop on the same key.
func (e *DispatchExecutor) shouldDispatch(ctx *query.Context, it query.Iterator, currentOp query.Operation) bool {
	if _, ok := it.(*DispatchIterator); !ok {
		return false
	}
	return currentOp == ctx.TopLevelOperation
}

func (e *DispatchExecutor) Check(ctx *query.Context, it query.Iterator, resource query.Object, subject query.ObjectAndRelation) (*query.Path, error) {
	if e.shouldDispatch(ctx, it, query.OperationCheck) {
		return e.dispatchCheck(ctx, it, resource, subject)
	}
	return it.CheckImpl(ctx, resource, subject)
}

func (e *DispatchExecutor) CheckManySubjects(ctx *query.Context, it query.Iterator, resource query.Object, subjects []query.ObjectAndRelation) ([]*query.Path, error) {
	if !e.shouldDispatch(ctx, it, query.OperationCheck) {
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
	if !e.shouldDispatch(ctx, it, query.OperationCheck) {
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
	if e.shouldDispatch(ctx, it, query.OperationIterSubjects) {
		return e.dispatchIterSubjects(ctx, it, resource, filterSubjectType)
	}
	pathSeq, err := it.IterSubjectsImpl(ctx, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}
	return query.FilterSubjectsByType(pathSeq, filterSubjectType), nil
}

func (e *DispatchExecutor) IterResources(ctx *query.Context, it query.Iterator, subject query.ObjectAndRelation, filterResourceType query.ObjectType) (query.PathSeq, error) {
	if e.shouldDispatch(ctx, it, query.OperationIterResources) {
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

	// The serialized iterator bytes are the dispatch identity — we serialize
	// once and reuse the bytes for both the cache lookup and (on miss) the
	// dispatched request. Cache hits skip the full DispatchQueryPlan machinery
	// (request alloc, stream setup, delegate hop) but still pay this serialize
	// cost — fine, because dispatching mandates the serialize anyway.
	plan, err := serializePlan(it)
	if err != nil {
		return nil, err
	}
	resourceONR := &core.ObjectAndRelation{
		Namespace: resource.ObjectType,
		ObjectId:  resource.ObjectID,
	}
	subjectONR := &core.ObjectAndRelation{
		Namespace: subject.ObjectType,
		ObjectId:  subject.ObjectID,
		Relation:  subject.Relation,
	}
	if path, ok, err := e.dispatcher.LookupPlanCheck(subCtx, PlanCheckLookup{
		Plan:        plan,
		Resource:    resourceONR,
		Subject:     subjectONR,
		PlanContext: e.planContext,
	}); err != nil {
		return nil, err
	} else if ok {
		return resultPathToQueryPath(path), nil
	}

	req := e.buildRequest(ctx, v1.PlanOperation_PLAN_OPERATION_CHECK, plan, resourceONR, subjectONR)
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

	plan, err := serializePlan(it)
	if err != nil {
		return nil, err
	}

	// For LookupSubjects the proto Subject field carries the *filter* subject
	// type+relation rather than a real subject ONR — the receiver needs the
	// filter to apply the same reachability/optimizer decisions the sender
	// would have applied. The actual receiver-side iteration uses NoObjectFilter
	// since the sender filters the returned paths itself.
	resourceONR := &core.ObjectAndRelation{
		Namespace: resource.ObjectType,
		ObjectId:  resource.ObjectID,
	}
	subjectONR := &core.ObjectAndRelation{
		Namespace: filterSubjectType.Type,
		Relation:  filterSubjectType.Subrelation,
	}
	req := e.buildRequest(ctx, v1.PlanOperation_PLAN_OPERATION_LOOKUP_SUBJECTS, plan, resourceONR, subjectONR)
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

	plan, err := serializePlan(it)
	if err != nil {
		return nil, err
	}

	resourceONR := &core.ObjectAndRelation{
		Namespace: subject.ObjectType,
		ObjectId:  subject.ObjectID,
	}
	subjectONR := &core.ObjectAndRelation{
		Namespace: subject.ObjectType,
		ObjectId:  subject.ObjectID,
		Relation:  subject.Relation,
	}
	req := e.buildRequest(ctx, v1.PlanOperation_PLAN_OPERATION_LOOKUP_RESOURCES, plan, resourceONR, subjectONR)
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

	plan, err := serializePlan(it)
	if err != nil {
		return nil, err
	}
	resourceONR := &core.ObjectAndRelation{
		Namespace: resource.ObjectType,
		ObjectId:  resource.ObjectID,
	}

	out := make([]*query.Path, len(subjects))
	idx := 0
	for chunk := range slices.Chunk(subjects, int(e.dispatchChunkSize)) {
		// CHECK_MANY_SUBJECTS: real resource ONR, empty subject ONR (real subjects
		// ride on `many`). Empty placeholder rather than nil so the receiver's
		// dereferences of req.Subject.* don't NPE.
		req := e.buildManyRequest(ctx, v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS, plan, resourceONR, &core.ObjectAndRelation{}, nil, chunk)
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

	plan, err := serializePlan(it)
	if err != nil {
		return nil, err
	}
	subjectONR := &core.ObjectAndRelation{
		Namespace: subject.ObjectType,
		ObjectId:  subject.ObjectID,
		Relation:  subject.Relation,
	}

	out := make([]*query.Path, len(resources))
	idx := 0
	for chunk := range slices.Chunk(resources, int(e.dispatchChunkSize)) {
		// CHECK_MANY_RESOURCES: empty resource ONR (real resources on `many`),
		// real subject ONR. Empty placeholder rather than nil so the receiver's
		// dereferences of req.Resource.* don't NPE.
		req := e.buildManyRequest(ctx, v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES, plan, &core.ObjectAndRelation{}, subjectONR, chunk, nil)
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

// buildManyRequest constructs a DispatchQueryPlanRequest for CheckMany
// operations. Exactly one of (manyResources, manySubjects) is non-nil; the
// corresponding singular field (resource for CHECK_MANY_RESOURCES, subject
// for CHECK_MANY_SUBJECTS) must be left zero — the receiver inspects `many`
// for those operations. Callers serialize the iterator into `plan` before
// calling so the bytes can also be used for cache lookups when relevant.
func (e *DispatchExecutor) buildManyRequest(ctx *query.Context, op v1.PlanOperation, plan []byte, resource *core.ObjectAndRelation, subject *core.ObjectAndRelation, manyResources []query.Object, manySubjects []query.ObjectAndRelation) *v1.DispatchQueryPlanRequest {
	req := &v1.DispatchQueryPlanRequest{
		Operation:   op,
		Resource:    resource,
		Subject:     subject,
		PlanContext: planContextForDispatch(e.planContext, ctx.TopLevelOperation),
		Plan:        plan,
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

// serializePlan emits the iterator subtree using the pkg/query wire format.
// The bytes are simultaneously: (a) the dispatch identity used to compute
// cache/singleflight/cluster keys, and (b) the payload the receiver
// deserializes and executes. One serialize per dispatch, used for both.
func serializePlan(it query.Iterator) ([]byte, error) {
	var buf bytes.Buffer
	if err := it.Serialize(&buf); err != nil {
		return nil, fmt.Errorf("DispatchQueryPlan: serialize plan: %w", err)
	}
	return buf.Bytes(), nil
}

// buildRequest constructs a DispatchQueryPlanRequest for the single-target
// dispatch operations (CHECK, LOOKUP_RESOURCES, LOOKUP_SUBJECTS). Callers
// serialize the iterator into `plan` before calling — for CHECK the same
// bytes drive the cache lookup, so we don't re-serialize inside.
func (e *DispatchExecutor) buildRequest(ctx *query.Context, op v1.PlanOperation, plan []byte, resource *core.ObjectAndRelation, subject *core.ObjectAndRelation) *v1.DispatchQueryPlanRequest {
	return &v1.DispatchQueryPlanRequest{
		Operation:   op,
		Resource:    resource,
		Subject:     subject,
		PlanContext: planContextForDispatch(e.planContext, ctx.TopLevelOperation),
		Plan:        plan,
	}
}

// planContextForDispatch returns a shallow copy of pc with `topLevelOp`
// recorded as the user-facing operation. The receiver-side guard
// TopLevelOperation propagates the original API operation across hops so
// iterators that consult TopLevelOperation (e.g. AliasIterator's self-edge
// logic) see consistent user intent. No InProgressKeys append — the plan
// bytes are the dispatch identity now (see keys.PlanCheckLookupKey), and
// cycle-breaks are guaranteed by the compiler's RecursiveIterator wrapping.
func planContextForDispatch(pc *v1.PlanContext, topLevelOp query.Operation) *v1.PlanContext {
	if pc == nil {
		return &v1.PlanContext{
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
	return &v1.PlanContext{
		Revision:               pc.Revision,
		CaveatContext:          pc.CaveatContext,
		MaxRecursionDepth:      pc.MaxRecursionDepth,
		OptionalDatastoreLimit: pc.OptionalDatastoreLimit,
		SchemaHash:             pc.SchemaHash,
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
