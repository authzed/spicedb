package dispatch

import (
	"context"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
)

// DispatchExecutor is an Executor that selectively dispatches sub-operations
// through the dispatch infrastructure at alias iterator boundaries.
// For non-alias iterators, it delegates locally like LocalExecutor.
type DispatchExecutor struct {
	dispatcher  Dispatcher
	planContext *v1.PlanContext
}

var _ query.Executor = &DispatchExecutor{}

// NewDispatchExecutor creates a new DispatchExecutor that dispatches alias
// iterator operations through the given Dispatcher chain.
func NewDispatchExecutor(dispatcher Dispatcher, planContext *v1.PlanContext) *DispatchExecutor {
	return &DispatchExecutor{
		dispatcher:  dispatcher,
		planContext: planContext,
	}
}

func (e *DispatchExecutor) Check(ctx *query.Context, it query.Iterator, resource query.Object, subject query.ObjectAndRelation) (*query.Path, error) {
	if _, ok := it.(*query.AliasIterator); ok {
		return e.dispatchCheck(ctx, it, resource, subject)
	}
	return it.CheckImpl(ctx, resource, subject)
}

func (e *DispatchExecutor) IterSubjects(ctx *query.Context, it query.Iterator, resource query.Object, filterSubjectType query.ObjectType) (query.PathSeq, error) {
	if _, ok := it.(*query.AliasIterator); ok {
		return e.dispatchIterSubjects(ctx, it, resource, filterSubjectType)
	}
	pathSeq, err := it.IterSubjectsImpl(ctx, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}
	return query.FilterSubjectsByType(pathSeq, filterSubjectType), nil
}

func (e *DispatchExecutor) IterResources(ctx *query.Context, it query.Iterator, subject query.ObjectAndRelation, filterResourceType query.ObjectType) (query.PathSeq, error) {
	if _, ok := it.(*query.AliasIterator); ok {
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

	req := e.buildRequest(v1.PlanOperation_PLAN_OPERATION_CHECK, it, resource, subject)
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

	req := e.buildRequest(v1.PlanOperation_PLAN_OPERATION_LOOKUP_SUBJECTS, it, resource, query.ObjectAndRelation{
		ObjectType: resource.ObjectType,
		ObjectID:   resource.ObjectID,
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

	req := e.buildRequest(v1.PlanOperation_PLAN_OPERATION_LOOKUP_RESOURCES, it, query.Object{
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

func (e *DispatchExecutor) buildRequest(op v1.PlanOperation, it query.Iterator, resource query.Object, subject query.ObjectAndRelation) *v1.DispatchQueryPlanRequest {
	return &v1.DispatchQueryPlanRequest{
		Operation:    op,
		CanonicalKey: string(it.CanonicalKey()),
		Resource: &core.ObjectAndRelation{
			Namespace: resource.ObjectType,
			ObjectId:  resource.ObjectID,
		},
		Subject: &core.ObjectAndRelation{
			Namespace: subject.ObjectType,
			ObjectId:  subject.ObjectID,
			Relation:  subject.Relation,
		},
		PlanContext: e.planContext,
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
	return rp
}

func toProtoStruct(m map[string]any) (*structpb.Struct, error) {
	if len(m) == 0 {
		return nil, nil
	}
	return structpb.NewStruct(m)
}

// NewPlanContext builds a PlanContext proto from query.Context fields.
func NewPlanContext(revision string, caveatContext map[string]any, maxRecursionDepth int, datastoreLimit uint64) *v1.PlanContext {
	pc := &v1.PlanContext{
		Revision:               revision,
		MaxRecursionDepth:      int32(maxRecursionDepth),
		OptionalDatastoreLimit: datastoreLimit,
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
