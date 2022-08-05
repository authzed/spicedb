package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const errDispatch = "error dispatching request: %w"

var tracer = otel.Tracer("spicedb/internal/dispatch/local")

// NewLocalOnlyDispatcher creates a dispatcher that consults with the graph to formulate a response.
func NewLocalOnlyDispatcher(concurrencyLimit uint16) dispatch.Dispatcher {
	d := &localDispatcher{}

	d.checker = graph.NewConcurrentChecker(d, concurrencyLimit)
	d.expander = graph.NewConcurrentExpander(d)
	d.lookupHandler = graph.NewConcurrentLookup(d, d, concurrencyLimit)
	d.reachableResourcesHandler = graph.NewConcurrentReachableResources(d, concurrencyLimit)
	d.lookupSubjectsHandler = graph.NewConcurrentLookupSubjects(d, concurrencyLimit)

	return d
}

// NewDispatcher creates a dispatcher that consults with the graph and redispatches subproblems to
// the provided redispatcher.
func NewDispatcher(redispatcher dispatch.Dispatcher, concurrencyLimit uint16) dispatch.Dispatcher {
	checker := graph.NewConcurrentChecker(redispatcher, concurrencyLimit)
	expander := graph.NewConcurrentExpander(redispatcher)
	lookupHandler := graph.NewConcurrentLookup(redispatcher, redispatcher, concurrencyLimit)
	reachableResourcesHandler := graph.NewConcurrentReachableResources(redispatcher, concurrencyLimit)
	lookupSubjectsHandler := graph.NewConcurrentLookupSubjects(redispatcher, concurrencyLimit)

	return &localDispatcher{
		checker:                   checker,
		expander:                  expander,
		lookupHandler:             lookupHandler,
		reachableResourcesHandler: reachableResourcesHandler,
		lookupSubjectsHandler:     lookupSubjectsHandler,
	}
}

type localDispatcher struct {
	checker                   *graph.ConcurrentChecker
	expander                  *graph.ConcurrentExpander
	lookupHandler             *graph.ConcurrentLookup
	reachableResourcesHandler *graph.ConcurrentReachableResources
	lookupSubjectsHandler     *graph.ConcurrentLookupSubjects
}

func (ld *localDispatcher) loadNamespace(ctx context.Context, nsName string, revision decimal.Decimal) (*core.NamespaceDefinition, error) {
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(revision)

	// Load namespace and relation from the datastore
	ns, _, err := ds.ReadNamespace(ctx, nsName)
	if err != nil {
		return nil, rewriteError(err)
	}

	return ns, err
}

func (ld *localDispatcher) lookupRelation(ctx context.Context, ns *core.NamespaceDefinition, relationName string, revision decimal.Decimal) (*core.Relation, error) {
	var relation *core.Relation
	for _, candidate := range ns.Relation {
		if candidate.Name == relationName {
			relation = candidate
			break
		}
	}

	if relation == nil {
		return nil, NewRelationNotFoundErr(ns.Name, relationName)
	}

	return relation, nil
}

type stringableOnr struct {
	*core.ObjectAndRelation
}

func (onr stringableOnr) String() string {
	return tuple.StringONR(onr.ObjectAndRelation)
}

type stringableRelRef struct {
	*core.RelationReference
}

func (rr stringableRelRef) String() string {
	return fmt.Sprintf("%s::%s", rr.Namespace, rr.Relation)
}

// DispatchCheck implements dispatch.Check interface
func (ld *localDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "DispatchCheck", trace.WithAttributes(
		attribute.Stringer("start", stringableOnr{req.ResourceAndRelation}),
		attribute.Stringer("subject", stringableOnr{req.Subject}),
	))
	defer span.End()

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		if req.Debug != v1.DispatchCheckRequest_ENABLE_DEBUGGING {
			return &v1.DispatchCheckResponse{
				Metadata: &v1.ResponseMeta{
					DispatchCount: 0,
				},
			}, err
		}

		// NOTE: we return debug information here to ensure tooling can see the cycle.
		return &v1.DispatchCheckResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: 0,
				DebugInfo: &v1.DebugInformation{
					Check: &v1.CheckDebugTrace{
						Request: req,
					},
				},
			},
		}, err
	}

	revision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	ns, err := ld.loadNamespace(ctx, req.ResourceAndRelation.Namespace, revision)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	relation, err := ld.lookupRelation(ctx, ns, req.ResourceAndRelation.Relation, revision)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	// If the relation is aliasing another one and the subject does not have the same type as
	// resource, load the aliased relation and dispatch to it. We cannot use the alias if the
	// resource and subject types are the same because a check on the *exact same* resource and
	// subject must pass, and we don't know how many intermediate steps may hit that case.
	if relation.AliasingRelation != "" && req.ResourceAndRelation.Namespace != req.Subject.Namespace {
		relation, err := ld.lookupRelation(ctx, ns, relation.AliasingRelation, revision)
		if err != nil {
			return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
		}

		// Rewrite the request over the aliased relation.
		validatedReq := graph.ValidatedCheckRequest{
			DispatchCheckRequest: &v1.DispatchCheckRequest{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: req.ResourceAndRelation.Namespace,
					ObjectId:  req.ResourceAndRelation.ObjectId,
					Relation:  relation.Name,
				},
				Subject:  req.Subject,
				Metadata: req.Metadata,
				Debug:    req.Debug,
			},
			Revision: revision,
		}

		return ld.checker.Check(ctx, validatedReq, relation)
	}

	return ld.checker.Check(ctx, graph.ValidatedCheckRequest{
		DispatchCheckRequest: req,
		Revision:             revision,
	}, relation)
}

// DispatchExpand implements dispatch.Expand interface
func (ld *localDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	ctx, span := tracer.Start(ctx, "DispatchExpand", trace.WithAttributes(
		attribute.Stringer("start", stringableOnr{req.ResourceAndRelation}),
	))
	defer span.End()

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	revision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	ns, err := ld.loadNamespace(ctx, req.ResourceAndRelation.Namespace, revision)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	relation, err := ld.lookupRelation(ctx, ns, req.ResourceAndRelation.Relation, revision)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	return ld.expander.Expand(ctx, graph.ValidatedExpandRequest{
		DispatchExpandRequest: req,
		Revision:              revision,
	}, relation)
}

// DispatchLookup implements dispatch.Lookup interface
func (ld *localDispatcher) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	ctx, span := tracer.Start(ctx, "DispatchLookup", trace.WithAttributes(
		attribute.Stringer("start", stringableRelRef{req.ObjectRelation}),
		attribute.Stringer("subject", stringableOnr{req.Subject}),
		attribute.Int64("limit", int64(req.Limit)),
	))
	defer span.End()

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata}, err
	}

	revision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata}, err
	}

	if req.Limit <= 0 {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata, ResolvedOnrs: []*core.ObjectAndRelation{}}, nil
	}

	return ld.lookupHandler.LookupViaReachability(ctx, graph.ValidatedLookupRequest{
		DispatchLookupRequest: req,
		Revision:              revision,
	})
}

// DispatchReachableResources implements dispatch.ReachableResources interface
func (ld *localDispatcher) DispatchReachableResources(
	req *v1.DispatchReachableResourcesRequest,
	stream dispatch.ReachableResourcesStream,
) error {
	ctx, span := tracer.Start(stream.Context(), "DispatchReachableResources", trace.WithAttributes(
		attribute.Stringer("resource-type", stringableRelRef{req.ResourceRelation}),
		attribute.Stringer("subject-type", stringableRelRef{req.SubjectRelation}),
		attribute.StringSlice("subject-ids", req.SubjectIds),
	))
	defer span.End()

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return err
	}

	revision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return err
	}

	return ld.reachableResourcesHandler.ReachableResources(
		graph.ValidatedReachableResourcesRequest{
			DispatchReachableResourcesRequest: req,
			Revision:                          revision,
		},
		dispatch.StreamWithContext(ctx, stream),
	)
}

// DispatchLookupSubjects implements dispatch.LookupSubjects interface
func (ld *localDispatcher) DispatchLookupSubjects(
	req *v1.DispatchLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
) error {
	ctx, span := tracer.Start(stream.Context(), "DispatchLookupSubjects", trace.WithAttributes(
		attribute.Stringer("resource-type", stringableRelRef{req.ResourceRelation}),
		attribute.Stringer("subject-type", stringableRelRef{req.SubjectRelation}),
		attribute.StringSlice("resource-ids", req.ResourceIds),
	))
	defer span.End()

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return err
	}

	revision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return err
	}

	return ld.lookupSubjectsHandler.LookupSubjects(
		graph.ValidatedLookupSubjectsRequest{
			DispatchLookupSubjectsRequest: req,
			Revision:                      revision,
		},
		dispatch.StreamWithContext(ctx, stream),
	)
}

func (ld *localDispatcher) Close() error {
	return nil
}

func (ld *localDispatcher) IsReady() bool {
	return true
}

func rewriteError(original error) error {
	nsNotFound := datastore.ErrNamespaceNotFound{}

	switch {
	case errors.As(original, &nsNotFound):
		return NewNamespaceNotFoundErr(nsNotFound.NotFoundNamespaceName())
	case errors.As(original, &ErrNamespaceNotFound{}):
		fallthrough
	case errors.As(original, &ErrRelationNotFound{}):
		return original
	default:
		return fmt.Errorf(errDispatch, original)
	}
}

var emptyMetadata = &v1.ResponseMeta{
	DispatchCount: 0,
}
