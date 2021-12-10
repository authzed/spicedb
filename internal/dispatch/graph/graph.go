package graph

import (
	"context"
	"errors"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const errDispatch = "error dispatching request: %w"

var tracer = otel.Tracer("spicedb/internal/dispatch/local")

// NewLocalOnlyDispatcher creates a dispatcher that consults with the graph to formulate a response.
func NewLocalOnlyDispatcher(
	nsm namespace.Manager,
	ds datastore.Datastore,
) dispatch.Dispatcher {
	d := &localDispatcher{nsm: nsm}

	d.checker = graph.NewConcurrentChecker(d, ds, nsm)
	d.expander = graph.NewConcurrentExpander(d, ds, nsm)
	d.lookupHandler = graph.NewConcurrentLookup(d, ds, nsm)

	return d
}

// NewDispatcher creates a dispatcher that consults with the graph and redispatches subproblems to
// the provided redispatcher.
func NewDispatcher(
	redispatcher dispatch.Dispatcher,
	nsm namespace.Manager,
	ds datastore.Datastore,
) dispatch.Dispatcher {

	checker := graph.NewConcurrentChecker(redispatcher, ds, nsm)
	expander := graph.NewConcurrentExpander(redispatcher, ds, nsm)
	lookupHandler := graph.NewConcurrentLookup(redispatcher, ds, nsm)

	return &localDispatcher{checker, expander, lookupHandler, nsm}
}

type localDispatcher struct {
	checker       *graph.ConcurrentChecker
	expander      *graph.ConcurrentExpander
	lookupHandler *graph.ConcurrentLookup

	nsm namespace.Manager
}

func (ld *localDispatcher) loadRelation(ctx context.Context, nsName, relationName string, revision decimal.Decimal) (*v0.Relation, error) {
	// Load namespace and relation from the datastore
	ns, err := ld.nsm.ReadNamespace(ctx, nsName, revision)
	if err != nil {
		return nil, rewriteError(err)
	}

	var relation *v0.Relation
	for _, candidate := range ns.Relation {
		if candidate.Name == relationName {
			relation = candidate
			break
		}
	}

	if relation == nil {
		return nil, NewRelationNotFoundErr(nsName, relationName)
	}

	return relation, nil
}

type stringableOnr struct {
	*v0.ObjectAndRelation
}

func (onr stringableOnr) String() string {
	return tuple.StringONR(onr.ObjectAndRelation)
}

type stringableRelRef struct {
	*v0.RelationReference
}

func (rr stringableRelRef) String() string {
	return fmt.Sprintf("%s::%s", rr.Namespace, rr.Relation)
}

// DispatchCheck implements dispatch.Check interface
func (ld *localDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "DispatchCheck", trace.WithAttributes(
		attribute.Stringer("start", stringableOnr{req.ObjectAndRelation}),
		attribute.Stringer("subject", stringableOnr{req.Subject}),
	))
	defer span.End()

	err := dispatch.CheckDepth(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	revision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	relation, err := ld.loadRelation(ctx, req.ObjectAndRelation.Namespace, req.ObjectAndRelation.Relation, revision)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	validatedReq := graph.ValidatedCheckRequest{
		DispatchCheckRequest: req,
		Revision:             revision,
	}

	return ld.checker.Check(ctx, validatedReq, relation)
}

// DispatchExpand implements dispatch.Expand interface
func (ld *localDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	ctx, span := tracer.Start(ctx, "DispatchExpand", trace.WithAttributes(
		attribute.Stringer("start", stringableOnr{req.ObjectAndRelation}),
	))
	defer span.End()

	err := dispatch.CheckDepth(ctx, req)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	revision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	relation, err := ld.loadRelation(ctx, req.ObjectAndRelation.Namespace, req.ObjectAndRelation.Relation, revision)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	validatedReq := graph.ValidatedExpandRequest{
		DispatchExpandRequest: req,
		Revision:              revision,
	}

	return ld.expander.Expand(ctx, validatedReq, relation)
}

// DispatchLookup implements dispatch.Lookup interface
func (ld *localDispatcher) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	ctx, span := tracer.Start(ctx, "DispatchLookup", trace.WithAttributes(
		attribute.Stringer("start", stringableRelRef{req.ObjectRelation}),
		attribute.Stringer("subject", stringableOnr{req.Subject}),
		attribute.Int64("limit", int64(req.Limit)),
	))
	defer span.End()

	err := dispatch.CheckDepth(ctx, req)
	if err != nil {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata}, err
	}

	revision, err := decimal.NewFromString(req.Metadata.AtRevision)
	if err != nil {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata}, err
	}

	if req.Limit <= 0 {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata, ResolvedOnrs: []*v0.ObjectAndRelation{}}, nil
	}

	validatedReq := graph.ValidatedLookupRequest{
		DispatchLookupRequest: req,
		Revision:              revision,
	}

	return ld.lookupHandler.Lookup(ctx, validatedReq)
}

func (ld *localDispatcher) Close() error {
	return nil
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

var emptyMetadata *v1.ResponseMeta = &v1.ResponseMeta{
	DispatchCount: 0,
}
