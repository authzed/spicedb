package graph

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

const errDispatch = "error dispatching request: %w"

var errMaxDepth = errors.New("max depth has been reached")

// NewLocalDispatcher creates a dispatcher that checks everything in the same
// process on the same machine.
func NewLocalDispatcher(nsm namespace.Manager, ds datastore.GraphDatastore) (Dispatcher, error) {
	return &localDispatcher{nsm: nsm, ds: ds}, nil
}

type localDispatcher struct {
	nsm namespace.Manager
	ds  datastore.GraphDatastore
}

func (ld *localDispatcher) loadRelation(ctx context.Context, nsName, relationName string) (*pb.Relation, error) {
	// Load namespace and relation from the datastore
	ns, _, err := ld.nsm.ReadNamespace(ctx, nsName)
	if err != nil {
		return nil, rewriteError(err)
	}

	var relation *pb.Relation
	for _, candidate := range ns.Relation {
		if candidate.Name == relationName {
			relation = candidate
			break
		}
	}

	if relation == nil {
		return nil, ErrRelationNotFound
	}

	return relation, nil
}

type stringableOnr struct {
	*pb.ObjectAndRelation
}

func (onr stringableOnr) String() string {
	return tuple.StringONR(onr.ObjectAndRelation)
}

type stringableRelRef struct {
	*pb.RelationReference
}

func (rr stringableRelRef) String() string {
	return fmt.Sprintf("%s::%s", rr.Namespace, rr.Relation)
}

type stringableTuple struct {
	*pb.RelationTuple
}

func (t stringableTuple) String() string {
	return tuple.String(t.RelationTuple)
}

func (ld *localDispatcher) Check(ctx context.Context, req CheckRequest) CheckResult {
	ctx, span := tracer.Start(ctx, "DispatchCheck", trace.WithAttributes(
		attribute.Stringer("start", stringableOnr{req.Start}),
		attribute.Stringer("goal", stringableOnr{req.Goal}),
	))
	defer span.End()

	if req.DepthRemaining < 1 {
		return CheckResult{Err: fmt.Errorf(errDispatch, errMaxDepth)}
	}

	relation, err := ld.loadRelation(ctx, req.Start.Namespace, req.Start.Relation)
	if err != nil {
		return CheckResult{Err: err}
	}

	chk := newConcurrentChecker(ld, ld.ds)

	asyncCheck := chk.check(ctx, req, relation)
	return Any(ctx, []ReduceableCheckFunc{asyncCheck})
}

func (ld *localDispatcher) Expand(ctx context.Context, req ExpandRequest) ExpandResult {
	ctx, span := tracer.Start(ctx, "DispatchExpand", trace.WithAttributes(
		attribute.Stringer("start", stringableOnr{req.Start}),
	))
	defer span.End()

	if req.DepthRemaining < 1 {
		return ExpandResult{Err: fmt.Errorf(errDispatch, errMaxDepth)}
	}

	relation, err := ld.loadRelation(ctx, req.Start.Namespace, req.Start.Relation)
	if err != nil {
		return ExpandResult{Tree: nil, Err: err}
	}

	expand := newConcurrentExpander(ld, ld.ds)

	asyncExpand := expand.expand(ctx, req, relation)
	return ExpandOne(ctx, asyncExpand)
}

func (ld *localDispatcher) Lookup(ctx context.Context, req LookupRequest) LookupResult {
	ctx, span := tracer.Start(ctx, "DispatchLookup", trace.WithAttributes(
		attribute.Stringer("start", stringableOnr{req.Start}),
		attribute.Stringer("targetrelation", stringableRelRef{req.TargetRelation}),
		attribute.Int64("limit", int64(req.Limit)),
	))
	defer span.End()

	if req.DepthRemaining < 1 {
		return LookupResult{Err: fmt.Errorf(errDispatch, errMaxDepth)}
	}

	if req.Limit <= 0 {
		return LookupResult{
			ResolvedObjects: []ResolvedObject{},
		}
	}

	_, typeSystem, _, err := ld.nsm.ReadNamespaceAndTypes(ctx, req.TargetRelation.Namespace)
	if err != nil {
		return LookupResult{
			Err: err,
		}
	}

	reachabilityGraph, err := typeSystem.RelationReachability(ctx, req.TargetRelation.Relation)
	if err != nil {
		return LookupResult{
			Err: err,
		}
	}

	lookup := newConcurrentLookup(ld, ld.ds, reachabilityGraph)
	asyncLookup := lookup.lookup(ctx, req)
	return LookupOne(ctx, asyncLookup)
}

func rewriteError(original error) error {
	switch original {
	case datastore.ErrNamespaceNotFound:
		return ErrNamespaceNotFound
	case ErrNamespaceNotFound:
		fallthrough
	case ErrRelationNotFound:
		return original
	default:
		return fmt.Errorf(errDispatch, original)
	}
}
