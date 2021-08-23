package graph

import (
	"context"
	"errors"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const errDispatch = "error dispatching request: %w"

var errMaxDepth = errors.New("max depth has been reached")

// NewLocalDispatcher creates a dispatcher that checks everything in the same
// process on the same machine.
func NewLocalDispatcher(
	nsm namespace.Manager,
	ds datastore.Datastore,
) (Dispatcher, error) {
	d := &localDispatcher{nsm: nsm}

	d.checker = NewConcurrentChecker(d, ds, nsm)
	d.expander = NewConcurrentExpander(d, ds, nsm)
	d.lookupHandler = NewConcurrentLookup(d, ds, nsm)

	return d, nil
}

type localDispatcher struct {
	checker       Checker
	expander      Expander
	lookupHandler LookupHandler

	nsm namespace.Manager
}

func (ld *localDispatcher) loadRelation(ctx context.Context, nsName, relationName string) (*v0.Relation, error) {
	// Load namespace and relation from the datastore
	ns, _, err := ld.nsm.ReadNamespace(ctx, nsName)
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

func (ld *localDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) CheckResult {
	ctx, span := tracer.Start(ctx, "DispatchCheck", trace.WithAttributes(
		attribute.Stringer("start", stringableOnr{req.ObjectAndRelation}),
		attribute.Stringer("subject", stringableOnr{req.Subject}),
	))
	defer span.End()

	err := checkDepth(req)
	if err != nil {
		return checkResultError(err, 0)
	}

	relation, err := ld.loadRelation(ctx, req.ObjectAndRelation.Namespace, req.ObjectAndRelation.Relation)
	if err != nil {
		return checkResultError(err, 0)
	}

	asyncCheck := ld.checker.Check(ctx, req, relation)
	return Any(ctx, []ReduceableCheckFunc{asyncCheck})
}

func (ld *localDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) ExpandResult {
	ctx, span := tracer.Start(ctx, "DispatchExpand", trace.WithAttributes(
		attribute.Stringer("start", stringableOnr{req.ObjectAndRelation}),
	))
	defer span.End()

	err := checkDepth(req)
	if err != nil {
		return expandResultError(err, 0)
	}

	relation, err := ld.loadRelation(ctx, req.ObjectAndRelation.Namespace, req.ObjectAndRelation.Relation)
	if err != nil {
		return expandResultError(err, 0)
	}

	asyncExpand := ld.expander.Expand(ctx, req, relation)
	return ExpandOne(ctx, asyncExpand)
}

func (ld *localDispatcher) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) LookupResult {
	ctx, span := tracer.Start(ctx, "DispatchLookup", trace.WithAttributes(
		attribute.Stringer("start", stringableRelRef{req.ObjectRelation}),
		attribute.Stringer("subject", stringableOnr{req.Subject}),
		attribute.Int64("limit", int64(req.Limit)),
	))
	defer span.End()

	err := checkDepth(req)
	if err != nil {
		return lookupResultError(err, 0)
	}

	if req.Limit <= 0 {
		return lookupResult([]*v0.ObjectAndRelation{}, 0)
	}

	asyncLookup := ld.lookupHandler.Lookup(ctx, req)
	result := LookupAny(ctx, req.Limit, []ReduceableLookupFunc{asyncLookup})
	return result
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

type hasMetadata interface {
	zerolog.LogObjectMarshaler

	GetMetadata() *v1.ResolverMeta
}

func checkDepth(req hasMetadata) error {
	metadata := req.GetMetadata()
	if metadata == nil {
		log.Warn().Object("req", req).Msg("request missing metadata")
		return fmt.Errorf("request missing metadata")
	}

	if metadata.DepthRemaining == 0 {
		return fmt.Errorf(errDispatch, errMaxDepth)
	}

	return nil
}
