package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
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

// ConcurrencyLimits defines per-dispatch-type concurrency limits.
type ConcurrencyLimits struct {
	Check              uint16
	LookupResources    uint16
	ReachableResources uint16
	LookupSubjects     uint16
}

const defaultConcurrencyLimit = 50

// WithOverallDefaultLimit sets the overall default limit for any unspecified limits
// and returns a new struct.
func (cl ConcurrencyLimits) WithOverallDefaultLimit(overallDefaultLimit uint16) ConcurrencyLimits {
	return limitsOrDefaults(cl, overallDefaultLimit)
}

func (cl ConcurrencyLimits) MarshalZerologObject(e *zerolog.Event) {
	e.Uint16("check-permission", cl.Check)
	e.Uint16("lookup-resources", cl.LookupResources)
	e.Uint16("lookup-subjects", cl.LookupSubjects)
	e.Uint16("reachable-resources", cl.ReachableResources)
}

func limitsOrDefaults(limits ConcurrencyLimits, overallDefaultLimit uint16) ConcurrencyLimits {
	limits.Check = limitOrDefault(limits.Check, overallDefaultLimit)
	limits.LookupResources = limitOrDefault(limits.LookupResources, overallDefaultLimit)
	limits.LookupSubjects = limitOrDefault(limits.LookupSubjects, overallDefaultLimit)
	limits.ReachableResources = limitOrDefault(limits.ReachableResources, overallDefaultLimit)
	return limits
}

func limitOrDefault(limit uint16, defaultLimit uint16) uint16 {
	if limit <= 0 {
		return defaultLimit
	}
	return limit
}

// SharedConcurrencyLimits returns a ConcurrencyLimits struct with the limit
// set to that provided for each operation.
func SharedConcurrencyLimits(concurrencyLimit uint16) ConcurrencyLimits {
	return ConcurrencyLimits{
		Check:              concurrencyLimit,
		LookupResources:    concurrencyLimit,
		ReachableResources: concurrencyLimit,
		LookupSubjects:     concurrencyLimit,
	}
}

// NewLocalOnlyDispatcher creates a dispatcher that consults with the graph to formulate a response.
func NewLocalOnlyDispatcher(concurrencyLimit uint16) dispatch.Dispatcher {
	return NewLocalOnlyDispatcherWithLimits(SharedConcurrencyLimits(concurrencyLimit))
}

// NewLocalOnlyDispatcherWithLimits creates a dispatcher thatg consults with the graph to formulate a response
// and has the defined concurrency limits per dispatch type.
func NewLocalOnlyDispatcherWithLimits(concurrencyLimits ConcurrencyLimits) dispatch.Dispatcher {
	d := &localDispatcher{}

	concurrencyLimits = limitsOrDefaults(concurrencyLimits, defaultConcurrencyLimit)

	d.checker = graph.NewConcurrentChecker(d, concurrencyLimits.Check)
	d.expander = graph.NewConcurrentExpander(d)
	d.lookupHandler = graph.NewConcurrentLookup(d, d, concurrencyLimits.LookupResources)
	d.reachableResourcesHandler = graph.NewConcurrentReachableResources(d, concurrencyLimits.ReachableResources)
	d.lookupSubjectsHandler = graph.NewConcurrentLookupSubjects(d, concurrencyLimits.LookupSubjects)

	return d
}

// NewDispatcher creates a dispatcher that consults with the graph and redispatches subproblems to
// the provided redispatcher.
func NewDispatcher(redispatcher dispatch.Dispatcher, concurrencyLimits ConcurrencyLimits) dispatch.Dispatcher {
	concurrencyLimits = limitsOrDefaults(concurrencyLimits, defaultConcurrencyLimit)

	checker := graph.NewConcurrentChecker(redispatcher, concurrencyLimits.Check)
	expander := graph.NewConcurrentExpander(redispatcher)
	lookupHandler := graph.NewConcurrentLookup(redispatcher, redispatcher, concurrencyLimits.LookupResources)
	reachableResourcesHandler := graph.NewConcurrentReachableResources(redispatcher, concurrencyLimits.ReachableResources)
	lookupSubjectsHandler := graph.NewConcurrentLookupSubjects(redispatcher, concurrencyLimits.LookupSubjects)

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

func (ld *localDispatcher) loadNamespace(ctx context.Context, nsName string, revision datastore.Revision) (*core.NamespaceDefinition, error) {
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(revision)

	// Load namespace and relation from the datastore
	ns, _, err := ds.ReadNamespace(ctx, nsName)
	if err != nil {
		return nil, rewriteError(err)
	}

	return ns, err
}

func (ld *localDispatcher) parseRevision(ctx context.Context, s string) (datastore.Revision, error) {
	ds := datastoremw.MustFromContext(ctx)
	return ds.RevisionFromString(s)
}

func (ld *localDispatcher) lookupRelation(ctx context.Context, ns *core.NamespaceDefinition, relationName string) (*core.Relation, error) {
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
		attribute.Stringer("resource-type", stringableRelRef{req.ResourceRelation}),
		attribute.StringSlice("resource-ids", req.ResourceIds),
		attribute.Stringer("subject", stringableOnr{req.Subject}),
	))
	defer span.End()

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		if req.Debug != v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING {
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

	revision, err := ld.parseRevision(ctx, req.Metadata.AtRevision)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	ns, err := ld.loadNamespace(ctx, req.ResourceRelation.Namespace, revision)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	relation, err := ld.lookupRelation(ctx, ns, req.ResourceRelation.Relation)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	// If the relation is aliasing another one and the subject does not have the same type as
	// resource, load the aliased relation and dispatch to it. We cannot use the alias if the
	// resource and subject types are the same because a check on the *exact same* resource and
	// subject must pass, and we don't know how many intermediate steps may hit that case.
	if relation.AliasingRelation != "" && req.ResourceRelation.Namespace != req.Subject.Namespace {
		relation, err := ld.lookupRelation(ctx, ns, relation.AliasingRelation)
		if err != nil {
			return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
		}

		// Rewrite the request over the aliased relation.
		validatedReq := graph.ValidatedCheckRequest{
			DispatchCheckRequest: &v1.DispatchCheckRequest{
				ResourceRelation: &core.RelationReference{
					Namespace: req.ResourceRelation.Namespace,
					Relation:  relation.Name,
				},
				ResourceIds: req.ResourceIds,
				Subject:     req.Subject,
				Metadata:    req.Metadata,
				Debug:       req.Debug,
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

	revision, err := ld.parseRevision(ctx, req.Metadata.AtRevision)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	ns, err := ld.loadNamespace(ctx, req.ResourceAndRelation.Namespace, revision)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	relation, err := ld.lookupRelation(ctx, ns, req.ResourceAndRelation.Relation)
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
	// TODO(jschorr): Since lookup is now calling reachable resources exclusively, we should
	// probably move it out of the dispatcher and into computed
	ctx, span := tracer.Start(ctx, "DispatchLookup", trace.WithAttributes(
		attribute.Stringer("start", stringableRelRef{req.ObjectRelation}),
		attribute.Stringer("subject", stringableOnr{req.Subject}),
		attribute.Int64("limit", int64(req.Limit)),
	))
	defer span.End()

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata}, err
	}

	revision, err := ld.parseRevision(ctx, req.Metadata.AtRevision)
	if err != nil {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata}, err
	}

	if req.Limit <= 0 {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata, ResolvedResources: []*v1.ResolvedResource{}}, nil
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

	revision, err := ld.parseRevision(ctx, req.Metadata.AtRevision)
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

	revision, err := ld.parseRevision(ctx, req.Metadata.AtRevision)
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
