package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/middleware/nodeid"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const errDispatch = "error dispatching request: %w"

var tracer = otel.Tracer("spicedb/internal/dispatch/local")

// ConcurrencyLimits defines per-dispatch-type concurrency limits.
//
//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . ConcurrencyLimits
type ConcurrencyLimits struct {
	Check              uint16 `debugmap:"visible"`
	ReachableResources uint16 `debugmap:"visible"`
	LookupResources    uint16 `debugmap:"visible"`
	LookupSubjects     uint16 `debugmap:"visible"`
}

const defaultConcurrencyLimit = 50

// WithOverallDefaultLimit sets the overall default limit for any unspecified limits
// and returns a new struct.
func (cl ConcurrencyLimits) WithOverallDefaultLimit(overallDefaultLimit uint16) ConcurrencyLimits {
	return limitsOrDefaults(cl, overallDefaultLimit)
}

func (cl ConcurrencyLimits) MarshalZerologObject(e *zerolog.Event) {
	e.Uint16("concurrency-limit-check-permission", cl.Check)
	e.Uint16("concurrency-limit-lookup-resources", cl.LookupResources)
	e.Uint16("concurrency-limit-lookup-subjects", cl.LookupSubjects)
	e.Uint16("concurrency-limit-reachable-resources", cl.ReachableResources)
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
		ReachableResources: concurrencyLimit,
		LookupResources:    concurrencyLimit,
		LookupSubjects:     concurrencyLimit,
	}
}

// NewLocalOnlyDispatcher creates a dispatcher that consults with the graph to formulate a response.
func NewLocalOnlyDispatcher(concurrencyLimit uint16, dispatchChunkSize uint16) dispatch.Dispatcher {
	return NewLocalOnlyDispatcherWithLimits(SharedConcurrencyLimits(concurrencyLimit), dispatchChunkSize)
}

// NewLocalOnlyDispatcherWithLimits creates a dispatcher thatg consults with the graph to formulate a response
// and has the defined concurrency limits per dispatch type.
func NewLocalOnlyDispatcherWithLimits(concurrencyLimits ConcurrencyLimits, dispatchChunkSize uint16) dispatch.Dispatcher {
	d := &localDispatcher{}

	concurrencyLimits = limitsOrDefaults(concurrencyLimits, defaultConcurrencyLimit)
	chunkSize := dispatchChunkSize
	if chunkSize == 0 {
		chunkSize = 100
		log.Warn().Msgf("LocalOnlyDispatcher: dispatchChunkSize not set, defaulting to %d", chunkSize)
	}

	d.checker = graph.NewConcurrentChecker(d, concurrencyLimits.Check, chunkSize)
	d.expander = graph.NewConcurrentExpander(d)
	d.lookupSubjectsHandler = graph.NewConcurrentLookupSubjects(d, concurrencyLimits.LookupSubjects, chunkSize)
	d.lookupResourcesHandler2 = graph.NewCursoredLookupResources2(d, d, concurrencyLimits.LookupResources, chunkSize)

	return d
}

// NewDispatcher creates a dispatcher that consults with the graph and redispatches subproblems to
// the provided redispatcher.
func NewDispatcher(redispatcher dispatch.Dispatcher, concurrencyLimits ConcurrencyLimits, dispatchChunkSize uint16) dispatch.Dispatcher {
	concurrencyLimits = limitsOrDefaults(concurrencyLimits, defaultConcurrencyLimit)
	chunkSize := dispatchChunkSize
	if chunkSize == 0 {
		chunkSize = 100
		log.Warn().Msgf("Dispatcher: dispatchChunkSize not set, defaulting to %d", chunkSize)
	}

	checker := graph.NewConcurrentChecker(redispatcher, concurrencyLimits.Check, chunkSize)
	expander := graph.NewConcurrentExpander(redispatcher)
	lookupSubjectsHandler := graph.NewConcurrentLookupSubjects(redispatcher, concurrencyLimits.LookupSubjects, chunkSize)
	lookupResourcesHandler2 := graph.NewCursoredLookupResources2(redispatcher, redispatcher, concurrencyLimits.LookupResources, chunkSize)

	return &localDispatcher{
		checker:                 checker,
		expander:                expander,
		lookupSubjectsHandler:   lookupSubjectsHandler,
		lookupResourcesHandler2: lookupResourcesHandler2,
	}
}

type localDispatcher struct {
	checker                 *graph.ConcurrentChecker
	expander                *graph.ConcurrentExpander
	lookupSubjectsHandler   *graph.ConcurrentLookupSubjects
	lookupResourcesHandler2 *graph.CursoredLookupResources2
}

func (ld *localDispatcher) loadNamespace(ctx context.Context, nsName string, revision datastore.Revision) (*core.NamespaceDefinition, error) {
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(revision)

	// Load namespace and relation from the datastore
	ns, _, err := ds.ReadNamespaceByName(ctx, nsName)
	if err != nil {
		return nil, rewriteNamespaceError(err)
	}

	return ns, err
}

func (ld *localDispatcher) parseRevision(ctx context.Context, s string) (datastore.Revision, error) {
	ds := datastoremw.MustFromContext(ctx)
	return ds.RevisionFromString(s)
}

func (ld *localDispatcher) lookupRelation(_ context.Context, ns *core.NamespaceDefinition, relationName string) (*core.Relation, error) {
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

// DispatchCheck implements dispatch.Check interface
func (ld *localDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	resourceType := tuple.StringCoreRR(req.ResourceRelation)
	spanName := "DispatchCheck → " + resourceType + "@" + req.Subject.Namespace + "#" + req.Subject.Relation

	nodeID, err := nodeid.FromContext(ctx)
	if err != nil {
		log.Err(err).Msg("failed to get node ID")
	}

	ctx, span := tracer.Start(ctx, spanName, trace.WithAttributes(
		attribute.String("resource-type", resourceType),
		attribute.StringSlice("resource-ids", req.ResourceIds),
		attribute.String("subject", tuple.StringCoreONR(req.Subject)),
		attribute.String("node-id", nodeID),
	))
	defer span.End()

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		if req.Debug != v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING {
			return &v1.DispatchCheckResponse{
				Metadata: &v1.ResponseMeta{
					DispatchCount: 0,
				},
			}, rewriteError(ctx, err)
		}

		// NOTE: we return debug information here to ensure tooling can see the cycle.
		nodeID, nerr := nodeid.FromContext(ctx)
		if nerr != nil {
			log.Err(nerr).Msg("failed to get nodeID from context")
		}

		return &v1.DispatchCheckResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: 0,
				DebugInfo: &v1.DebugInformation{
					Check: &v1.CheckDebugTrace{
						Request:  req,
						SourceId: nodeID,
					},
				},
			},
		}, rewriteError(ctx, err)
	}

	revision, err := ld.parseRevision(ctx, req.Metadata.AtRevision)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, rewriteError(ctx, err)
	}

	ns, err := ld.loadNamespace(ctx, req.ResourceRelation.Namespace, revision)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, rewriteError(ctx, err)
	}

	relation, err := ld.lookupRelation(ctx, ns, req.ResourceRelation.Relation)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, rewriteError(ctx, err)
	}

	// If the relation is aliasing another one and the subject does not have the same type as
	// resource, load the aliased relation and dispatch to it. We cannot use the alias if the
	// resource and subject types are the same because a check on the *exact same* resource and
	// subject must pass, and we don't know how many intermediate steps may hit that case.
	if relation.AliasingRelation != "" && req.ResourceRelation.Namespace != req.Subject.Namespace {
		relation, err := ld.lookupRelation(ctx, ns, relation.AliasingRelation)
		if err != nil {
			return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, rewriteError(ctx, err)
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
				CheckHints:  req.CheckHints,
			},
			Revision:             revision,
			OriginalRelationName: req.ResourceRelation.Relation,
		}

		resp, err := ld.checker.Check(ctx, validatedReq, relation)
		return resp, rewriteError(ctx, err)
	}

	resp, err := ld.checker.Check(ctx, graph.ValidatedCheckRequest{
		DispatchCheckRequest: req,
		Revision:             revision,
	}, relation)
	return resp, rewriteError(ctx, err)
}

// DispatchExpand implements dispatch.Expand interface
func (ld *localDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	nodeID, err := nodeid.FromContext(ctx)
	if err != nil {
		log.Err(err).Msg("failed to get node ID")
	}

	ctx, span := tracer.Start(ctx, "DispatchExpand", trace.WithAttributes(
		attribute.String("start", tuple.StringCoreONR(req.ResourceAndRelation)),
		attribute.String("node-id", nodeID),
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

func (ld *localDispatcher) DispatchLookupResources2(
	req *v1.DispatchLookupResources2Request,
	stream dispatch.LookupResources2Stream,
) error {
	nodeID, err := nodeid.FromContext(stream.Context())
	if err != nil {
		log.Err(err).Msg("failed to get node ID")
	}

	ctx, span := tracer.Start(stream.Context(), "DispatchLookupResources2", trace.WithAttributes(
		attribute.String("resource-type", tuple.StringCoreRR(req.ResourceRelation)),
		attribute.String("subject-type", tuple.StringCoreRR(req.SubjectRelation)),
		attribute.StringSlice("subject-ids", req.SubjectIds),
		attribute.String("terminal-subject", tuple.StringCoreONR(req.TerminalSubject)),
		attribute.String("node-id", nodeID),
	))
	defer span.End()

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return err
	}

	revision, err := ld.parseRevision(ctx, req.Metadata.AtRevision)
	if err != nil {
		return err
	}

	return ld.lookupResourcesHandler2.LookupResources2(
		graph.ValidatedLookupResources2Request{
			DispatchLookupResources2Request: req,
			Revision:                        revision,
		},
		dispatch.StreamWithContext(ctx, stream),
	)
}

// DispatchLookupSubjects implements dispatch.LookupSubjects interface
func (ld *localDispatcher) DispatchLookupSubjects(
	req *v1.DispatchLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
) error {
	nodeID, err := nodeid.FromContext(stream.Context())
	if err != nil {
		log.Err(err).Msg("failed to get node ID")
	}

	resourceType := tuple.StringCoreRR(req.ResourceRelation)
	subjectRelation := tuple.StringCoreRR(req.SubjectRelation)
	spanName := "DispatchLookupSubjects → " + resourceType + "@" + subjectRelation

	ctx, span := tracer.Start(stream.Context(), spanName, trace.WithAttributes(
		attribute.String("resource-type", resourceType),
		attribute.String("subject-type", subjectRelation),
		attribute.StringSlice("resource-ids", req.ResourceIds),
		attribute.String("node-id", nodeID),
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

func (ld *localDispatcher) ReadyState() dispatch.ReadyState {
	return dispatch.ReadyState{IsReady: true}
}

func rewriteNamespaceError(original error) error {
	nsNotFound := datastore.NamespaceNotFoundError{}

	switch {
	case errors.As(original, &nsNotFound):
		return NewNamespaceNotFoundErr(nsNotFound.NotFoundNamespaceName())
	case errors.As(original, &NamespaceNotFoundError{}):
		fallthrough
	case errors.As(original, &RelationNotFoundError{}):
		return original
	default:
		return fmt.Errorf(errDispatch, original)
	}
}

// rewriteError transforms graph errors into a gRPC Status
func rewriteError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	// Check if the error can be directly used.
	if st, ok := status.FromError(err); ok {
		return st.Err()
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "%s", err)
	case errors.Is(err, context.Canceled):
		err := context.Cause(ctx)
		if err != nil {
			if _, ok := status.FromError(err); ok {
				return err
			}
		}

		return status.Errorf(codes.Canceled, "%s", err)
	default:
		log.Ctx(ctx).Err(err).Msg("received unexpected graph error")
		return err
	}
}

var emptyMetadata = &v1.ResponseMeta{
	DispatchCount: 0,
}
