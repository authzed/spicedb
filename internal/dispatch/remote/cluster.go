package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/consistent"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	log "github.com/authzed/spicedb/internal/logging"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// defaultStartingPrimaryHedgingDelay is the delay used by default for primary calls (when secondaries are available),
// before statistics are available to determine the actual delay.
const defaultStartingPrimaryHedgingDelay = 1 * time.Millisecond

// supportsSecondaries lists the request kinds that may be dispatched to secondaries.
var supportsSecondaries = []string{"check", "lookupresources", "lookupsubjects"}

type ClusterClient interface {
	DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest, opts ...grpc.CallOption) (*v1.DispatchCheckResponse, error)
	DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest, opts ...grpc.CallOption) (*v1.DispatchExpandResponse, error)
	DispatchLookupResources2(ctx context.Context, in *v1.DispatchLookupResources2Request, opts ...grpc.CallOption) (v1.DispatchService_DispatchLookupResources2Client, error)
	DispatchLookupResources3(ctx context.Context, in *v1.DispatchLookupResources3Request, opts ...grpc.CallOption) (v1.DispatchService_DispatchLookupResources3Client, error)
	DispatchLookupSubjects(ctx context.Context, in *v1.DispatchLookupSubjectsRequest, opts ...grpc.CallOption) (v1.DispatchService_DispatchLookupSubjectsClient, error)
	DispatchQueryPlan(ctx context.Context, in *v1.DispatchQueryPlanRequest, opts ...grpc.CallOption) (v1.DispatchService_DispatchQueryPlanClient, error)
}

type ClusterDispatcherConfig struct {
	// KeyHandler is the handler to use for generating dispatch hash ring keys.
	KeyHandler keys.Handler

	// DispatchOverallTimeout is the maximum duration of a dispatched request
	// before it should timeout.
	DispatchOverallTimeout time.Duration

	// SecondaryDispatch holds the secondary dispatchers, by name.
	SecondaryDispatch map[string]SecondaryDispatch

	// SecondaryDispatchExprs holds, by request type, the expression that selects the
	// secondary dispatchers (and the strategy used to race them) for a request.
	SecondaryDispatchExprs map[string]*DispatchExpr

	// StartingPrimaryHedgingDelay is the primary's hedging delay used until enough
	// statistics are available. Defaults to defaultStartingPrimaryHedgingDelay.
	StartingPrimaryHedgingDelay time.Duration
}

// SecondaryDispatch defines a struct holding a client and its name for secondary
// dispatching.
type SecondaryDispatch struct {
	// Name is the name of the secondary dispatcher.
	Name string

	// Client is the client to use for dispatching to the secondary.
	Client ClusterClient

	// MaximumHedgingDelay is the maximum delay that the primary will wait before dispatching
	// when this secondary is active.
	MaximumPrimaryHedgingDelay time.Duration
}

// NewClusterDispatcher creates a dispatcher implementation that uses the provided client
// to dispatch requests to peer nodes in the cluster.
func NewClusterDispatcher(client ClusterClient, conn *grpc.ClientConn, config ClusterDispatcherConfig) (dispatch.Dispatcher, error) {
	keyHandler := config.KeyHandler
	if keyHandler == nil {
		keyHandler = &keys.DirectKeyHandler{}
	}

	dispatchOverallTimeout := config.DispatchOverallTimeout
	if dispatchOverallTimeout <= 0 {
		dispatchOverallTimeout = 60 * time.Second
	}

	startingPrimaryHedgingDelay := config.StartingPrimaryHedgingDelay
	if startingPrimaryHedgingDelay <= 0 {
		startingPrimaryHedgingDelay = defaultStartingPrimaryHedgingDelay
	}

	stats, err := newRaceStats(startingPrimaryHedgingDelay)
	if err != nil {
		return nil, err
	}

	return &clusterDispatcher{
		clusterClient:          client,
		conn:                   conn,
		keyHandler:             keyHandler,
		dispatchOverallTimeout: dispatchOverallTimeout,
		secondaryDispatch:      config.SecondaryDispatch,
		secondaryDispatchExprs: config.SecondaryDispatchExprs,
		stats:                  stats,
	}, nil
}

type clusterDispatcher struct {
	clusterClient          ClusterClient
	conn                   *grpc.ClientConn
	keyHandler             keys.Handler
	dispatchOverallTimeout time.Duration

	secondaryDispatch      map[string]SecondaryDispatch
	secondaryDispatchExprs map[string]*DispatchExpr
	stats                  *raceStats
}

// dispatchPlanFor evaluates the dispatch expression configured for reqKey. It returns false
// when there is no expression for reqKey or its evaluation failed: the request then goes to
// the primary only.
func (cr *clusterDispatcher) dispatchPlanFor(ctx context.Context, reqKey string, req any) (dispatchPlan, bool) {
	expr, ok := cr.secondaryDispatchExprs[reqKey]
	if !ok {
		return dispatchPlan{}, false
	}

	plan, err := runDispatchExpr(expr, req)
	if err != nil {
		metricsFor(reqKey).expressionEvalFailures.Inc()
		log.Ctx(ctx).Warn().Err(err).Msg("error when trying to evaluate the dispatch expression")
		return dispatchPlan{}, false
	}
	return plan, true
}

// resolveParticipants builds the participants of a race: the primary (when allowPrimary),
// then each named secondary. Unknown and repeated names are dropped. It also returns the
// number of secondaries.
func (cr *clusterDispatcher) resolveParticipants(ctx context.Context, names []string, allowPrimary bool) ([]participant, int) {
	participants := make([]participant, 0, len(names)+1)
	if allowPrimary {
		participants = append(participants, participant{
			name:    primaryDispatcher,
			primary: true,
			client:  cr.clusterClient,
		})
	}

	secondaries := 0
	for _, name := range names {
		secondary, ok := cr.secondaryDispatch[name]
		if !ok {
			log.Ctx(ctx).Warn().Str("secondary-dispatcher-name", name).Msg("received unknown secondary dispatcher")
			continue
		}
		if slices.ContainsFunc(participants, func(p participant) bool { return p.name == secondary.Name }) {
			continue
		}

		participants = append(participants, participant{
			name:            secondary.Name,
			client:          secondary.Client,
			maxPrimaryDelay: secondary.MaximumPrimaryHedgingDelay,
		})
		secondaries++
	}
	return participants, secondaries
}

func (cr *clusterDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	requestKey, err := cr.keyHandler.CheckDispatchKey(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	ctx = context.WithValue(ctx, consistent.CtxKey, requestKey)

	resp, err := dispatchSyncRequest(
		ctx,
		cr,
		"check",
		req,
		tuple.FromCoreRelationReference(req.ResourceRelation),
		tuple.RR(req.Subject.Namespace, req.Subject.Relation),
		func(ctx context.Context, client ClusterClient) (*v1.DispatchCheckResponse, error) {
			resp, err := client.DispatchCheck(ctx, req)
			if err != nil {
				return resp, err
			}

			err = adjustMetadataForDispatch(resp.Metadata)
			return resp, err
		})
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: requestFailureMetadata}, err
	}

	return resp, err
}

type requestMessage interface {
	zerolog.LogObjectMarshaler

	GetMetadata() *v1.ResolverMeta
}

type responseMessage interface {
	proto.Message

	GetMetadata() *v1.ResponseMeta
}

type streamingRequestMessage interface {
	GetResourceRelation() *corev1.RelationReference
	GetSubjectRelation() *corev1.RelationReference
}

// dispatchSyncRequest handles the dispatch of a unary request. If the request's dispatch
// expression selects secondaries, the primary and the secondaries are raced according to the
// expression's strategy; otherwise the request goes to the primary only.
func dispatchSyncRequest[Q requestMessage, S responseMessage](
	ctx context.Context,
	cr *clusterDispatcher,
	reqKey string,
	req Q,
	resourceTypeAndRelation tuple.RelationReference,
	subjectTypeAndRelation tuple.RelationReference,
	handler func(context.Context, ClusterClient) (S, error),
) (S, error) {
	withTimeout, cancelFn := context.WithTimeout(ctx, cr.dispatchOverallTimeout)
	defer cancelFn()

	// If no secondary dispatches are defined, just invoke directly.
	if len(cr.secondaryDispatchExprs) == 0 || len(cr.secondaryDispatch) == 0 {
		return handler(withTimeout, cr.clusterClient)
	}

	log.Ctx(ctx).Trace().Object("request", req).Msg("running dispatch expression")
	plan, ok := cr.dispatchPlanFor(ctx, reqKey, req)
	if !ok {
		return handler(withTimeout, cr.clusterClient)
	}

	participants, secondaries := cr.resolveParticipants(ctx, plan.secondaries, true)
	if secondaries == 0 {
		log.Ctx(ctx).Trace().Str("request-key", reqKey).Msg("no secondary dispatches selected, running primary dispatch")
		countPrimaryDispatch(reqKey, false)
		return handler(withTimeout, cr.clusterClient)
	}

	return runUnaryRace(withTimeout, raceRequest{
		reqKey:       reqKey,
		resource:     resourceTypeAndRelation,
		subject:      subjectTypeAndRelation,
		participants: participants,
		stats:        cr.stats,
	}, plan.strategy, handler)
}

const (
	secondaryCursorPrefix = "$$secondary:"
	primaryDispatcher     = "$primary"
)

// publishPrimary publishes the results of a primary dispatch that runs without secondaries.
func publishPrimary[R any](ctx context.Context, client receiver[R], reqKey string, stream dispatch.Stream[R]) error {
	isFirstResult := true
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		result, err := client.Recv()
		if errors.Is(err, io.EOF) {
			if isFirstResult {
				dispatchCounter.WithLabelValues(reqKey, primaryDispatcher).Add(1)
			}
			return nil
		} else if err != nil {
			return err
		}

		if isFirstResult {
			dispatchCounter.WithLabelValues(reqKey, primaryDispatcher).Add(1)
		}
		isFirstResult = false

		serr := stream.Publish(result)
		if serr != nil {
			return serr
		}
	}
}

// stripCursorDispatcherTag removes the secondary tag from the request's cursor, if any, and returns the
// name of the dispatcher that produced the cursor, or "" if the request has no cursor. LR2 cursors carry
// the tag at the front, while LR3 cursors are stacks whose head is the last section. Every streaming
// request type must be handled here, so that none skips the cursor lock unnoticed.
func stripCursorDispatcherTag(req any) (string, error) {
	switch r := req.(type) {
	case *v1.DispatchLookupResources3Request:
		last := len(r.OptionalCursor) - 1
		if last < 0 {
			return "", nil
		}
		if name, ok := strings.CutPrefix(r.OptionalCursor[last], secondaryCursorPrefix); ok {
			r.OptionalCursor = r.OptionalCursor[:last]
			return name, nil
		}
		return primaryDispatcher, nil

	case *v1.DispatchLookupResources2Request:
		cursor := r.GetOptionalCursor()
		if len(cursor.GetSections()) == 0 {
			return "", nil
		}
		if name, ok := strings.CutPrefix(cursor.Sections[0], secondaryCursorPrefix); ok {
			cursor.Sections = cursor.Sections[1:]
			return name, nil
		}
		return primaryDispatcher, nil

	case *v1.DispatchLookupSubjectsRequest:
		return "", nil

	default:
		return "", spiceerrors.MustBugf("unhandled streaming dispatch request type %T", req)
	}
}

// tagCursorWithSecondary marks the result's cursors as produced by the named secondary dispatcher. Like
// stripCursorDispatcherTag, it must handle every streaming response type, plus the unary response types
// that run on the race engine.
func tagCursorWithSecondary(result any, secondaryDispatchName string) error {
	switch r := result.(type) {
	case *v1.DispatchLookupResources3Response:
		tag := secondaryCursorPrefix + secondaryDispatchName
		for _, item := range r.Items {
			if len(item.AfterResponseCursorSections) > 0 {
				item.AfterResponseCursorSections = append(item.AfterResponseCursorSections, tag)
			}
		}
		return nil

	case *v1.DispatchLookupResources2Response:
		if cursor := r.GetAfterResponseCursor(); cursor != nil {
			cursor.Sections = append([]string{secondaryCursorPrefix + secondaryDispatchName}, cursor.Sections...)
		}
		return nil

	case *v1.DispatchLookupSubjectsResponse, *v1.DispatchCheckResponse:
		return nil

	default:
		return spiceerrors.MustBugf("unhandled streaming dispatch response type %T", result)
	}
}

// dispatchStreamingRequest handles the dispatching of a streaming request. If the request's
// cursor was produced by a secondary, the request goes to that secondary only. Otherwise, if
// the request's dispatch expression selects secondaries, the primary and the secondaries are
// raced according to the expression's strategy; if not, the request goes to the primary only.
func dispatchStreamingRequest[Q streamingRequestMessage, R any](
	ctx context.Context,
	cr *clusterDispatcher,
	reqKey string,
	req Q,
	stream dispatch.Stream[R],
	handler func(context.Context, ClusterClient) (receiver[R], error),
) error {
	ctxWithTimeout, cancelFn := context.WithTimeout(ctx, cr.dispatchOverallTimeout)
	defer cancelFn()

	// If no secondary dispatches are defined, just invoke directly.
	if len(cr.secondaryDispatchExprs) == 0 || len(cr.secondaryDispatch) == 0 {
		client, err := handler(ctxWithTimeout, cr.clusterClient)
		if err != nil {
			return err
		}
		return publishPrimary(ctxWithTimeout, client, reqKey, stream)
	}

	cursorLockedDispatcherName, err := stripCursorDispatcherTag(req)
	if err != nil {
		return err
	}

	var secondaryNames []string
	var strategy secondaryStrategy = hedgedStrategy{}
	allowPrimary := true
	switch cursorLockedDispatcherName {
	case primaryDispatcher:
		// An untagged cursor was produced by the primary, so no secondary can resume it.

	case "":
		if plan, ok := cr.dispatchPlanFor(ctxWithTimeout, reqKey, req); ok {
			secondaryNames = plan.secondaries
			strategy = plan.strategy
		}

	default:
		// The cursor is locked to the secondary that produced it. Only that secondary runs,
		// so the request's strategy has no effect.
		allowPrimary = false
		secondaryNames = []string{cursorLockedDispatcherName}
		strategy = cursorLockedStrategy{}
	}

	participants, secondaries := cr.resolveParticipants(ctxWithTimeout, secondaryNames, allowPrimary)

	// If no secondary dispatches are selected, just invoke directly.
	if secondaries == 0 {
		if !allowPrimary {
			return errors.New("cursor locked to unknown secondary dispatcher")
		}

		client, err := handler(ctxWithTimeout, cr.clusterClient)
		if err != nil {
			return err
		}
		return publishPrimary(ctxWithTimeout, client, reqKey, stream)
	}

	return runRace(ctxWithTimeout, raceRequest{
		reqKey:       reqKey,
		resource:     tuple.FromCoreRelationReference(req.GetResourceRelation()),
		subject:      tuple.FromCoreRelationReference(req.GetSubjectRelation()),
		participants: participants,
		stats:        cr.stats,
	}, strategy, handler, stream)
}

func adjustMetadataForDispatch(metadata *v1.ResponseMeta) error {
	if metadata == nil {
		return spiceerrors.MustBugf("received a nil metadata")
	}

	// NOTE: We only add 1 to the dispatch count if it was not already handled by the downstream dispatch,
	// which will only be the case in a fully cached or further undispatched call.
	if metadata.DispatchCount == 0 {
		metadata.DispatchCount++
	}

	return nil
}

func (cr *clusterDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	requestKey, err := cr.keyHandler.ExpandDispatchKey(ctx, req)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	ctx = context.WithValue(ctx, consistent.CtxKey, requestKey)

	withTimeout, cancelFn := context.WithTimeout(ctx, cr.dispatchOverallTimeout)
	defer cancelFn()

	resp, err := cr.clusterClient.DispatchExpand(withTimeout, req)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: requestFailureMetadata}, err
	}

	err = adjustMetadataForDispatch(resp.Metadata)
	return resp, err
}

func (cr *clusterDispatcher) DispatchLookupResources2(
	req *v1.DispatchLookupResources2Request,
	stream dispatch.LookupResources2Stream,
) error {
	requestKey, err := cr.keyHandler.LookupResources2DispatchKey(stream.Context(), req)
	if err != nil {
		return err
	}

	ctx := context.WithValue(stream.Context(), consistent.CtxKey, requestKey)
	stream = dispatch.StreamWithContext(ctx, stream)

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return err
	}

	return dispatchStreamingRequest(ctx, cr, "lookupresources", req, stream,
		func(ctx context.Context, client ClusterClient) (receiver[*v1.DispatchLookupResources2Response], error) {
			return client.DispatchLookupResources2(ctx, req)
		})
}

func (cr *clusterDispatcher) DispatchLookupResources3(
	req *v1.DispatchLookupResources3Request,
	stream dispatch.LookupResources3Stream,
) error {
	requestKey, err := cr.keyHandler.LookupResources3DispatchKey(stream.Context(), req)
	if err != nil {
		return err
	}

	ctx := context.WithValue(stream.Context(), consistent.CtxKey, requestKey)
	stream = dispatch.StreamWithContext(ctx, stream)

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return err
	}

	return dispatchStreamingRequest(ctx, cr, "lookupresources", req, stream,
		func(ctx context.Context, client ClusterClient) (receiver[*v1.DispatchLookupResources3Response], error) {
			return client.DispatchLookupResources3(ctx, req)
		})
}

func (cr *clusterDispatcher) DispatchLookupSubjects(
	req *v1.DispatchLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
) error {
	requestKey, err := cr.keyHandler.LookupSubjectsDispatchKey(stream.Context(), req)
	if err != nil {
		return err
	}

	ctx := context.WithValue(stream.Context(), consistent.CtxKey, requestKey)
	stream = dispatch.StreamWithContext(ctx, stream)

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return err
	}

	return dispatchStreamingRequest(ctx, cr, "lookupsubjects", req, stream,
		func(ctx context.Context, client ClusterClient) (receiver[*v1.DispatchLookupSubjectsResponse], error) {
			return client.DispatchLookupSubjects(ctx, req)
		})
}

// LookupPlanCheck is a no-op on the cluster (remote) dispatcher. Probing a
// remote cache would require its own RPC, which would defeat the purpose of
// avoiding the heavy DispatchQueryPlan call; the remote receiver consults its
// own caching layer when it gets a full request. Future work could expose a
// cheap "PlanCheckLookup" RPC if remote cache amplification is needed.
func (cr *clusterDispatcher) LookupPlanCheck(_ context.Context, _ dispatch.PlanCheckLookup) (*v1.ResultPath, bool, error) {
	return nil, false, nil
}

func (cr *clusterDispatcher) DispatchQueryPlan(req *v1.DispatchQueryPlanRequest, stream dispatch.PlanStream) error {
	var requestKey []byte
	var err error

	// Select dispatch key based on operation type.
	switch req.Operation {
	case v1.PlanOperation_PLAN_OPERATION_CHECK:
		requestKey, err = cr.keyHandler.PlanCheckDispatchKey(stream.Context(), req)
	case v1.PlanOperation_PLAN_OPERATION_LOOKUP_RESOURCES:
		requestKey, err = cr.keyHandler.PlanLookupResourcesDispatchKey(stream.Context(), req)
	case v1.PlanOperation_PLAN_OPERATION_LOOKUP_SUBJECTS:
		requestKey, err = cr.keyHandler.PlanLookupSubjectsDispatchKey(stream.Context(), req)
	default:
		return fmt.Errorf("unknown plan operation: %v", req.Operation)
	}
	if err != nil {
		return err
	}

	ctx := context.WithValue(stream.Context(), consistent.CtxKey, requestKey)

	withTimeout, cancelFn := context.WithTimeout(ctx, cr.dispatchOverallTimeout)
	defer cancelFn()

	client, err := cr.clusterClient.DispatchQueryPlan(withTimeout, req)
	if err != nil {
		return err
	}

	// TODO: route through dispatchStreamingRequest once DispatchQueryPlanRequest
	// implements streamingRequestMessage, so plan can also benefit from hedging
	// and secondary-dispatch.
	reqKind := "queryplan_" + dispatch.PlanOperationLabel(req.Operation)
	firstResult := true
	for {
		resp, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if firstResult {
			dispatchCounter.WithLabelValues(reqKind, primaryDispatcher).Inc()
			firstResult = false
		}
		if err := stream.Publish(resp); err != nil {
			return err
		}
	}
}

func (cr *clusterDispatcher) Close() error {
	if cr.conn != nil {
		return cr.conn.Close()
	}
	return nil
}

// ReadyState returns whether the underlying dispatch connection is available
func (cr *clusterDispatcher) ReadyState() dispatch.ReadyState {
	state := cr.conn.GetState()
	log.Trace().Interface("connection-state", state).Msg("checked if cluster dispatcher is ready")
	return dispatch.ReadyState{
		IsReady: state == connectivity.Ready || state == connectivity.Idle || state == connectivity.Connecting,
		Message: fmt.Sprintf("found expected state when trying to connect to cluster: %v", state),
	}
}

// Always verify that we implement the interface
var _ dispatch.Dispatcher = &clusterDispatcher{}

var emptyMetadata = &v1.ResponseMeta{
	DispatchCount: 0,
}

var requestFailureMetadata = &v1.ResponseMeta{
	DispatchCount: 1,
}
