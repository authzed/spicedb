package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/caio/go-tdigest/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"go.uber.org/atomic"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
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

var dispatchCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_handler_total",
	Help:      "which dispatcher handled a request",
}, []string{"request_kind", "handler_name"})

var hedgeWaitHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_hedge_wait_duration_seconds",
	Help:      "distribution in seconds of calculated wait time for hedging requests to the primary dispatcher when a secondary is active.",
	Buckets:   []float64{0.001, 0.002, 0.003, 0.005, 0.01, 0.02, 0.05, 0.1, 0.3, 0.5, 1, 10},
}, []string{"rpc"})

var hedgeActualWaitHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_hedge_actual_wait_duration_seconds",
	Help:      "distribution in seconds of actual wait time for hedging requests to the primary dispatcher when a secondary is active.",
	Buckets:   []float64{0.001, 0.002, 0.003, 0.005, 0.01, 0.02, 0.05, 0.1, 0.3, 0.5, 1, 10},
}, []string{"rpc"})

var primaryDispatch = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_primary_total",
	Help:      "indicates what was the outcome of a scheduled primary dispatch: dispatched, or cancelled",
}, []string{"cancelled", "rpc"})

// defaultStartingPrimaryHedgingDelay is the delay used by default for primary calls (when secondaries are available),
// before statistics are available to determine the actual delay.
const defaultStartingPrimaryHedgingDelay = 1 * time.Millisecond

// defaultHedgerQuantile is the default quantile used to determine the hedging delay for primary calls.
const defaultHedgerQuantile = 0.9

// minimumDigestCount is the minimum number of samples required in the digest before it can be used
// to determine the configured percentile.
const minimumDigestCount = 10

const defaultTDigestCompression = float64(1000)

var supportsSecondaries = []string{"check", "lookupresources", "lookupsubjects"}

func init() {
	prometheus.MustRegister(dispatchCounter)
	prometheus.MustRegister(hedgeWaitHistogram)
	prometheus.MustRegister(hedgeActualWaitHistogram)
	prometheus.MustRegister(primaryDispatch)
}

type ClusterClient interface {
	DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest, opts ...grpc.CallOption) (*v1.DispatchCheckResponse, error)
	DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest, opts ...grpc.CallOption) (*v1.DispatchExpandResponse, error)
	DispatchLookupResources2(ctx context.Context, in *v1.DispatchLookupResources2Request, opts ...grpc.CallOption) (v1.DispatchService_DispatchLookupResources2Client, error)
	DispatchLookupResources3(ctx context.Context, in *v1.DispatchLookupResources3Request, opts ...grpc.CallOption) (v1.DispatchService_DispatchLookupResources3Client, error)
	DispatchLookupSubjects(ctx context.Context, in *v1.DispatchLookupSubjectsRequest, opts ...grpc.CallOption) (v1.DispatchService_DispatchLookupSubjectsClient, error)
}

type ClusterDispatcherConfig struct {
	// KeyHandler is the handler to use for generating dispatch hash ring keys.
	KeyHandler keys.Handler

	// DispatchOverallTimeout is the maximum duration of a dispatched request
	// before it should timeout.
	DispatchOverallTimeout time.Duration
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
func NewClusterDispatcher(client ClusterClient, conn *grpc.ClientConn, config ClusterDispatcherConfig, secondaryDispatch map[string]SecondaryDispatch, secondaryDispatchExprs map[string]*DispatchExpr, startingPrimaryHedgingDelay time.Duration) (dispatch.Dispatcher, error) {
	keyHandler := config.KeyHandler
	if keyHandler == nil {
		keyHandler = &keys.DirectKeyHandler{}
	}

	dispatchOverallTimeout := config.DispatchOverallTimeout
	if dispatchOverallTimeout <= 0 {
		dispatchOverallTimeout = 60 * time.Second
	}

	if startingPrimaryHedgingDelay <= 0 {
		startingPrimaryHedgingDelay = defaultStartingPrimaryHedgingDelay
	}

	secondaryInitialResponseDigests := make(map[string]*digestAndLock, len(supportsSecondaries))
	for _, requestKey := range supportsSecondaries {
		digest, err := tdigest.New(tdigest.Compression(defaultTDigestCompression))
		if err != nil {
			return nil, err
		}

		secondaryInitialResponseDigests[requestKey] = &digestAndLock{
			digest:                      digest,
			startingPrimaryHedgingDelay: startingPrimaryHedgingDelay,
			lock:                        sync.RWMutex{},
		}
	}

	return &clusterDispatcher{
		clusterClient:                   client,
		conn:                            conn,
		keyHandler:                      keyHandler,
		dispatchOverallTimeout:          dispatchOverallTimeout,
		secondaryDispatch:               secondaryDispatch,
		secondaryDispatchExprs:          secondaryDispatchExprs,
		secondaryInitialResponseDigests: secondaryInitialResponseDigests,
		supportedResourceSubjectTracker: newSupportedResourceSubjectTracker(),
	}, nil
}

type clusterDispatcher struct {
	clusterClient          ClusterClient
	conn                   *grpc.ClientConn
	keyHandler             keys.Handler
	dispatchOverallTimeout time.Duration

	secondaryDispatch               map[string]SecondaryDispatch
	secondaryDispatchExprs          map[string]*DispatchExpr
	secondaryInitialResponseDigests map[string]*digestAndLock
	supportedResourceSubjectTracker *supportedResourceSubjectTracker
}

// digestAndLock is a struct that holds a TDigest and a lock to protect it.
type digestAndLock struct {
	digest                      *tdigest.TDigest // GUARDED_BY(lock)
	startingPrimaryHedgingDelay time.Duration
	lock                        sync.RWMutex
}

// getWaitTime returns the configured percentile of the digest, or a default value if the digest is empty.
func (dal *digestAndLock) getWaitTime(maximumHedgingDelay time.Duration) time.Duration {
	dal.lock.RLock()
	milliseconds := dal.digest.Quantile(defaultHedgerQuantile)
	count := dal.digest.Count()
	dal.lock.RUnlock()

	if milliseconds <= 0 || count < minimumDigestCount {
		return dal.startingPrimaryHedgingDelay
	}

	waitTime := min(time.Duration(milliseconds)*time.Millisecond, maximumHedgingDelay)
	return waitTime
}

func (dal *digestAndLock) addResultTime(ctx context.Context, duration time.Duration) {
	if dal.lock.TryLock() {
		err := dal.digest.Add(float64(duration.Milliseconds()))
		dal.lock.Unlock()
		if err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error when trying to add result time to digest")
		}
	}
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

type respTuple[S responseMessage] struct {
	resp S
	err  error
}

type secondaryRespTuple[S responseMessage] struct {
	handlerName string
	resp        S
}

// dispatchSyncRequest handles the dispatch of a unary request.
// It first attempts to use the secondary dispatchers, if any are defined and match,
// before falling back to the primary dispatcher.
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

	if len(cr.secondaryDispatchExprs) == 0 || len(cr.secondaryDispatch) == 0 {
		return handler(withTimeout, cr.clusterClient)
	}

	// If no secondary dispatches are defined, just invoke directly.
	expr, ok := cr.secondaryDispatchExprs[reqKey]
	if !ok {
		return handler(withTimeout, cr.clusterClient)
	}

	// Run the dispatch expression to find the name(s) of the secondary dispatchers to use, if any.
	log.Ctx(ctx).Trace().Object("request", req).Msg("running dispatch expression")
	secondaryDispatcherNames, err := RunDispatchExpr(expr, req)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("error when trying to evaluate the dispatch expression")
		return handler(withTimeout, cr.clusterClient)
	}

	if len(secondaryDispatcherNames) == 0 {
		// If no secondary dispatches are defined, just invoke directly.
		log.Ctx(ctx).Trace().Str("request-key", reqKey).Msg("no secondary dispatches defined, running primary dispatch")
		primaryDispatch.WithLabelValues("false", reqKey).Inc()
		return handler(withTimeout, cr.clusterClient)
	}

	var maximumHedgingDelay time.Duration
	for _, secondaryDispatchName := range secondaryDispatcherNames {
		secondary, ok := cr.secondaryDispatch[secondaryDispatchName]
		if !ok {
			log.Ctx(ctx).Warn().Str("secondary-dispatcher-name", secondaryDispatchName).Msg("received unknown secondary dispatcher")
			continue
		}

		maximumHedgingDelay = max(maximumHedgingDelay, secondary.MaximumPrimaryHedgingDelay)
	}

	// Otherwise invoke in parallel with any secondary matches.
	primaryResultChan := make(chan respTuple[S], 1)
	secondaryResultChan := make(chan secondaryRespTuple[S], len(cr.secondaryDispatch))

	primarySleeper := cr.newPrimarySleeper(reqKey, resourceTypeAndRelation, subjectTypeAndRelation, maximumHedgingDelay)

	// Run the main dispatch.
	go func() {
		// Have the main dispatch wait some time before returning, to allow the secondaries to
		// potentially return first.
		primarySleeper.sleep(withTimeout)

		log.Ctx(ctx).Trace().Msg("running primary dispatch after wait")
		select {
		case <-withTimeout.Done():
			log.Ctx(ctx).Trace().Str("request-key", reqKey).Msg("primary dispatch timed out or was canceled")
			primaryDispatch.WithLabelValues("true", reqKey).Inc()
			return

		default:
			resp, err := handler(withTimeout, cr.clusterClient)
			log.Ctx(ctx).Trace().Str("request-key", reqKey).Msg("primary dispatch completed")
			primaryResultChan <- respTuple[S]{resp, err}
			primaryDispatch.WithLabelValues("false", reqKey).Inc()
		}
	}()

	for _, secondaryDispatchName := range secondaryDispatcherNames {
		secondary, ok := cr.secondaryDispatch[secondaryDispatchName]
		if !ok {
			log.Ctx(ctx).Warn().Str("secondary-dispatcher-name", secondaryDispatchName).Msg("received unknown secondary dispatcher")
			continue
		}

		go func() {
			select {
			case <-withTimeout.Done():
				log.Ctx(ctx).Trace().Str("secondary", secondary.Name).Msg("secondary dispatch timed out or was canceled")
				return

			default:
				log.Ctx(ctx).Trace().Str("secondary", secondary.Name).Msg("running secondary dispatch")
				startTime := time.Now()
				resp, err := handler(withTimeout, secondary.Client)
				endTime := time.Now()

				handlerDuration := endTime.Sub(startTime)
				if err != nil {
					// For secondary dispatches, ignore any errors, as only the primary will be handled in
					// that scenario.
					log.Ctx(ctx).Trace().Stringer("duration", handlerDuration).Str("secondary", secondary.Name).Err(err).Msg("got ignored secondary dispatch error")
					cr.supportedResourceSubjectTracker.updateForError(err)

					// Cancel the primary's sleep if it is still sleeping, so it can immediately being its own work.
					primarySleeper.cancelSleep()
					return
				}

				log.Ctx(ctx).Trace().Stringer("duration", handlerDuration).Str("secondary", secondary.Name).Msg("secondary dispatch completed")
				go cr.supportedResourceSubjectTracker.updateForSuccess(resourceTypeAndRelation, subjectTypeAndRelation)
				cr.secondaryInitialResponseDigests[reqKey].addResultTime(ctx, handlerDuration)
				secondaryResultChan <- secondaryRespTuple[S]{resp: resp, handlerName: secondary.Name}
			}
		}()
	}

	var foundError error
	select {
	case <-withTimeout.Done():
		return *new(S), errors.New("check dispatch has timed out")

	case r := <-primaryResultChan:
		if r.err == nil {
			dispatchCounter.WithLabelValues(reqKey, primaryDispatcher).Add(1)
			return r.resp, nil
		}

		// Otherwise, if an error was found, log it and we'll return after *all* the secondaries have run.
		// This allows an otherwise error-state to be handled by one of the secondaries.
		foundError = r.err

	case r := <-secondaryResultChan:
		dispatchCounter.WithLabelValues(reqKey, r.handlerName).Add(1)
		return r.resp, nil
	}

	dispatchCounter.WithLabelValues(reqKey, primaryDispatcher).Add(1)
	return *new(S), foundError
}

type requestMessageWithCursor interface {
	requestMessage
	GetOptionalCursor() *v1.Cursor
}

type responseMessageWithCursor interface {
	responseMessage
	GetAfterResponseCursor() *v1.Cursor
}

type receiver[S any] interface {
	Recv() (S, error)
	grpc.ClientStream
}

const (
	secondaryCursorPrefix = "$$secondary:"
	primaryDispatcher     = "$primary"
	noDispatcherResults   = "$$no_dispatcher_results"
)

func publishClient[R any](ctx context.Context, client receiver[R], reqKey string, stream dispatch.Stream[R], secondaryDispatchName string) error {
	isFirstResult := true
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		result, err := client.Recv()
		if errors.Is(err, io.EOF) {
			if isFirstResult {
				dispatchCounter.WithLabelValues(reqKey, secondaryDispatchName).Add(1)
			}
			return nil
		} else if err != nil {
			return err
		}

		if isFirstResult {
			dispatchCounter.WithLabelValues(reqKey, secondaryDispatchName).Add(1)
		}
		isFirstResult = false

		if secondaryDispatchName != primaryDispatcher {
			if supportsCursors, ok := any(result).(responseMessageWithCursor); ok {
				afterResponseCursor := supportsCursors.GetAfterResponseCursor()
				if afterResponseCursor == nil {
					return spiceerrors.MustBugf("received a nil after response cursor for secondary dispatch")
				}
				afterResponseCursor.Sections = append([]string{secondaryCursorPrefix + secondaryDispatchName}, afterResponseCursor.Sections...)
			}
		}

		serr := stream.Publish(result)
		if serr != nil {
			return serr
		}
	}
}

type ctxAndCancel struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// dispatchStreamingRequest handles the dispatching of a streaming request to the primary and any
// secondary dispatchers. Unlike dispatchSyncRequest, this will first attempt to dispatch
// from the allowed secondary dispatchers before falling back to the primary, rather than running
// them in parallel.
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
		return publishClient(ctxWithTimeout, client, reqKey, stream, primaryDispatcher)
	}

	// Check the cursor to see if the dispatch went to one of the secondary endpoints.
	cursorLockedSecondaryName := ""
	if cursorSupports, ok := any(req).(requestMessageWithCursor); ok {
		cursor := cursorSupports.GetOptionalCursor()
		if cursor != nil && len(cursor.Sections) > 0 {
			if after, ok0 := strings.CutPrefix(cursor.Sections[0], secondaryCursorPrefix); ok0 {
				cursorLockedSecondaryName = after
				cursor.Sections = cursor.Sections[1:]
			}
		}
	}

	// Find the set of valid secondary dispatchers, if any.
	validSecondaryDispatchers := make([]SecondaryDispatch, 0, len(cr.secondaryDispatch))
	allowPrimary := true
	if cursorLockedSecondaryName != "" {
		// If the cursor is locked to a known secondary, dispatch to it.
		allowPrimary = false
		if sd, ok := cr.secondaryDispatch[cursorLockedSecondaryName]; ok {
			validSecondaryDispatchers = append(validSecondaryDispatchers, sd)
		}
	} else if expr, ok := cr.secondaryDispatchExprs[reqKey]; ok {
		dispatcherNames, err := RunDispatchExpr(expr, req)
		if err != nil {
			log.Ctx(ctxWithTimeout).Warn().Err(err).Msg("error when trying to evaluate the dispatch expression")
		} else {
			for _, secondaryDispatchName := range dispatcherNames {
				if sd, ok := cr.secondaryDispatch[secondaryDispatchName]; ok {
					validSecondaryDispatchers = append(validSecondaryDispatchers, sd)
				}
			}
		}
	}

	// If no secondary dispatches are defined, just invoke directly.
	if len(validSecondaryDispatchers) == 0 {
		if !allowPrimary {
			return errors.New("cursor locked to unknown secondary dispatcher")
		}

		client, err := handler(ctxWithTimeout, cr.clusterClient)
		if err != nil {
			return err
		}
		return publishClient(ctxWithTimeout, client, reqKey, stream, primaryDispatcher)
	}

	var maximumHedgingDelay time.Duration
	for _, secondary := range validSecondaryDispatchers {
		maximumHedgingDelay = max(maximumHedgingDelay, secondary.MaximumPrimaryHedgingDelay)
	}

	contexts := make(map[string]ctxAndCancel, len(validSecondaryDispatchers)+1)

	primaryCtx, primaryCancelFn := context.WithCancel(ctxWithTimeout)
	primaryDeadline, _ := primaryCtx.Deadline()
	primaryCtx = log.Ctx(primaryCtx).With().Time("deadline", primaryDeadline).Logger().WithContext(primaryCtx)
	contexts[primaryDispatcher] = ctxAndCancel{primaryCtx, primaryCancelFn}
	for _, secondary := range validSecondaryDispatchers {
		secondaryCtx, secondaryCancelFn := context.WithCancel(ctxWithTimeout)
		secondaryDeadline, _ := secondaryCtx.Deadline()
		secondaryCtx = log.Ctx(secondaryCtx).With().Time("deadline", secondaryDeadline).Logger().WithContext(secondaryCtx)
		contexts[secondary.Name] = ctxAndCancel{secondaryCtx, secondaryCancelFn}
	}

	// For each secondary dispatch (as well as the primary), dispatch. Whichever one returns first,
	// stream its results and cancel the remaining dispatches.
	errorsByDispatcherName := xsync.NewMap[string, error]()

	var wg sync.WaitGroup
	if allowPrimary {
		wg.Add(1)
	}

	wg.Add(len(validSecondaryDispatchers))

	returnedResultsDispatcherName := atomic.NewString(noDispatcherResults)

	primarySleeper := cr.newPrimarySleeper(reqKey,
		tuple.FromCoreRelationReference(req.GetResourceRelation()),
		tuple.FromCoreRelationReference(req.GetSubjectRelation()),
		maximumHedgingDelay,
	)

	runHandler := func(handlerContext context.Context, name string, clusterClient ClusterClient) {
		defer wg.Done()
		if name == "" {
			log.Ctx(handlerContext).Warn().Msg("attempting to run a dispatch handler with an empty name, skipping")
			return
		}

		log.Ctx(handlerContext).Debug().Str("dispatcher", name).Msg("preparing to run streaming dispatcher")

		var startTime time.Time
		isPrimary := name == primaryDispatcher
		if isPrimary {
			// Have the primary wait a bit to ensure the secondaries have a chance to return first.
			primarySleeper.sleep(handlerContext)
		} else {
			startTime = time.Now()
		}

		select {
		case <-handlerContext.Done():
			log.Ctx(handlerContext).Trace().Str("dispatcher", name).Msg("dispatcher context canceled")
			if isPrimary {
				primaryDispatch.WithLabelValues("true", reqKey).Inc()
			}
			return

		default:
			// Do the rest of the work in a function
		}

		log.Ctx(handlerContext).Trace().Str("dispatcher", name).Msg("creating streaming dispatcher stream client")
		client, err := handler(handlerContext, clusterClient)
		log.Ctx(handlerContext).Trace().Str("dispatcher", name).Msg("created streaming dispatcher stream client")
		if isPrimary {
			primaryDispatch.WithLabelValues("false", reqKey).Inc()
		}
		if err != nil {
			if !isPrimary {
				cr.supportedResourceSubjectTracker.updateForError(err)
			}

			log.Ctx(handlerContext).Warn().Err(err).Str("dispatcher", name).Msg("error when trying to run secondary dispatcher")
			errorsByDispatcherName.Store(name, err)
			return
		}

		var hasPublishedFirstResult bool
		for {
			select {
			case <-handlerContext.Done():
				log.Ctx(handlerContext).Trace().Str("dispatcher", name).Msg("dispatcher context canceled, in results loop")
				return

			default:
				result, err := client.Recv()
				if err != nil {
					log.Ctx(handlerContext).Trace().Str("dispatcher", name).Err(err).Msg("dispatcher recv error")
				}

				// isResult is true if the result is not an error or we received an EOF, which is considered
				// a "result" (just the end of the stream).
				isResult := err == nil || errors.Is(err, io.EOF)
				if isResult && !hasPublishedFirstResult {
					// If a valid result was returned by a secondary dispatcher, and it was the first
					// result returned, record the time it took to get that result.
					if !isPrimary {
						finishTime := time.Now()
						duration := finishTime.Sub(startTime)
						cr.secondaryInitialResponseDigests[reqKey].addResultTime(handlerContext, duration)
						go cr.supportedResourceSubjectTracker.updateForSuccess(
							tuple.FromCoreRelationReference(req.GetResourceRelation()),
							tuple.FromCoreRelationReference(req.GetSubjectRelation()),
						)
					}

					// If a valid result, and we have not yet returned any results, try take a "lock" on
					// returning results to ensure only a single dispatcher returns results.
					swapped := returnedResultsDispatcherName.CompareAndSwap(noDispatcherResults, name)
					if !swapped {
						// Another dispatcher has started returning results, so terminate.
						log.Ctx(handlerContext).Trace().Str("dispatcher", name).Msg("another dispatcher has already returned results")
						return
					}

					log.Ctx(handlerContext).Trace().Str("dispatcher", name).Msg("this dispatcher is the first to return results, will publish them and cancel the others")
					dispatchCounter.WithLabelValues(reqKey, name).Add(1)

					// Cancel all other contexts to prevent them from running, or stop them
					// from running if started.
					log.Ctx(handlerContext).Trace().Str("dispatcher", name).Msg("canceling other dispatchers")
					for key, otherCtx := range contexts {
						if key != name {
							log.Ctx(handlerContext).Trace().Str("canceling-dispatcher", key).Msg("canceling dispatcher context")
							otherCtx.cancel()
						}
					}
				}

				// If the end of the results, nothing more to do.
				if errors.Is(err, io.EOF) {
					return
				}

				if err != nil {
					if !isPrimary {
						cr.supportedResourceSubjectTracker.updateForError(err)

						// If this is a secondary dispatcher and we received an error, cancel the primary's sleep
						// so it can begin execution immediately.
						primarySleeper.cancelSleep()
					}

					errorsByDispatcherName.Store(name, err)
					return
				}

				hasPublishedFirstResult = true
				serr := stream.Publish(result)
				if serr != nil {
					errorsByDispatcherName.Store(name, serr)
					return
				}
			}
		}
	}

	// Run the primary.
	if allowPrimary {
		go runHandler(contexts[primaryDispatcher].ctx, primaryDispatcher, cr.clusterClient)
	}

	// Run each of the secondary dispatches.
	for _, secondary := range validSecondaryDispatchers {
		go runHandler(contexts[secondary.Name].ctx, secondary.Name, secondary.Client)
	}

	// Wait for all the handlers to finish.
	wg.Wait()

	// Check for the first dispatcher that returned results and return its error, if any.
	resultHandlerName := returnedResultsDispatcherName.Load()
	if resultHandlerName == "" {
		log.Ctx(ctxWithTimeout).Error().Msg("got empty result handler name; this should never happen")
		return spiceerrors.MustBugf("got empty result handler name")
	}

	if resultHandlerName != noDispatcherResults {
		if err, ok := errorsByDispatcherName.Load(resultHandlerName); ok {
			log.Ctx(ctxWithTimeout).Warn().Err(err).Str("dispatcher", resultHandlerName).Msg("dispatcher that returned results encountered an error during streaming")
			return err
		}
		return nil
	}

	// If there is a primary dispatcher error, return it.
	if primaryErr, ok := errorsByDispatcherName.Load(primaryDispatcher); ok {
		allErrors := make([]error, 0, errorsByDispatcherName.Size())
		errorsByDispatcherName.Range(func(key string, value error) bool {
			allErrors = append(allErrors, value)
			return true
		})
		log.Ctx(ctxWithTimeout).Warn().Err(primaryErr).Errs("all-errors", allErrors).Msg("returning primary dispatcher error as no dispatchers returned results")
		return primaryErr
	}

	// Otherwise return a combined error.
	return errors.New("no dispatcher returned results; please check the logs for more information")
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
		IsReady: state == connectivity.Ready || state == connectivity.Idle,
		Message: fmt.Sprintf("found expected state when trying to connect to cluster: %v", state),
	}
}

// newPrimarySleeper creates a new primarySleeper instance that will wait for the
// specified wait time before allowing the primary dispatch to run. The wait time is
// determined based on the request key and the resource and subject types and relations.
// If the resource or subject is unsupported, the wait time will be zero.
//
// The primarySleeper can be used to cancel the sleep if the delay is no longer needed,
// such as when a secondary dispatcher returns results first.
func (cr *clusterDispatcher) newPrimarySleeper(
	reqKey string,
	resourceTypeAndRelation tuple.RelationReference,
	subjectTypeAndRelation tuple.RelationReference,
	maximumHedgingDelay time.Duration,
) *primarySleeper {
	waitTime := cr.getPrimaryWaitTime(reqKey, resourceTypeAndRelation, subjectTypeAndRelation, maximumHedgingDelay)
	return &primarySleeper{
		reqKey:     reqKey,
		waitTime:   waitTime,
		cancelFunc: func() {}, // No-op by default, will be set when sleep is called.
		lock:       sync.Mutex{},
	}
}

// getPrimaryWaitTime returns the wait time for the primary dispatch, based on the request key and the
// resource and subject types and relations. If the resource or subject is unsupported, the wait time
// will be zero.
func (cr *clusterDispatcher) getPrimaryWaitTime(
	reqKey string,
	resourceTypeAndRelation tuple.RelationReference,
	subjectTypeAndRelation tuple.RelationReference,
	maximumHedgingDelay time.Duration,
) time.Duration {
	// If the resource or subject is unsupported, return zero time.
	if cr.supportedResourceSubjectTracker.isUnsupported(
		resourceTypeAndRelation,
		subjectTypeAndRelation,
	) {
		return 0
	}

	return cr.secondaryInitialResponseDigests[reqKey].getWaitTime(maximumHedgingDelay)
}

// supportedResourceSubjectTracker is a struct that tracks the resources and subjects that are
// unsupported by the secondary dispatcher(s). If a resource or subject is unsupported, the primary
// dispatcher will not wait for the secondary dispatchers to return results, as it is extremely
// likely that hedging will not be beneficial.
type supportedResourceSubjectTracker struct {
	unsupportedResources sync.Map
	unsupportedSubjects  sync.Map
}

func newSupportedResourceSubjectTracker() *supportedResourceSubjectTracker {
	return &supportedResourceSubjectTracker{
		unsupportedResources: sync.Map{},
		unsupportedSubjects:  sync.Map{},
	}
}

// isUnsupported returns whether the resource or subject is unsupported by the secondary dispatcher(s).
func (srst *supportedResourceSubjectTracker) isUnsupported(
	resourceTypeAndRelation tuple.RelationReference,
	subjectTypeAndRelation tuple.RelationReference,
) bool {
	isUnsupportedResource, found := srst.unsupportedResources.Load(resourceTypeAndRelation)
	if found && isUnsupportedResource.(bool) {
		return true
	}

	isUnsupportedSubject, found := srst.unsupportedSubjects.Load(subjectTypeAndRelation)
	return found && isUnsupportedSubject.(bool)
}

// updateForSuccess updates the tracker for a successful dispatch, removing the resource and subject
// from the unsupported set.
func (srst *supportedResourceSubjectTracker) updateForSuccess(
	resourceTypeAndRelation tuple.RelationReference,
	subjectTypeAndRelation tuple.RelationReference,
) {
	srst.unsupportedResources.CompareAndSwap(resourceTypeAndRelation, true, false)
	srst.unsupportedSubjects.CompareAndSwap(subjectTypeAndRelation, true, false)
}

// updateForError updates the tracker for an error, adding the resource or subject to the unsupported set
// if the error indicates that the resource or subject is unsupported.
func (srst *supportedResourceSubjectTracker) updateForError(err error) {
	// Check for a FAILED_PRECONDITION with error details that indicate an unsupported
	// resource or subject.
	st, ok := status.FromError(err)
	if !ok {
		return
	}

	if st.Code() != codes.FailedPrecondition {
		return
	}

	for _, detail := range st.Details() {
		errDetail, ok := detail.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}

		// If the error is an unsupported resource or subject, add it to the tracker.
		if errDetail.Reason == "UNSUPPORTED_RESOURCE_RELATION" {
			definitionName := errDetail.Metadata["definition_name"]
			relationName := errDetail.Metadata["relation_name"]
			rr := tuple.RR(definitionName, relationName)
			existing, loaded := srst.unsupportedResources.LoadOrStore(rr, true)
			if !loaded || !existing.(bool) {
				srst.unsupportedResources.Store(rr, true)
			}
		}

		if errDetail.Reason == "UNSUPPORTED_SUBJECT_RELATION" {
			definitionName := errDetail.Metadata["definition_name"]
			relationName := errDetail.Metadata["relation_name"]
			rr := tuple.RR(definitionName, relationName)
			existing, loaded := srst.unsupportedSubjects.LoadOrStore(rr, true)
			if !loaded || !existing.(bool) {
				srst.unsupportedSubjects.Store(rr, true)
			}
		}
	}
}

type primarySleeper struct {
	reqKey     string
	waitTime   time.Duration
	cancelFunc context.CancelFunc // GUARDED_BY(lock)
	lock       sync.Mutex
}

// sleep sets the value of cancelFunc, and sleeps for the configured wait time or exits early if the context is cancelled.
func (s *primarySleeper) sleep(parentCtx context.Context) {
	if s.waitTime <= 0 {
		return
	}

	sleepCtx, cf := context.WithCancel(context.Background())

	s.lock.Lock()
	s.cancelFunc = cf
	s.lock.Unlock()

	hedgeWaitHistogram.WithLabelValues(s.reqKey).Observe(s.waitTime.Seconds())
	log.Ctx(parentCtx).Trace().Str("request-key", s.reqKey).Stringer("wait-time", s.waitTime).Msg("primary dispatch waiting before running")

	startTime := time.Now()
	timer := time.NewTimer(s.waitTime)
	defer timer.Stop()

	select {
	case <-parentCtx.Done():
		log.Ctx(parentCtx).Trace().Str("request-key", s.reqKey).Msg("primary dispatch canceled before waiting completed")
		return

	case <-sleepCtx.Done():
		log.Ctx(parentCtx).Trace().Str("request-key", s.reqKey).Msg("primary dispatch early terminated waiting")
		hedgeActualWaitHistogram.WithLabelValues(s.reqKey).Observe(time.Since(startTime).Seconds())
		return

	case <-timer.C:
		log.Ctx(parentCtx).Trace().Str("request-key", s.reqKey).Msg("primary dispatch finished waiting")
		hedgeActualWaitHistogram.WithLabelValues(s.reqKey).Observe(s.waitTime.Seconds())
		return
	}
}

func (s *primarySleeper) cancelSleep() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.cancelFunc()
}

// Always verify that we implement the interface
var _ dispatch.Dispatcher = &clusterDispatcher{}

var emptyMetadata = &v1.ResponseMeta{
	DispatchCount: 0,
}

var requestFailureMetadata = &v1.ResponseMeta{
	DispatchCount: 1,
}
