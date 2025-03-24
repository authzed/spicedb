package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/authzed/consistent"
	"github.com/caio/go-tdigest/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"go.uber.org/atomic"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

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
	Name:      "remote_dispatch_hedge_wait_duration",
	Help:      "distribution in seconds of calculated wait time for hedging requests to the primary dispatcher when a secondary is active.",
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
const defaultStartingPrimaryHedgingDelay = 5 * time.Millisecond

// defaultHedgerQuantile is the default quantile used to determine the hedging delay for primary calls.
const defaultHedgerQuantile = 0.95

// minimumDigestCount is the minimum number of samples required in the digest before it can be used
// to determine the configured percentile.
const minimumDigestCount = 10

const defaultTDigestCompression = float64(1000)

var supportsSecondaries = []string{"check", "lookupresources", "lookupsubjects"}

func init() {
	prometheus.MustRegister(dispatchCounter)
	prometheus.MustRegister(hedgeWaitHistogram)
	prometheus.MustRegister(primaryDispatch)
}

type ClusterClient interface {
	DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest, opts ...grpc.CallOption) (*v1.DispatchCheckResponse, error)
	DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest, opts ...grpc.CallOption) (*v1.DispatchExpandResponse, error)
	DispatchLookupResources2(ctx context.Context, in *v1.DispatchLookupResources2Request, opts ...grpc.CallOption) (v1.DispatchService_DispatchLookupResources2Client, error)
	DispatchLookupSubjects(ctx context.Context, in *v1.DispatchLookupSubjectsRequest, opts ...grpc.CallOption) (v1.DispatchService_DispatchLookupSubjectsClient, error)
}

type ClusterDispatcherConfig struct {
	// KeyHandler is then handler to use for generating dispatch hash ring keys.
	KeyHandler keys.Handler

	// DispatchOverallTimeout is the maximum duration of a dispatched request
	// before it should timeout.
	DispatchOverallTimeout time.Duration
}

// SecondaryDispatch defines a struct holding a client and its name for secondary
// dispatching.
type SecondaryDispatch struct {
	Name   string
	Client ClusterClient
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
	digest                      *tdigest.TDigest
	startingPrimaryHedgingDelay time.Duration
	lock                        sync.RWMutex
}

// getWaitTime returns the configured percentile of the digest, or a default value if the digest is empty.
// In
func (dal *digestAndLock) getWaitTime() time.Duration {
	dal.lock.RLock()
	milliseconds := dal.digest.Quantile(defaultHedgerQuantile)
	count := dal.digest.Count()
	dal.lock.RUnlock()

	if milliseconds <= 0 || count < minimumDigestCount {
		return dal.startingPrimaryHedgingDelay
	}

	return time.Duration(milliseconds) * time.Millisecond
}

func (dal *digestAndLock) addResultTime(duration time.Duration) {
	if dal.lock.TryLock() {
		err := dal.digest.Add(float64(duration.Milliseconds()))
		dal.lock.Unlock()
		if err != nil {
			log.Warn().Err(err).Msg("error when trying to add result time to digest")
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
	requestMessage

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

	// Otherwise invoke in parallel with any secondary matches.
	primaryResultChan := make(chan respTuple[S], 1)
	secondaryResultChan := make(chan secondaryRespTuple[S], len(cr.secondaryDispatch))

	// Run the main dispatch.
	go func() {
		// Have the main dispatch wait some time before returning, to allow the secondaries to
		// potentially return first.
		computedWait := cr.getPrimaryWaitTime(reqKey, resourceTypeAndRelation, subjectTypeAndRelation)
		log.Trace().Stringer("computed-wait", computedWait).Msg("primary dispatch started; sleeping for computed wait time")
		if computedWait > 0 {
			time.Sleep(computedWait)
		}

		log.Trace().Msg("running primary dispatch after wait")
		select {
		case <-withTimeout.Done():
			log.Trace().Str("request-key", reqKey).Msg("primary dispatch timed out or was canceled")
			primaryDispatch.WithLabelValues("true", reqKey).Inc()
			return

		default:
			resp, err := handler(withTimeout, cr.clusterClient)
			log.Trace().Str("request-key", reqKey).Msg("primary dispatch completed")
			primaryResultChan <- respTuple[S]{resp, err}
			hedgeWaitHistogram.WithLabelValues(reqKey).Observe(computedWait.Seconds())
			primaryDispatch.WithLabelValues("false", reqKey).Inc()
		}
	}()

	log.Trace().Object("request", req).Msg("running dispatch expression")
	result, err := RunDispatchExpr(expr, req)
	if err != nil {
		log.Warn().Err(err).Msg("error when trying to evaluate the dispatch expression")
	} else {
		for _, secondaryDispatchName := range result {
			secondary, ok := cr.secondaryDispatch[secondaryDispatchName]
			if !ok {
				log.Warn().Str("secondary-dispatcher-name", secondaryDispatchName).Msg("received unknown secondary dispatcher")
				continue
			}

			go func() {
				select {
				case <-withTimeout.Done():
					log.Trace().Str("secondary", secondary.Name).Msg("secondary dispatch timed out or was canceled")
					return

				default:
					log.Trace().Str("secondary", secondary.Name).Msg("running secondary dispatch")
					startTime := time.Now()
					resp, err := handler(withTimeout, secondary.Client)
					endTime := time.Now()
					handlerDuration := endTime.Sub(startTime)
					if err != nil {
						// For secondary dispatches, ignore any errors, as only the primary will be handled in
						// that scenario.
						log.Trace().Stringer("duration", handlerDuration).Str("secondary", secondary.Name).Err(err).Msg("got ignored secondary dispatch error")
						cr.supportedResourceSubjectTracker.updateForError(err)
						return
					}

					log.Trace().Stringer("duration", handlerDuration).Str("secondary", secondary.Name).Msg("secondary dispatch completed")
					go cr.supportedResourceSubjectTracker.updateForSuccess(resourceTypeAndRelation, subjectTypeAndRelation)
					cr.secondaryInitialResponseDigests[reqKey].addResultTime(handlerDuration)
					secondaryResultChan <- secondaryRespTuple[S]{resp: resp, handlerName: secondary.Name}
				}
			}()
		}
	}

	var foundError error
	select {
	case <-withTimeout.Done():
		return *new(S), fmt.Errorf("check dispatch has timed out")

	case r := <-primaryResultChan:
		if r.err == nil {
			dispatchCounter.WithLabelValues(reqKey, "(primary)").Add(1)
			return r.resp, nil
		}

		// Otherwise, if an error was found, log it and we'll return after *all* the secondaries have run.
		// This allows an otherwise error-state to be handled by one of the secondaries.
		foundError = r.err

	case r := <-secondaryResultChan:
		dispatchCounter.WithLabelValues(reqKey, r.handlerName).Add(1)
		return r.resp, nil
	}

	dispatchCounter.WithLabelValues(reqKey, "(primary)").Add(1)
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

type receiver[S responseMessage] interface {
	Recv() (S, error)
	grpc.ClientStream
}

const (
	secondaryCursorPrefix = "$$secondary:"
	primaryDispatcher     = "$primary"
)

func publishClient[R responseMessage](ctx context.Context, client receiver[R], stream dispatch.Stream[R], secondaryDispatchName string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			result, err := client.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return err
			}

			merr := adjustMetadataForDispatch(result.GetMetadata())
			if merr != nil {
				return merr
			}

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
}

type ctxAndCancel struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// dispatchStreamingRequest handles the dispatching of a streaming request to the primary and any
// secondary dispatchers. Unlike the non-streaming version, this will first attempt to dispatch
// from the allowed secondary dispatchers before falling back to the primary, rather than running
// them in parallel.
func dispatchStreamingRequest[Q streamingRequestMessage, R responseMessage](
	ctx context.Context,
	cr *clusterDispatcher,
	reqKey string,
	req Q,
	stream dispatch.Stream[R],
	handler func(context.Context, ClusterClient) (receiver[R], error),
) error {
	withTimeout, cancelFn := context.WithTimeout(ctx, cr.dispatchOverallTimeout)
	defer cancelFn()

	// If no secondary dispatches are defined, just invoke directly.
	if len(cr.secondaryDispatchExprs) == 0 || len(cr.secondaryDispatch) == 0 {
		client, err := handler(withTimeout, cr.clusterClient)
		if err != nil {
			return err
		}
		return publishClient(withTimeout, client, stream, primaryDispatcher)
	}

	// Check the cursor to see if the dispatch went to one of the secondary endpoints.
	cursorLockedSecondaryName := ""
	if cursorSupports, ok := any(req).(requestMessageWithCursor); ok {
		cursor := cursorSupports.GetOptionalCursor()
		if cursor != nil && len(cursor.Sections) > 0 {
			if strings.HasPrefix(cursor.Sections[0], secondaryCursorPrefix) {
				cursorLockedSecondaryName = strings.TrimPrefix(cursor.Sections[0], secondaryCursorPrefix)
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
			log.Warn().Err(err).Msg("error when trying to evaluate the dispatch expression")
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
			return fmt.Errorf("cursor locked to unknown secondary dispatcher")
		}

		client, err := handler(withTimeout, cr.clusterClient)
		if err != nil {
			return err
		}
		return publishClient(withTimeout, client, stream, primaryDispatcher)
	}

	contexts := make(map[string]ctxAndCancel, len(validSecondaryDispatchers)+1)

	primaryCtx, primaryCancelFn := context.WithCancel(withTimeout)
	contexts[primaryDispatcher] = ctxAndCancel{primaryCtx, primaryCancelFn}
	for _, secondary := range validSecondaryDispatchers {
		secondaryCtx, secondaryCancelFn := context.WithCancel(withTimeout)
		contexts[secondary.Name] = ctxAndCancel{secondaryCtx, secondaryCancelFn}
	}

	// For each secondary dispatch (as well as the primary), dispatch. Whichever one returns first,
	// stream its results and cancel the remaining dispatches.
	var errorsLock sync.Mutex
	errorsByDispatcherName := make(map[string]error)

	var wg sync.WaitGroup
	if allowPrimary {
		wg.Add(1)
	}

	wg.Add(len(validSecondaryDispatchers))

	returnedResultsDispatcherName := atomic.NewString("")
	runHandler := func(name string, clusterClient ClusterClient) {
		ctx := contexts[name].ctx
		log.Debug().Str("dispatcher", name).Msg("running secondary dispatcher")
		defer wg.Done()

		var startTime time.Time
		isPrimary := name == primaryDispatcher
		if isPrimary {
			// Have the primary wait a bit to ensure the secondaries have a chance to return first.
			computedWait := cr.getPrimaryWaitTime(reqKey,
				tuple.FromCoreRelationReference(req.GetResourceRelation()),
				tuple.FromCoreRelationReference(req.GetSubjectRelation()),
			)
			if computedWait > 0 {
				time.Sleep(computedWait)
				hedgeWaitHistogram.WithLabelValues(reqKey).Observe(computedWait.Seconds())
			}
		} else {
			startTime = time.Now()
		}

		select {
		case <-ctx.Done():
			log.Trace().Str("dispatcher", name).Msg("dispatcher context canceled")
			if isPrimary {
				primaryDispatch.WithLabelValues("true", reqKey).Inc()
			}
			return

		default:
			// Do the rest of the work in a function
		}

		log.Trace().Str("dispatcher", name).Msg("running streaming dispatcher")
		client, err := handler(ctx, clusterClient)
		log.Trace().Str("dispatcher", name).Msg("streaming dispatcher completed initial request")
		if isPrimary {
			primaryDispatch.WithLabelValues("false", reqKey).Inc()
		}
		if err != nil {
			if !isPrimary {
				cr.supportedResourceSubjectTracker.updateForError(err)
			}

			log.Warn().Err(err).Str("dispatcher", name).Msg("error when trying to run secondary dispatcher")
			errorsLock.Lock()
			errorsByDispatcherName[name] = err
			errorsLock.Unlock()
			return
		}

		var hasPublishedFirstResult bool
		for {
			select {
			case <-ctx.Done():
				log.Trace().Str("dispatcher", name).Msg("dispatcher context canceled, in results loop")
				return

			default:
				result, err := client.Recv()
				log.Trace().Str("dispatcher", name).Err(err).Any("result", result).Msg("dispatcher recv")

				// isResult is true if the result is not an error or we received an EOF, which is considered
				// a "result" (just the end of the stream).
				isResult := err == nil || errors.Is(err, io.EOF)
				if isResult && !hasPublishedFirstResult {
					// If a valid result was returned by a secondary dispatcher, and it was the first
					// result returned, record the time it took to get that result.
					if !isPrimary {
						finishTime := time.Now()
						duration := finishTime.Sub(startTime)
						cr.secondaryInitialResponseDigests[reqKey].addResultTime(duration)
						go cr.supportedResourceSubjectTracker.updateForSuccess(
							tuple.FromCoreRelationReference(req.GetResourceRelation()),
							tuple.FromCoreRelationReference(req.GetSubjectRelation()),
						)
					}

					// If a valid result, and we have not yet returned any results, try take a "lock" on
					// returning results to ensure only a single dispatcher returns results.
					swapped := returnedResultsDispatcherName.CompareAndSwap("", name)
					if !swapped {
						// Another dispatcher has started returning results, so terminate.
						log.Trace().Str("dispatcher", name).Msg("another dispatcher has already returned results")
						return
					}

					// Cancel all other contexts to prevent them from running, or stop them
					// from running if started.
					log.Trace().Str("dispatcher", name).Msg("canceling other dispatchers")
					for key, ctxAndCancel := range contexts {
						if key != name {
							ctxAndCancel.cancel()
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
					}

					errorsLock.Lock()
					errorsByDispatcherName[name] = err
					errorsLock.Unlock()
					return
				}

				hasPublishedFirstResult = true
				merr := adjustMetadataForDispatch(result.GetMetadata())
				if merr != nil {
					errorsLock.Lock()
					errorsByDispatcherName[name] = merr
					errorsLock.Unlock()
					return
				}

				serr := stream.Publish(result)
				if serr != nil {
					errorsLock.Lock()
					errorsByDispatcherName[name] = serr
					errorsLock.Unlock()
					return
				}
			}
		}
	}

	// Run the primary.
	if allowPrimary {
		go runHandler(primaryDispatcher, cr.clusterClient)
	}

	// Run each of the secondary dispatches.
	for _, secondary := range validSecondaryDispatchers {
		go runHandler(secondary.Name, secondary.Client)
	}

	// Wait for all the handlers to finish.
	wg.Wait()

	// Check for the first dispatcher that returned results and return its error, if any.
	resultHandlerName := returnedResultsDispatcherName.Load()
	if resultHandlerName != "" {
		if err, ok := errorsByDispatcherName[resultHandlerName]; ok {
			return err
		}
		return nil
	}

	// Otherwise return a combined error.
	cerr := fmt.Errorf("no dispatcher returned results")
	for name, err := range errorsByDispatcherName {
		cerr = fmt.Errorf("%w; error in dispatcher %s: %w", cerr, name, err)
	}
	return cerr
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

// getPrimaryWaitTime returns the wait time for the primary dispatch, based on the request key and the
// resource and subject types and relations. If the resource or subject is unsupported, the wait time
// will be zero.
func (cr *clusterDispatcher) getPrimaryWaitTime(
	reqKey string,
	resourceTypeAndRelation tuple.RelationReference,
	subjectTypeAndRelation tuple.RelationReference,
) time.Duration {
	// If the resource or subject is unsupported, return zero time.
	if cr.supportedResourceSubjectTracker.isUnsupported(
		resourceTypeAndRelation,
		subjectTypeAndRelation,
	) {
		return 0
	}

	return cr.secondaryInitialResponseDigests[reqKey].getWaitTime()
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

// Always verify that we implement the interface
var _ dispatch.Dispatcher = &clusterDispatcher{}

var emptyMetadata = &v1.ResponseMeta{
	DispatchCount: 0,
}

var requestFailureMetadata = &v1.ResponseMeta{
	DispatchCount: 1,
}
