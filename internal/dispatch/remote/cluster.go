package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/authzed/consistent"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	log "github.com/authzed/spicedb/internal/logging"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var dispatchCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "remote_dispatch_handler_total",
	Help:      "which dispatcher handled a request",
}, []string{"request_kind", "handler_name"})

func init() {
	prometheus.MustRegister(dispatchCounter)
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
func NewClusterDispatcher(client ClusterClient, conn *grpc.ClientConn, config ClusterDispatcherConfig, secondaryDispatch map[string]SecondaryDispatch, secondaryDispatchExprs map[string]*DispatchExpr) dispatch.Dispatcher {
	keyHandler := config.KeyHandler
	if keyHandler == nil {
		keyHandler = &keys.DirectKeyHandler{}
	}

	dispatchOverallTimeout := config.DispatchOverallTimeout
	if dispatchOverallTimeout <= 0 {
		dispatchOverallTimeout = 60 * time.Second
	}

	return &clusterDispatcher{
		clusterClient:          client,
		conn:                   conn,
		keyHandler:             keyHandler,
		dispatchOverallTimeout: dispatchOverallTimeout,
		secondaryDispatch:      secondaryDispatch,
		secondaryDispatchExprs: secondaryDispatchExprs,
	}
}

type clusterDispatcher struct {
	clusterClient          ClusterClient
	conn                   *grpc.ClientConn
	keyHandler             keys.Handler
	dispatchOverallTimeout time.Duration
	secondaryDispatch      map[string]SecondaryDispatch
	secondaryDispatchExprs map[string]*DispatchExpr
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

	resp, err := dispatchRequest(ctx, cr, "check", req, func(ctx context.Context, client ClusterClient) (*v1.DispatchCheckResponse, error) {
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

type respTuple[S responseMessage] struct {
	resp S
	err  error
}

type secondaryRespTuple[S responseMessage] struct {
	handlerName string
	resp        S
}

func dispatchRequest[Q requestMessage, S responseMessage](ctx context.Context, cr *clusterDispatcher, reqKey string, req Q, handler func(context.Context, ClusterClient) (S, error)) (S, error) {
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
		resp, err := handler(withTimeout, cr.clusterClient)
		primaryResultChan <- respTuple[S]{resp, err}
	}()

	result, err := RunDispatchExpr(expr, req)
	if err != nil {
		log.Warn().Err(err).Msg("error when trying to evaluate the dispatch expression")
	}

	log.Trace().Str("secondary-dispatchers", strings.Join(result, ",")).Object("request", req).Msg("running secondary dispatchers")

	for _, secondaryDispatchName := range result {
		secondary, ok := cr.secondaryDispatch[secondaryDispatchName]
		if !ok {
			log.Warn().Str("secondary-dispatcher-name", secondaryDispatchName).Msg("received unknown secondary dispatcher")
			continue
		}

		log.Trace().Str("secondary-dispatcher", secondary.Name).Object("request", req).Msg("running secondary dispatcher")
		go func() {
			resp, err := handler(withTimeout, secondary.Client)
			if err != nil {
				// For secondary dispatches, ignore any errors, as only the primary will be handled in
				// that scenario.
				log.Trace().Str("secondary", secondary.Name).Err(err).Msg("got ignored secondary dispatch error")
				return
			}

			secondaryResultChan <- secondaryRespTuple[S]{resp: resp, handlerName: secondary.Name}
		}()
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
	primaryDispatcher     = ""
)

func publishClient[Q requestMessageWithCursor, R responseMessageWithCursor](ctx context.Context, client receiver[R], stream dispatch.Stream[R], secondaryDispatchName string) error {
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
				afterResponseCursor := result.GetAfterResponseCursor()
				if afterResponseCursor == nil {
					return spiceerrors.MustBugf("received a nil after response cursor for secondary dispatch")
				}
				afterResponseCursor.Sections = append([]string{secondaryCursorPrefix + secondaryDispatchName}, afterResponseCursor.Sections...)
			}

			serr := stream.Publish(result)
			if serr != nil {
				return serr
			}
		}
	}
}

// dispatchStreamingRequest handles the dispatching of a streaming request to the primary and any
// secondary dispatchers. Unlike the non-streaming version, this will first attempt to dispatch
// from the allowed secondary dispatchers before falling back to the primary, rather than running
// them in parallel.
func dispatchStreamingRequest[Q requestMessageWithCursor, R responseMessageWithCursor](
	ctx context.Context,
	cr *clusterDispatcher,
	reqKey string,
	req Q,
	stream dispatch.Stream[R],
	handler func(context.Context, ClusterClient) (receiver[R], error),
) error {
	withTimeout, cancelFn := context.WithTimeout(ctx, cr.dispatchOverallTimeout)
	defer cancelFn()

	client, err := handler(withTimeout, cr.clusterClient)
	if err != nil {
		return err
	}

	// Check the cursor to see if the dispatch went to one of the secondary endpoints.
	cursor := req.GetOptionalCursor()
	cursorLockedSecondaryName := ""
	if cursor != nil && len(cursor.Sections) > 0 {
		if strings.HasPrefix(cursor.Sections[0], secondaryCursorPrefix) {
			cursorLockedSecondaryName = strings.TrimPrefix(cursor.Sections[0], secondaryCursorPrefix)
			cursor.Sections = cursor.Sections[1:]
		}
	}

	// If no secondary dispatches are defined, just invoke directly.
	if len(cr.secondaryDispatchExprs) == 0 || len(cr.secondaryDispatch) == 0 {
		return publishClient[Q](withTimeout, client, stream, primaryDispatcher)
	}

	// If the cursor is locked to a known secondary, dispatch to it.
	if cursorLockedSecondaryName != "" {
		secondary, ok := cr.secondaryDispatch[cursorLockedSecondaryName]
		if ok {
			secondaryClient, err := handler(withTimeout, secondary.Client)
			if err != nil {
				return err
			}

			log.Debug().Str("secondary-dispatcher", secondary.Name).Object("request", req).Msg("running secondary dispatcher based on cursor")
			return publishClient[Q](withTimeout, secondaryClient, stream, cursorLockedSecondaryName)
		}

		return fmt.Errorf("unknown secondary dispatcher in cursor: %s", cursorLockedSecondaryName)
	}

	// Otherwise, look for a matching expression for the initial secondary dispatch
	// and, if present, try to dispatch to it.
	expr, ok := cr.secondaryDispatchExprs[reqKey]
	if !ok {
		return publishClient[Q](withTimeout, client, stream, primaryDispatcher)
	}

	result, err := RunDispatchExpr(expr, req)
	if err != nil {
		log.Warn().Err(err).Msg("error when trying to evaluate the dispatch expression")
	}

	for _, secondaryDispatchName := range result {
		secondary, ok := cr.secondaryDispatch[secondaryDispatchName]
		if !ok {
			log.Warn().Str("secondary-dispatcher-name", secondaryDispatchName).Msg("received unknown secondary dispatcher")
			continue
		}

		log.Trace().Str("secondary-dispatcher", secondary.Name).Object("request", req).Msg("running secondary dispatcher")
		secondaryClient, err := handler(withTimeout, secondary.Client)
		if err != nil {
			log.Warn().Str("secondary-dispatcher", secondary.Name).Err(err).Msg("failed to create secondary dispatch client")
			continue
		}

		if err := publishClient[Q](withTimeout, secondaryClient, stream, secondaryDispatchName); err != nil {
			log.Warn().Str("secondary-dispatcher", secondary.Name).Err(err).Msg("failed to publish secondary dispatch response")
			continue
		}

		return nil
	}

	// Fallback: use the primary client if no secondary matched.
	return publishClient[Q](withTimeout, client, stream, primaryDispatcher)
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

	withTimeout, cancelFn := context.WithTimeout(ctx, cr.dispatchOverallTimeout)
	defer cancelFn()

	client, err := cr.clusterClient.DispatchLookupSubjects(withTimeout, req)
	if err != nil {
		return err
	}

	for {
		select {
		case <-withTimeout.Done():
			return withTimeout.Err()

		default:
			result, err := client.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return err
			}

			merr := adjustMetadataForDispatch(result.Metadata)
			if merr != nil {
				return merr
			}

			serr := stream.Publish(result)
			if serr != nil {
				return serr
			}
		}
	}
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

// Always verify that we implement the interface
var _ dispatch.Dispatcher = &clusterDispatcher{}

var emptyMetadata = &v1.ResponseMeta{
	DispatchCount: 0,
}

var requestFailureMetadata = &v1.ResponseMeta{
	DispatchCount: 1,
}
