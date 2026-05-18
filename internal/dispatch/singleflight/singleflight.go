package singleflight

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"resenje.org/singleflight"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/metrics"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var singleFlightCountOpts = metrics.Opts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "single_flight_total",
	Help:      "total number of dispatch requests that were single flighted",
}

func New(delegate dispatch.Dispatcher, handler keys.Handler) dispatch.Dispatcher {
	return NewWithMetricsFactory(delegate, handler, metrics.NewPrometheusFactory(nil))
}

func NewWithMetricsFactory(delegate dispatch.Dispatcher, handler keys.Handler, factory metrics.Factory) dispatch.Dispatcher {
	if factory == nil {
		factory = metrics.NoopFactory{}
	}

	return &Dispatcher{
		delegate:          delegate,
		keyHandler:        handler,
		singleFlightCount: factory.CounterVec(singleFlightCountOpts, []string{"method", "shared"}),
	}
}

type Dispatcher struct {
	delegate          dispatch.Dispatcher
	keyHandler        keys.Handler
	singleFlightCount metrics.CounterVec

	checkGroup  singleflight.Group[string, *v1.DispatchCheckResponse]
	expandGroup singleflight.Group[string, *v1.DispatchExpandResponse]
}

func (d *Dispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	key, err := d.keyHandler.CheckDispatchKey(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}},
			status.Error(codes.Internal, "unexpected DispatchCheck error")
	}

	keyString := hex.EncodeToString(key)

	// this is in place so that upgrading to a SpiceDB version with traversal bloom does not cause dispatch failures
	// if this is observed frequently it suggests a callsite is missing setting the bloom filter.
	// Since there is no bloom filter, there is no guarantee recursion won't happen, so it's safer not to singleflight
	if len(req.Metadata.TraversalBloom) == 0 {
		tb, err := v1.NewTraversalBloomFilter(50)
		if err != nil {
			return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, status.Error(codes.Internal, fmt.Errorf("unable to create traversal bloom filter: %w", err).Error())
		}

		d.singleFlightCount.WithLabelValues("DispatchCheck", "missing").Inc()
		req.Metadata.TraversalBloom = tb
		return d.delegate.DispatchCheck(ctx, req)
	}

	// Check if the key has already been part of a dispatch. If so, this represents a
	// likely recursive call, so we dispatch it to the delegate to avoid the singleflight from blocking it.
	// If the bloom filter presents a false positive, a dispatch will happen, which is a small inefficiency
	// traded-off to prevent a recursive-call deadlock
	possiblyLoop, err := req.Metadata.RecordTraversal(keyString)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	} else if possiblyLoop {
		log.Debug().Object("DispatchCheckRequest", req).Str("key", keyString).Msg("potential DispatchCheckRequest loop detected")
		d.singleFlightCount.WithLabelValues("DispatchCheck", "loop").Inc()
		return d.delegate.DispatchCheck(ctx, req)
	}

	sharedResp, isShared, err := d.checkGroup.Do(ctx, keyString, func(innerCtx context.Context) (*v1.DispatchCheckResponse, error) {
		return d.delegate.DispatchCheck(innerCtx, req)
	})

	if sharedResp == nil {
		sharedResp = &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}
	}

	d.singleFlightCount.WithLabelValues("DispatchCheck", strconv.FormatBool(isShared)).Inc()
	return sharedResp.CloneVT(), err
}

func (d *Dispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	key, err := d.keyHandler.ExpandDispatchKey(ctx, req)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}},
			status.Error(codes.Internal, "unexpected DispatchExpand error")
	}

	keyString := hex.EncodeToString(key)

	// this is in place so that upgrading to a SpiceDB version with traversal bloom does not cause dispatch failures
	// if this is observed frequently it suggests a callsite is missing setting the bloom filter
	// Since there is no bloom filter, there is no guarantee recursion won't happen, so it's safer not to singleflight
	if len(req.Metadata.TraversalBloom) == 0 {
		tb, err := v1.NewTraversalBloomFilter(50)
		if err != nil {
			return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, status.Error(codes.Internal, fmt.Errorf("unable to create traversal bloom filter: %w", err).Error())
		}

		d.singleFlightCount.WithLabelValues("DispatchExpand", "missing").Inc()
		req.Metadata.TraversalBloom = tb
		return d.delegate.DispatchExpand(ctx, req)
	}

	possiblyLoop, err := req.Metadata.RecordTraversal(keyString)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	} else if possiblyLoop {
		log.Debug().Object("DispatchExpand", req).Str("key", keyString).Msg("potential DispatchExpand loop detected")
		d.singleFlightCount.WithLabelValues("DispatchExpand", "loop").Inc()
		return d.delegate.DispatchExpand(ctx, req)
	}

	v, isShared, err := d.expandGroup.Do(ctx, keyString, func(innerCtx context.Context) (*v1.DispatchExpandResponse, error) {
		return d.delegate.DispatchExpand(innerCtx, req)
	})

	d.singleFlightCount.WithLabelValues("DispatchExpand", strconv.FormatBool(isShared)).Inc()
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	}
	return v, err
}

func (d *Dispatcher) DispatchLookupResources2(req *v1.DispatchLookupResources2Request, stream dispatch.LookupResources2Stream) error {
	return d.delegate.DispatchLookupResources2(req, stream)
}

func (d *Dispatcher) DispatchLookupResources3(req *v1.DispatchLookupResources3Request, stream dispatch.LookupResources3Stream) error {
	return d.delegate.DispatchLookupResources3(req, stream)
}

func (d *Dispatcher) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	return d.delegate.DispatchLookupSubjects(req, stream)
}

func (d *Dispatcher) DispatchQueryPlan(req *v1.DispatchQueryPlanRequest, stream dispatch.PlanStream) error {
	// Plan dispatches are not currently grouped via singleflight; record the
	// passthrough so the metric series exists and can be compared against the
	// other dispatch methods.
	d.singleFlightCount.WithLabelValues("DispatchQueryPlan", "passthrough").Inc()
	return d.delegate.DispatchQueryPlan(req, stream)
}

func (d *Dispatcher) Close() error                    { return d.delegate.Close() }
func (d *Dispatcher) ReadyState() dispatch.ReadyState { return d.delegate.ReadyState() }
