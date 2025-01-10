package singleflight

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"resenje.org/singleflight"

	log "github.com/authzed/spicedb/internal/logging"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var (
	singleFlightCount       = promauto.NewCounterVec(singleFlightCountConfig, []string{"method", "shared"})
	singleFlightCountConfig = prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "dispatch",
		Name:      "single_flight_total",
		Help:      "total number of dispatch requests that were single flighted",
	}
)

func New(delegate dispatch.Dispatcher, handler keys.Handler) dispatch.Dispatcher {
	return &Dispatcher{
		delegate:   delegate,
		keyHandler: handler,
	}
}

type Dispatcher struct {
	delegate   dispatch.Dispatcher
	keyHandler keys.Handler

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

		singleFlightCount.WithLabelValues("DispatchCheck", "missing").Inc()
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
		singleFlightCount.WithLabelValues("DispatchCheck", "loop").Inc()
		return d.delegate.DispatchCheck(ctx, req)
	}

	sharedResp, isShared, err := d.checkGroup.Do(ctx, keyString, func(innerCtx context.Context) (*v1.DispatchCheckResponse, error) {
		return d.delegate.DispatchCheck(innerCtx, req)
	})

	if sharedResp == nil {
		sharedResp = &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}
	}

	singleFlightCount.WithLabelValues("DispatchCheck", strconv.FormatBool(isShared)).Inc()
	return sharedResp, err
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

		singleFlightCount.WithLabelValues("DispatchExpand", "missing").Inc()
		req.Metadata.TraversalBloom = tb
		return d.delegate.DispatchExpand(ctx, req)
	}

	possiblyLoop, err := req.Metadata.RecordTraversal(keyString)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	} else if possiblyLoop {
		log.Debug().Object("DispatchExpand", req).Str("key", keyString).Msg("potential DispatchExpand loop detected")
		singleFlightCount.WithLabelValues("DispatchExpand", "loop").Inc()
		return d.delegate.DispatchExpand(ctx, req)
	}

	v, isShared, err := d.expandGroup.Do(ctx, keyString, func(innerCtx context.Context) (*v1.DispatchExpandResponse, error) {
		return d.delegate.DispatchExpand(innerCtx, req)
	})

	singleFlightCount.WithLabelValues("DispatchExpand", strconv.FormatBool(isShared)).Inc()
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	}
	return v, err
}

func (d *Dispatcher) DispatchLookupResources2(req *v1.DispatchLookupResources2Request, stream dispatch.LookupResources2Stream) error {
	return d.delegate.DispatchLookupResources2(req, stream)
}

func (d *Dispatcher) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	return d.delegate.DispatchLookupSubjects(req, stream)
}

func (d *Dispatcher) Close() error                    { return d.delegate.Close() }
func (d *Dispatcher) ReadyState() dispatch.ReadyState { return d.delegate.ReadyState() }
