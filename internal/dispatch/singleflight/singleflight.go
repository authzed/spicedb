package singleflight

import (
	"context"
	"encoding/hex"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"resenje.org/singleflight"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var singleFlightCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "single_flight_total",
	Help:      "total number of dispatch requests that were single flighted",
}, []string{"method", "shared"})

func New(delegate dispatch.Dispatcher, handler keys.Handler) dispatch.Dispatcher {
	return &Dispatcher{
		delegate:            delegate,
		keyHandler:          handler,
		checkByDispatchKey:  mapz.NewCountingMultiMap[string, string](),
		expandByDispatchKey: mapz.NewCountingMultiMap[string, string](),
	}
}

type Dispatcher struct {
	delegate   dispatch.Dispatcher
	keyHandler keys.Handler

	checkGroup  singleflight.Group[string, *v1.DispatchCheckResponse]
	expandGroup singleflight.Group[string, *v1.DispatchExpandResponse]

	checkByDispatchKey  *mapz.CountingMultiMap[string, string]
	expandByDispatchKey *mapz.CountingMultiMap[string, string]
}

func (d *Dispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	key, err := d.keyHandler.CheckDispatchKey(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}},
			status.Error(codes.Internal, "unexpected DispatchCheck error")
	}

	keyString := hex.EncodeToString(key)

	// Check if the key has already been part of a dispatch, for the *same* request ID. If so, this represents a
	// likely recursive call, so we dispatch it to the delegate to avoid the singleflight from blocking it.
	requestID := req.Metadata.RequestId
	existed := d.checkByDispatchKey.Add(keyString, requestID)
	defer d.checkByDispatchKey.Remove(keyString, requestID)

	if existed {
		// Likely a recursive call.
		return d.delegate.DispatchCheck(ctx, req)
	}

	v, isShared, err := d.checkGroup.Do(ctx, keyString, func(innerCtx context.Context) (*v1.DispatchCheckResponse, error) {
		return d.delegate.DispatchCheck(innerCtx, req)
	})

	singleFlightCount.WithLabelValues("DispatchCheck", strconv.FormatBool(isShared)).Inc()
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	}

	return v, err
}

func (d *Dispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	key, err := d.keyHandler.ExpandDispatchKey(ctx, req)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}},
			status.Error(codes.Internal, "unexpected DispatchExpand error")
	}

	keyString := hex.EncodeToString(key)

	// Check if the key has already been part of a dispatch, for the *same* request ID. If so, this represents a
	// likely recursive call, so we dispatch it to the delegate to avoid the singleflight from blocking it.
	requestID := req.Metadata.RequestId
	existed := d.expandByDispatchKey.Add(keyString, requestID)
	defer d.expandByDispatchKey.Remove(keyString, requestID)

	if existed {
		// Likely a recursive call.
		return d.delegate.DispatchExpand(ctx, req)
	}

	v, isShared, err := d.expandGroup.Do(ctx, keyString, func(ictx context.Context) (*v1.DispatchExpandResponse, error) {
		return d.delegate.DispatchExpand(ictx, req)
	})
	singleFlightCount.WithLabelValues("DispatchExpand", strconv.FormatBool(isShared)).Inc()
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, err
	}
	return v, err
}

func (d *Dispatcher) DispatchReachableResources(req *v1.DispatchReachableResourcesRequest, stream dispatch.ReachableResourcesStream) error {
	return d.delegate.DispatchReachableResources(req, stream)
}

func (d *Dispatcher) DispatchLookupResources(req *v1.DispatchLookupResourcesRequest, stream dispatch.LookupResourcesStream) error {
	return d.delegate.DispatchLookupResources(req, stream)
}

func (d *Dispatcher) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	return d.delegate.DispatchLookupSubjects(req, stream)
}

func (d *Dispatcher) Close() error                    { return d.delegate.Close() }
func (d *Dispatcher) ReadyState() dispatch.ReadyState { return d.delegate.ReadyState() }
