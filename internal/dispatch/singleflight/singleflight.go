package singleflight

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"resenje.org/singleflight"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/services/shared"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var singleFlightCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "single_flight_total",
	Help:      "total number of dispatch requests that were single flighted",
}, []string{"shared"})

func New(delegate dispatch.Dispatcher) dispatch.Dispatcher {
	return &Dispatcher{delegate: delegate}
}

type Dispatcher struct {
	delegate   dispatch.Dispatcher
	checkGroup singleflight.Group[string, *v1.DispatchCheckResponse]
}

func (d *Dispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	key, err := hashForDispatchCheck(req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{
			DispatchCount: 1,
		}}, status.Error(codes.Internal, "unexpected DispatchCheck error")
	}

	v, isShared, err := d.checkGroup.Do(ctx, key, func(innerCtx context.Context) (*v1.DispatchCheckResponse, error) {
		return d.delegate.DispatchCheck(innerCtx, req)
	})

	singleFlightCount.WithLabelValues(strconv.FormatBool(isShared)).Inc()
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{
			DispatchCount: 1,
		}}, err
	}

	return v.CloneVT(), err
}

func hashForDispatchCheck(req *v1.DispatchCheckRequest) (string, error) {
	key, err := shared.ComputeCallHash("v1.dispatchcheckrequest", nil, map[string]any{
		"revision":          req.Metadata.AtRevision,
		"resource-ids":      req.ResourceIds,
		"resource-type":     req.ResourceRelation.Namespace,
		"resource-relation": req.ResourceRelation.Relation,
		"subject-type":      req.Subject.Namespace,
		"subject-id":        req.Subject.ObjectId,
		"subject-relation":  req.Subject.Relation,
		"result-setting":    req.ResultsSetting.String(),
	})
	return key, err
}

func (d *Dispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return d.delegate.DispatchExpand(ctx, req)
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

func (d *Dispatcher) Close() error {
	return d.delegate.Close()
}

func (d *Dispatcher) ReadyState() dispatch.ReadyState {
	return d.delegate.ReadyState()
}
