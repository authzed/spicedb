package servicespecific

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"resenje.org/singleflight"

	"github.com/authzed/spicedb/internal/services/shared"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var singleFlightCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "dispatch",
	Name:      "single_flight_total",
	Help:      "total number of dispatch requests that were single flighted",
}, []string{"shared"})

func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	var group singleflight.Group[string, any]
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		key, eligible, err := eligibleRequest(req)
		if err != nil {
			return nil, status.Error(codes.Internal, "unexpected dispatch middleware error")
		}

		if !eligible {
			return handler(ctx, req)
		}

		v, isShared, err := group.Do(ctx, key, func(innerCtx context.Context) (any, error) {
			return handler(innerCtx, req)
		})

		singleFlightCount.WithLabelValues(strconv.FormatBool(isShared)).Inc()
		return v, err
	}
}

func eligibleRequest(req any) (string, bool, error) {
	switch typedReq := req.(type) {
	case *dispatch.DispatchCheckRequest:
		key, err := shared.ComputeCallHash("v1.dispatchcheckrequest", nil, map[string]any{
			"revision":          typedReq.Metadata.AtRevision,
			"resource-ids":      typedReq.ResourceIds,
			"resource-type":     typedReq.ResourceRelation.Namespace,
			"resource-relation": typedReq.ResourceRelation.Relation,
			"subject-type":      typedReq.Subject.Namespace,
			"subject-id":        typedReq.Subject.ObjectId,
			"subject-relation":  typedReq.Subject.Relation,
			"result-setting":    typedReq.ResultsSetting.String(),
		})

		return key, true, err
	default:
		return "", false, nil
	}
}
