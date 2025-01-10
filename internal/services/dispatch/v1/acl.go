package dispatch

import (
	"context"
	"errors"
	"time"

	"github.com/authzed/spicedb/internal/middleware/streamtimeout"

	"github.com/authzed/spicedb/internal/middleware"

	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/services/shared"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

const streamAPITimeout = 45 * time.Second

type dispatchServer struct {
	dispatchv1.UnimplementedDispatchServiceServer
	shared.WithServiceSpecificInterceptors

	localDispatch dispatch.Dispatcher
}

// NewDispatchServer creates a server which can be called for internal dispatch.
func NewDispatchServer(localDispatch dispatch.Dispatcher) dispatchv1.DispatchServiceServer {
	return &dispatchServer{
		localDispatch: localDispatch,
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Unary: grpcvalidate.UnaryServerInterceptor(),
			Stream: middleware.ChainStreamServer(
				grpcvalidate.StreamServerInterceptor(),
				streamtimeout.MustStreamServerInterceptor(streamAPITimeout),
			),
		},
	}
}

func (ds *dispatchServer) DispatchCheck(ctx context.Context, req *dispatchv1.DispatchCheckRequest) (*dispatchv1.DispatchCheckResponse, error) {
	resp, err := ds.localDispatch.DispatchCheck(ctx, req)
	return resp, rewriteGraphError(ctx, err)
}

func (ds *dispatchServer) DispatchExpand(ctx context.Context, req *dispatchv1.DispatchExpandRequest) (*dispatchv1.DispatchExpandResponse, error) {
	resp, err := ds.localDispatch.DispatchExpand(ctx, req)
	return resp, rewriteGraphError(ctx, err)
}

func (ds *dispatchServer) DispatchLookupResources2(
	req *dispatchv1.DispatchLookupResources2Request,
	resp dispatchv1.DispatchService_DispatchLookupResources2Server,
) error {
	return ds.localDispatch.DispatchLookupResources2(req,
		dispatch.WrapGRPCStream[*dispatchv1.DispatchLookupResources2Response](resp))
}

func (ds *dispatchServer) DispatchLookupSubjects(
	req *dispatchv1.DispatchLookupSubjectsRequest,
	resp dispatchv1.DispatchService_DispatchLookupSubjectsServer,
) error {
	return ds.localDispatch.DispatchLookupSubjects(req,
		dispatch.WrapGRPCStream[*dispatchv1.DispatchLookupSubjectsResponse](resp))
}

func (ds *dispatchServer) Close() error {
	return nil
}

func rewriteGraphError(ctx context.Context, err error) error {
	// Check if the error can be directly used.
	if st, ok := status.FromError(err); ok {
		return st.Err()
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "%s", err)
	case errors.Is(err, context.Canceled):
		return status.Errorf(codes.Canceled, "%s", err)
	case status.Code(err) == codes.Canceled:
		return err
	case err == nil:
		return nil

	case errors.As(err, &graph.AlwaysFailError{}):
		fallthrough
	default:
		log.Ctx(ctx).Err(err).Msg("unexpected dispatch graph error")
		return err
	}
}
