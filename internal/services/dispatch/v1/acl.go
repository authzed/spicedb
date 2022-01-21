package dispatch

import (
	"context"
	"errors"

	"github.com/authzed/grpcutil"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type dispatchServer struct {
	dispatchv1.UnimplementedDispatchServiceServer

	localDispatch dispatch.Dispatcher
}

// RegisterDispatchServer adds the Dispatch Server to a grpc service registrar
// This is preferred over manually registering the service; it will add required middleware
func RegisterDispatchServer(r grpc.ServiceRegistrar, s dispatchv1.DispatchServiceServer) *grpc.ServiceDesc {
	r.RegisterService(grpcutil.WrapMethods(dispatchv1.DispatchService_ServiceDesc, grpcutil.DefaultUnaryMiddleware...), s)
	return &dispatchv1.DispatchService_ServiceDesc
}

// NewDispatchServer creates a server which can be called for internal dispatch.
func NewDispatchServer(localDispatch dispatch.Dispatcher) dispatchv1.DispatchServiceServer {
	return &dispatchServer{
		localDispatch: localDispatch,
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

func (ds *dispatchServer) DispatchLookup(ctx context.Context, req *dispatchv1.DispatchLookupRequest) (*dispatchv1.DispatchLookupResponse, error) {
	resp, err := ds.localDispatch.DispatchLookup(ctx, req)
	return resp, rewriteGraphError(ctx, err)
}

func rewriteGraphError(ctx context.Context, err error) error {
	switch {
	case errors.As(err, &graph.ErrRequestCanceled{}):
		return status.Errorf(codes.Canceled, "request canceled: %s", err)

	case err == nil:
		return nil

	case errors.As(err, &graph.ErrAlwaysFail{}):
		fallthrough
	default:
		log.Err(err)
		return err
	}
}
