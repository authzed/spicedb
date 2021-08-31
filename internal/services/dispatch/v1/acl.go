package dispatch

import (
	"context"
	"errors"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dispatchServer struct {
	v1.UnimplementedDispatchServiceServer

	localDispatch dispatch.Dispatcher
}

// NewDispatchServer creates a server which can be called for internal dispatch.
func NewDispatchServer(localDispatch dispatch.Dispatcher) v1.DispatchServiceServer {
	return &dispatchServer{
		localDispatch: localDispatch,
	}
}

func (ds *dispatchServer) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	resp, err := ds.localDispatch.DispatchCheck(ctx, req)
	return resp, rewriteGraphError(err)
}

func (ds *dispatchServer) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	resp, err := ds.localDispatch.DispatchExpand(ctx, req)
	return resp, rewriteGraphError(err)
}

func (ds *dispatchServer) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	resp, err := ds.localDispatch.DispatchLookup(ctx, req)
	return resp, rewriteGraphError(err)
}

func rewriteGraphError(err error) error {
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
