package dispatch

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/middleware"
	"github.com/authzed/spicedb/internal/middleware/streamtimeout"
	"github.com/authzed/spicedb/internal/services/shared"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

const streamAPITimeout = 45 * time.Second

type dispatchServer struct {
	dispatchv1.UnimplementedDispatchServiceServer
	shared.WithServiceSpecificInterceptors

	localDispatch dispatch.Dispatcher

	// chunker forms the dispatch chunks of the requests that this server receives.
	chunker dispatch.Chunker
}

// NewDispatchServer creates a server which can be called for internal dispatch.
// The server puts chunker on the context of each request, so the dispatches of localDispatch use it.
// Use the chunker of the dispatcher that localDispatch sends dispatches to. A nil chunker selects dispatch.DefaultChunker.
func NewDispatchServer(localDispatch dispatch.Dispatcher, chunker dispatch.Chunker) dispatchv1.DispatchServiceServer {
	return &dispatchServer{
		localDispatch: localDispatch,
		chunker:       chunker,
		WithServiceSpecificInterceptors: shared.WithServiceSpecificInterceptors{
			Stream: middleware.ChainStreamServer(
				streamtimeout.MustStreamServerInterceptor(streamAPITimeout),
			),
		},
	}
}

func (ds *dispatchServer) DispatchCheck(ctx context.Context, req *dispatchv1.DispatchCheckRequest) (*dispatchv1.DispatchCheckResponse, error) {
	resp, err := ds.localDispatch.DispatchCheck(dispatch.ContextWithChunker(ctx, ds.chunker), req)
	return resp, rewriteGraphError(ctx, err)
}

func (ds *dispatchServer) DispatchExpand(ctx context.Context, req *dispatchv1.DispatchExpandRequest) (*dispatchv1.DispatchExpandResponse, error) {
	resp, err := ds.localDispatch.DispatchExpand(dispatch.ContextWithChunker(ctx, ds.chunker), req)
	return resp, rewriteGraphError(ctx, err)
}

func (ds *dispatchServer) DispatchLookupResources2(
	req *dispatchv1.DispatchLookupResources2Request,
	resp dispatchv1.DispatchService_DispatchLookupResources2Server,
) error {
	return ds.localDispatch.DispatchLookupResources2(req, withChunker(ds.chunker, dispatch.WrapGRPCStream(resp)))
}

func (ds *dispatchServer) DispatchLookupResources3(
	req *dispatchv1.DispatchLookupResources3Request,
	resp dispatchv1.DispatchService_DispatchLookupResources3Server,
) error {
	return ds.localDispatch.DispatchLookupResources3(req, withChunker(ds.chunker, dispatch.WrapGRPCStream(resp)))
}

func (ds *dispatchServer) DispatchLookupSubjects(
	req *dispatchv1.DispatchLookupSubjectsRequest,
	resp dispatchv1.DispatchService_DispatchLookupSubjectsServer,
) error {
	return ds.localDispatch.DispatchLookupSubjects(req, withChunker(ds.chunker, dispatch.WrapGRPCStream(resp)))
}

func (ds *dispatchServer) DispatchQueryPlan(
	req *dispatchv1.DispatchQueryPlanRequest,
	resp dispatchv1.DispatchService_DispatchQueryPlanServer,
) error {
	return ds.localDispatch.DispatchQueryPlan(req, withChunker(ds.chunker, dispatch.WrapGRPCStream(resp)))
}

// withChunker returns stream with chunker on its context.
func withChunker[T any](chunker dispatch.Chunker, stream dispatch.Stream[T]) dispatch.Stream[T] {
	return dispatch.StreamWithContext(dispatch.ContextWithChunker(stream.Context(), chunker), stream)
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
