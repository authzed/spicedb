package dispatch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/chunkertest"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// chunkerRecorder records the chunker of the context of each dispatch.
type chunkerRecorder struct {
	dispatch.Dispatcher
	got map[string]dispatch.Chunker
}

func (r *chunkerRecorder) DispatchCheck(ctx context.Context, _ *dispatchv1.DispatchCheckRequest) (*dispatchv1.DispatchCheckResponse, error) {
	r.got["check"] = dispatch.ChunkerFromContext(ctx)
	return &dispatchv1.DispatchCheckResponse{}, nil
}

func (r *chunkerRecorder) DispatchExpand(ctx context.Context, _ *dispatchv1.DispatchExpandRequest) (*dispatchv1.DispatchExpandResponse, error) {
	r.got["expand"] = dispatch.ChunkerFromContext(ctx)
	return &dispatchv1.DispatchExpandResponse{}, nil
}

func (r *chunkerRecorder) DispatchLookupResources2(_ *dispatchv1.DispatchLookupResources2Request, stream dispatch.LookupResources2Stream) error {
	r.got["lr2"] = dispatch.ChunkerFromContext(stream.Context())
	return nil
}

func (r *chunkerRecorder) DispatchLookupResources3(_ *dispatchv1.DispatchLookupResources3Request, stream dispatch.LookupResources3Stream) error {
	r.got["lr3"] = dispatch.ChunkerFromContext(stream.Context())
	return nil
}

func (r *chunkerRecorder) DispatchLookupSubjects(_ *dispatchv1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	r.got["ls"] = dispatch.ChunkerFromContext(stream.Context())
	return nil
}

func (r *chunkerRecorder) DispatchQueryPlan(_ *dispatchv1.DispatchQueryPlanRequest, stream dispatch.PlanStream) error {
	r.got["queryplan"] = dispatch.ChunkerFromContext(stream.Context())
	return nil
}

type fakeServerStream[T any] struct {
	grpc.ServerStreamingServer[T]
	ctx context.Context
}

func (s *fakeServerStream[T]) Context() context.Context {
	return s.ctx
}

func TestDispatchServerSetsChunker(t *testing.T) {
	for _, tc := range []struct {
		name    string
		chunker dispatch.Chunker
		want    dispatch.Chunker
	}{
		{"chunker", &chunkertest.ByFirstByte{}, &chunkertest.ByFirstByte{}},
		{"nil chunker", nil, dispatch.DefaultChunker},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &chunkerRecorder{got: map[string]dispatch.Chunker{}}
			srv := NewDispatchServer(rec, tc.chunker)
			ctx := t.Context()

			_, err := srv.DispatchCheck(ctx, &dispatchv1.DispatchCheckRequest{})
			require.NoError(t, err)
			_, err = srv.DispatchExpand(ctx, &dispatchv1.DispatchExpandRequest{})
			require.NoError(t, err)
			require.NoError(t, srv.DispatchLookupResources2(&dispatchv1.DispatchLookupResources2Request{},
				&fakeServerStream[dispatchv1.DispatchLookupResources2Response]{ctx: ctx}))
			require.NoError(t, srv.DispatchLookupResources3(&dispatchv1.DispatchLookupResources3Request{},
				&fakeServerStream[dispatchv1.DispatchLookupResources3Response]{ctx: ctx}))
			require.NoError(t, srv.DispatchLookupSubjects(&dispatchv1.DispatchLookupSubjectsRequest{},
				&fakeServerStream[dispatchv1.DispatchLookupSubjectsResponse]{ctx: ctx}))
			require.NoError(t, srv.DispatchQueryPlan(&dispatchv1.DispatchQueryPlanRequest{},
				&fakeServerStream[dispatchv1.DispatchQueryPlanResponse]{ctx: ctx}))

			require.Equal(t, map[string]dispatch.Chunker{
				"check":     tc.want,
				"expand":    tc.want,
				"lr2":       tc.want,
				"lr3":       tc.want,
				"ls":        tc.want,
				"queryplan": tc.want,
			}, rec.got)
		})
	}
}
