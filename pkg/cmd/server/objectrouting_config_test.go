package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/chunkertest"
	"github.com/authzed/spicedb/internal/middleware/memoryprotection"
)

func TestObjectAffinityRoutingConfigDefaults(t *testing.T) {
	cfg := NewConfigWithOptionsAndDefaults()
	require.Equal(t, "disabled", cfg.ExperimentalObjectAffinityRouting)
	require.Zero(t, cfg.DispatchObjectSpreadShare)
	require.Equal(t, uint8(2), cfg.DispatchObjectSpread)
	require.InDelta(t, 1.5, cfg.DispatchObjectSpreadLatencyFactor, 0)
}

func TestObjectSpreadConfigOptions(t *testing.T) {
	cfg := NewConfigWithOptionsAndDefaults(
		WithDispatchObjectSpreadShare(0.5),
		WithDispatchObjectSpreadLatencyFactor(2),
	)
	require.InDelta(t, 0.5, cfg.DispatchObjectSpreadShare, 0)
	require.InDelta(t, 2, cfg.DispatchObjectSpreadLatencyFactor, 0)
}

func TestDispatchChunkerOption(t *testing.T) {
	require.Nil(t, NewConfigWithOptionsAndDefaults().DispatchChunker)
	cfg := NewConfigWithOptionsAndDefaults(WithDispatchChunker(dispatch.DefaultChunker))
	require.Equal(t, dispatch.DefaultChunker, cfg.DispatchChunker)
}

type chunkerServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *chunkerServerStream) Context() context.Context {
	return s.ctx
}

func TestDefaultMiddlewareSetsDispatchChunker(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, time.Hour, time.Hour)
	require.NoError(t, err)

	for _, tc := range []struct {
		name    string
		chunker dispatch.Chunker
		want    dispatch.Chunker
	}{
		{"chunker", &chunkertest.ByFirstByte{}, &chunkertest.ByFirstByte{}},
		{"nil chunker", nil, dispatch.DefaultChunker},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := MiddlewareOption{
				MemoryUsageProvider:          memoryprotection.NewNoopMemoryUsageProvider(),
				DispatchChunkerForMiddleware: tc.chunker,
			}.WithDatastore(ds)

			unaryChain, err := DefaultUnaryMiddleware(opts)
			require.NoError(t, err)
			var unary grpc.UnaryServerInterceptor
			for _, mw := range unaryChain.chain {
				if mw.Name == DefaultInternalMiddlewareDispatchChunker {
					require.True(t, mw.Internal)
					unary = mw.Middleware
				}
			}
			require.NotNil(t, unary)

			var got dispatch.Chunker
			_, err = unary(t.Context(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
				got = dispatch.ChunkerFromContext(ctx)
				return nil, nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)

			streamChain, err := DefaultStreamingMiddleware(opts)
			require.NoError(t, err)
			var stream grpc.StreamServerInterceptor
			for _, mw := range streamChain.chain {
				if mw.Name == DefaultInternalMiddlewareDispatchChunker {
					require.True(t, mw.Internal)
					stream = mw.Middleware
				}
			}
			require.NotNil(t, stream)

			got = nil
			err = stream(nil, &chunkerServerStream{ctx: t.Context()}, &grpc.StreamServerInfo{}, func(_ any, ss grpc.ServerStream) error {
				got = dispatch.ChunkerFromContext(ss.Context())
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
