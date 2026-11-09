package dispatch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *fakeServerStream) Context() context.Context {
	return s.ctx
}

func TestUnaryServerInterceptor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		chunker Chunker
		want    Chunker
	}{
		{"chunker", otherChunker{}, otherChunker{}},
		{"nil chunker", nil, DefaultChunker},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got Chunker
			_, err := UnaryServerInterceptor(tc.chunker)(t.Context(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
				got = ChunkerFromContext(ctx)
				return nil, nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestStreamServerInterceptor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		chunker Chunker
		want    Chunker
	}{
		{"chunker", otherChunker{}, otherChunker{}},
		{"nil chunker", nil, DefaultChunker},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got Chunker
			stream := &fakeServerStream{ctx: t.Context()}
			err := StreamServerInterceptor(tc.chunker)(nil, stream, &grpc.StreamServerInfo{}, func(_ any, stream grpc.ServerStream) error {
				got = ChunkerFromContext(stream.Context())
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
