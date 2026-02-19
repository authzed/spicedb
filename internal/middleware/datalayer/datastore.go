package datalayer

import (
	"context"

	"google.golang.org/grpc"

	"github.com/authzed/spicedb/pkg/datalayer"
)

// ContextWithHandle adds a placeholder to a context that will later be
// filled by the datalayer.
func ContextWithHandle(ctx context.Context) context.Context {
	return datalayer.ContextWithHandle(ctx)
}

// FromContext reads the selected DataLayer out of a context.Context
// and returns nil if it does not exist.
func FromContext(ctx context.Context) datalayer.DataLayer {
	return datalayer.FromContext(ctx)
}

// MustFromContext reads the selected DataLayer out of a context.Context and panics if it does not exist
func MustFromContext(ctx context.Context) datalayer.DataLayer {
	return datalayer.MustFromContext(ctx)
}

// SetInContext adds a DataLayer to the given context
func SetInContext(ctx context.Context, dl datalayer.DataLayer) error {
	return datalayer.SetInContext(ctx, dl)
}

// ContextWithDataLayer adds the handle and DataLayer in one step
func ContextWithDataLayer(ctx context.Context, dl datalayer.DataLayer) context.Context {
	return datalayer.ContextWithDataLayer(ctx, dl)
}

// UnaryServerInterceptor returns a new unary server interceptor that adds the
// DataLayer to the context
func UnaryServerInterceptor(dl datalayer.DataLayer) grpc.UnaryServerInterceptor {
	return datalayer.UnaryServerInterceptor(dl)
}

// StreamServerInterceptor returns a new stream server interceptor that adds the
// DataLayer to the context
func StreamServerInterceptor(dl datalayer.DataLayer) grpc.StreamServerInterceptor {
	return datalayer.StreamServerInterceptor(dl)
}
