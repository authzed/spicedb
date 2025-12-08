package datastore

import (
	"context"

	"google.golang.org/grpc"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

// FromContext reads the selected datastore out of a context.Context
// and returns nil if it does not exist.
func FromContext(ctx context.Context) datastore.Datastore {
	return datastoremw.FromContext(ctx)
}

// MustFromContext reads the selected datastore out of a context.Context, computes a zedtoken
// from it, and panics if it has not been set on the context.
func MustFromContext(ctx context.Context) datastore.Datastore {
	datastore := FromContext(ctx)
	if datastore == nil {
		panic("datastore middleware did not inject datastore")
	}

	return datastore
}

// UnaryCountingInterceptor wraps the datastore with a counting proxy for unary requests.
// After each request completes, it exports the method call counts to Prometheus metrics.
func UnaryCountingInterceptor() grpc.UnaryServerInterceptor {
	return datastoremw.UnaryCountingInterceptor()
}

// StreamCountingInterceptor wraps the datastore with a counting proxy for stream requests.
// After each stream completes, it exports the method call counts to Prometheus metrics.
func StreamCountingInterceptor() grpc.StreamServerInterceptor {
	return datastoremw.StreamCountingInterceptor()
}
