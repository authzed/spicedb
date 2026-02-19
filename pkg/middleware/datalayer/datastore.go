package datalayer

import (
	"context"

	"google.golang.org/grpc"

	datalayermw "github.com/authzed/spicedb/internal/middleware/datalayer"
	"github.com/authzed/spicedb/pkg/datalayer"
)

// FromContext reads the selected DataLayer out of a context.Context
// and returns nil if it does not exist.
func FromContext(ctx context.Context) datalayer.DataLayer {
	return datalayermw.FromContext(ctx)
}

// MustFromContext reads the selected DataLayer out of a context.Context, computes a zedtoken
// from it, and panics if it has not been set on the context.
func MustFromContext(ctx context.Context) datalayer.DataLayer {
	dl := FromContext(ctx)
	if dl == nil {
		panic("datastore middleware did not inject datalayer")
	}

	return dl
}

// UnaryCountingInterceptor wraps the datalayer with a counting proxy for unary requests.
// After each request completes, it exports the method call counts to Prometheus metrics.
func UnaryCountingInterceptor() grpc.UnaryServerInterceptor {
	return datalayermw.UnaryCountingInterceptor()
}

// StreamCountingInterceptor wraps the datalayer with a counting proxy for stream requests.
// After each stream completes, it exports the method call counts to Prometheus metrics.
func StreamCountingInterceptor() grpc.StreamServerInterceptor {
	return datalayermw.StreamCountingInterceptor()
}
