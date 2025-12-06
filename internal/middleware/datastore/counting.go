package datastore

import (
	"context"

	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/proxy"
)

// UnaryCountingInterceptor wraps the datastore in a counting proxy for each unary request.
// After the request completes, it exports the counts to Prometheus metrics.
func UnaryCountingInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Get the current datastore from context
		ds := MustFromContext(ctx)

		// Wrap with counting proxy
		countingDS, counts := proxy.NewCountingDatastoreProxyForDatastore(ds)

		// Set the wrapped datastore back into context
		if err := SetInContext(ctx, countingDS); err != nil {
			return nil, err
		}

		// Handle the request
		resp, err := handler(ctx, req)

		// Export counts to Prometheus after request completes
		proxy.WriteMethodCounts(counts)

		return resp, err
	}
}

// StreamCountingInterceptor wraps the datastore in a counting proxy for each stream request.
// After the stream completes, it exports the counts to Prometheus metrics.
func StreamCountingInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Get the current datastore from context
		ctx := ss.Context()
		ds := MustFromContext(ctx)

		// Wrap with counting proxy
		countingDS, counts := proxy.NewCountingDatastoreProxyForDatastore(ds)

		// Set the wrapped datastore back into context
		if err := SetInContext(ctx, countingDS); err != nil {
			return err
		}

		// Handle the stream
		err := handler(srv, ss)

		// Export counts to Prometheus after stream completes
		proxy.WriteMethodCounts(counts)

		return err
	}
}
