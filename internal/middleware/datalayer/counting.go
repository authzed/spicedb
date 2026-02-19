package datalayer

import (
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/pkg/datalayer"
)

// UnaryCountingInterceptor wraps the datalayer in a counting proxy for each unary request.
// After the request completes, it exports the counts to Prometheus metrics.
func UnaryCountingInterceptor() grpc.UnaryServerInterceptor {
	return datalayer.UnaryCountingInterceptor(nil)
}

// StreamCountingInterceptor wraps the datalayer in a counting proxy for each stream request.
// After the stream completes, it exports the counts to Prometheus metrics.
func StreamCountingInterceptor() grpc.StreamServerInterceptor {
	return datalayer.StreamCountingInterceptor(nil)
}
