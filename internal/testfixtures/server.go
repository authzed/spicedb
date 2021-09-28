package testfixtures

import (
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/middleware/servicespecific"
)

// NewTestServer creates a grpc.Server instance that has the service specific
// interceptor running middleware preinstalled.
func NewTestServer() *grpc.Server {
	return grpc.NewServer(
		grpc.UnaryInterceptor(servicespecific.UnaryServerInterceptor),
		grpc.StreamInterceptor(servicespecific.StreamServerInterceptor),
	)
}
