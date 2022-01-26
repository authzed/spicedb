package testfixtures

import (
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
)

// NewTestServer creates a grpc.Server instance that has the service specific
// interceptor and consistency interceptor running middleware preinstalled.
func NewTestServer(ds datastore.Datastore) *grpc.Server {
	return grpc.NewServer(
		grpc.ChainUnaryInterceptor(consistency.UnaryServerInterceptor(ds), servicespecific.UnaryServerInterceptor),
		grpc.ChainStreamInterceptor(consistency.StreamServerInterceptor(ds), servicespecific.StreamServerInterceptor),
	)
}
