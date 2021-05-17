package grpcutil

import (
	"google.golang.org/grpc/health"
)

// AuthlessHealthServer implements a gRPC health endpoint that will ignore any auth
// requirements set by github.com/grpc-ecosystem/go-grpc-middleware/auth.
type AuthlessHealthServer struct {
	*health.Server
	IgnoreAuthMixin
}

// NewAuthlessServer returns a new gRPC health server that ignores auth
// middleware.
func NewAuthlessHealthServer() *AuthlessHealthServer {
	return &AuthlessHealthServer{Server: health.NewServer()}
}
