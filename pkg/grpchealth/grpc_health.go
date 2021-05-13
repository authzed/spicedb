package grpchealth

import (
	"context"

	"google.golang.org/grpc/health"
)

// AuthlessServer implements a gRPC health endpoint that will ignore any auth
// requirements set by github.com/grpc-ecosystem/go-grpc-middleware/auth.
type AuthlessServer struct {
	*health.Server
}

// NewAuthlessServer returns a new gRPC health server that ignores auth
// middleware.
func NewAuthlessServer() *AuthlessServer {
	return &AuthlessServer{health.NewServer()}
}

func (s *AuthlessServer) AuthFuncOverride(ctx context.Context, methodName string) (context.Context, error) {
	return ctx, nil
}
