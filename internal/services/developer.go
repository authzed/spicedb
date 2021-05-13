package services

import (
	"context"

	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

type devServer struct {
	api.UnimplementedDeveloperServiceServer
}

// Implement the ServiceAuthFuncOverride interface to ignore any auth
// requirements set by github.com/grpc-ecosystem/go-grpc-middleware/auth.
func (ds *devServer) AuthFuncOverride(ctx context.Context, methodName string) (context.Context, error) {
	return ctx, nil
}

// NewDeveloperServer creates an instance of the developer server.
func NewDeveloperServer() api.DeveloperServiceServer {
	s := &devServer{}
	return s
}
