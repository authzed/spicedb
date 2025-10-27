package v1

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// checkPermissionWithQueryPlan is a stub implementation for the experimental query plan API.
// This will be implemented in future PRs to return a query plan instead of just a boolean result.
func (ps *permissionServer) checkPermissionWithQueryPlan(ctx context.Context, req *v1.CheckPermissionRequest) (*v1.CheckPermissionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "query plan API is not yet implemented")
}
