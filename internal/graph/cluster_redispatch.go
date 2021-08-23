package graph

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
)

const (
	errCheckRedispatch  = "error remotely dispatching check request: %w"
	errExpandRedispatch = "error remotely dispatching expand request: %w"
	errLookupRedispatch = "error remotely dispatching lookup request: %w"
)

type clusterClient interface {
	DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest, opts ...grpc.CallOption) (*v1.DispatchCheckResponse, error)
	DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest, opts ...grpc.CallOption) (*v1.DispatchExpandResponse, error)
	DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest, opts ...grpc.CallOption) (*v1.DispatchLookupResponse, error)
}

// NewClusterDispatcher creates a dispatcher implementation that uses the provided client
// to dispatch requests to peer nodes in the cluster.
func NewClusterDispatcher(client clusterClient, depthRemainingHeader, forcedRevisionHeader string) Dispatcher {
	return &clusterDispatcher{client, depthRemainingHeader, forcedRevisionHeader}
}

type clusterDispatcher struct {
	clusterClient        clusterClient
	depthRemainingHeader string
	forcedRevisionHeader string
}

func (cr *clusterDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) CheckResult {
	err := checkDepth(req)
	if err != nil {
		return checkResultError(err, 0)
	}

	resp, err := cr.clusterClient.DispatchCheck(ctx, req)
	if err != nil {
		return CheckResult{
			Resp: resp,
			Err:  fmt.Errorf(errCheckRedispatch, err),
		}
	}

	return CheckResult{
		Resp: resp,
	}
}

func (cr *clusterDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) ExpandResult {
	err := checkDepth(req)
	if err != nil {
		return expandResultError(err, 0)
	}

	resp, err := cr.clusterClient.DispatchExpand(ctx, req)
	if err != nil {
		return ExpandResult{
			Resp: resp,
			Err:  fmt.Errorf(errExpandRedispatch, err),
		}
	}

	return ExpandResult{
		Resp: resp,
	}
}

func (cr *clusterDispatcher) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) LookupResult {
	err := checkDepth(req)
	if err != nil {
		return lookupResultError(err, 0)
	}

	resp, err := cr.clusterClient.DispatchLookup(ctx, req)
	if err != nil {
		return lookupResultError(fmt.Errorf(errLookupRedispatch, err), resp.Metadata.DispatchCount)
	}

	return LookupResult{
		Resp: resp,
	}
}

// Always verify that we implement the interface
var _ Dispatcher = &clusterDispatcher{}
