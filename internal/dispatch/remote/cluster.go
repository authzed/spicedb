package remote

import (
	"context"
	"io"

	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/pkg/balancer"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type clusterClient interface {
	DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest, opts ...grpc.CallOption) (*v1.DispatchCheckResponse, error)
	DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest, opts ...grpc.CallOption) (*v1.DispatchExpandResponse, error)
	DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest, opts ...grpc.CallOption) (*v1.DispatchLookupResponse, error)
	DispatchReachableResources(ctx context.Context, in *v1.DispatchReachableResourcesRequest, opts ...grpc.CallOption) (v1.DispatchService_DispatchReachableResourcesClient, error)
}

// NewClusterDispatcher creates a dispatcher implementation that uses the provided client
// to dispatch requests to peer nodes in the cluster.
func NewClusterDispatcher(client clusterClient, keyHandler keys.Handler) dispatch.Dispatcher {
	if keyHandler == nil {
		keyHandler = &keys.DirectKeyHandler{}
	}

	return &clusterDispatcher{clusterClient: client, keyHandler: keyHandler}
}

type clusterDispatcher struct {
	v1.UnimplementedDispatchServiceServer

	clusterClient clusterClient
	keyHandler    keys.Handler
}

func (cr *clusterDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	err := dispatch.CheckDepth(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	requestKey, err := cr.keyHandler.ComputeCheckKey(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	ctx = context.WithValue(ctx, balancer.CtxKey, []byte(requestKey))
	resp, err := cr.clusterClient.DispatchCheck(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: requestFailureMetadata}, err
	}

	return resp, nil
}

func (cr *clusterDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	err := dispatch.CheckDepth(ctx, req)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}
	ctx = context.WithValue(ctx, balancer.CtxKey, []byte(dispatch.ExpandRequestToKey(req)))
	resp, err := cr.clusterClient.DispatchExpand(ctx, req)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: requestFailureMetadata}, err
	}

	return resp, nil
}

func (cr *clusterDispatcher) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	err := dispatch.CheckDepth(ctx, req)
	if err != nil {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata}, err
	}
	ctx = context.WithValue(ctx, balancer.CtxKey, []byte(dispatch.LookupRequestToKey(req)))
	resp, err := cr.clusterClient.DispatchLookup(ctx, req)
	if err != nil {
		return &v1.DispatchLookupResponse{Metadata: requestFailureMetadata}, err
	}

	return resp, nil
}

func (cr *clusterDispatcher) DispatchReachableResources(
	req *v1.DispatchReachableResourcesRequest,
	stream v1.DispatchService_DispatchReachableResourcesServer,
) error {
	ctx := stream.Context()
	ctx = context.WithValue(ctx, balancer.CtxKey, []byte(dispatch.ReachableResourcesRequestToKey(req)))
	err := dispatch.CheckDepth(ctx, req)
	if err != nil {
		return err
	}

	client, err := cr.clusterClient.DispatchReachableResources(ctx, req)
	if err != nil {
		return err
	}

	for {
		result, err := client.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		serr := stream.Send(result)
		if serr != nil {
			return serr
		}
	}
}

func (cr *clusterDispatcher) Close() error {
	return nil
}

// Always verify that we implement the interfaces
var _ dispatch.Dispatcher = &clusterDispatcher{}
var _ v1.DispatchServiceServer = &clusterDispatcher{}

var emptyMetadata *v1.ResponseMeta = &v1.ResponseMeta{
	DispatchCount: 0,
}

var requestFailureMetadata *v1.ResponseMeta = &v1.ResponseMeta{
	DispatchCount: 1,
}
