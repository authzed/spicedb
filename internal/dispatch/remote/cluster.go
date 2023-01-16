package remote

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/balancer"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type clusterClient interface {
	DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest, opts ...grpc.CallOption) (*v1.DispatchCheckResponse, error)
	DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest, opts ...grpc.CallOption) (*v1.DispatchExpandResponse, error)
	DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest, opts ...grpc.CallOption) (*v1.DispatchLookupResponse, error)
	DispatchReachableResources(ctx context.Context, in *v1.DispatchReachableResourcesRequest, opts ...grpc.CallOption) (v1.DispatchService_DispatchReachableResourcesClient, error)
	DispatchLookupSubjects(ctx context.Context, in *v1.DispatchLookupSubjectsRequest, opts ...grpc.CallOption) (v1.DispatchService_DispatchLookupSubjectsClient, error)
}

// NewClusterDispatcher creates a dispatcher implementation that uses the provided client
// to dispatch requests to peer nodes in the cluster.
func NewClusterDispatcher(client clusterClient, conn *grpc.ClientConn, keyHandler keys.Handler) dispatch.Dispatcher {
	if keyHandler == nil {
		keyHandler = &keys.DirectKeyHandler{}
	}

	return &clusterDispatcher{clusterClient: client, conn: conn, keyHandler: keyHandler}
}

type clusterDispatcher struct {
	clusterClient clusterClient
	conn          *grpc.ClientConn
	keyHandler    keys.Handler
}

func (cr *clusterDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	requestKey, err := cr.keyHandler.CheckDispatchKey(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}

	ctx = context.WithValue(ctx, balancer.CtxKey, requestKey)
	resp, err := cr.clusterClient.DispatchCheck(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: requestFailureMetadata}, err
	}

	return resp, nil
}

func (cr *clusterDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	requestKey, err := cr.keyHandler.ExpandDispatchKey(ctx, req)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: emptyMetadata}, err
	}

	ctx = context.WithValue(ctx, balancer.CtxKey, requestKey)
	resp, err := cr.clusterClient.DispatchExpand(ctx, req)
	if err != nil {
		return &v1.DispatchExpandResponse{Metadata: requestFailureMetadata}, err
	}

	return resp, nil
}

func (cr *clusterDispatcher) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata}, err
	}

	requestKey, err := cr.keyHandler.LookupResourcesDispatchKey(ctx, req)
	if err != nil {
		return &v1.DispatchLookupResponse{Metadata: emptyMetadata}, err
	}

	ctx = context.WithValue(ctx, balancer.CtxKey, requestKey)
	resp, err := cr.clusterClient.DispatchLookup(ctx, req)
	if err != nil {
		return &v1.DispatchLookupResponse{Metadata: requestFailureMetadata}, err
	}

	return resp, nil
}

func (cr *clusterDispatcher) DispatchReachableResources(
	req *v1.DispatchReachableResourcesRequest,
	stream dispatch.ReachableResourcesStream,
) error {
	requestKey, err := cr.keyHandler.ReachableResourcesDispatchKey(stream.Context(), req)
	if err != nil {
		return err
	}

	ctx := context.WithValue(stream.Context(), balancer.CtxKey, requestKey)
	stream = dispatch.StreamWithContext(ctx, stream)

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return err
	}

	client, err := cr.clusterClient.DispatchReachableResources(ctx, req)
	if err != nil {
		return err
	}

	for {
		result, err := client.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return err
		}

		serr := stream.Publish(result)
		if serr != nil {
			return serr
		}
	}

	return nil
}

func (cr *clusterDispatcher) DispatchLookupSubjects(
	req *v1.DispatchLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
) error {
	requestKey, err := cr.keyHandler.LookupSubjectsDispatchKey(stream.Context(), req)
	if err != nil {
		return err
	}

	ctx := context.WithValue(stream.Context(), balancer.CtxKey, requestKey)
	stream = dispatch.StreamWithContext(ctx, stream)

	if err := dispatch.CheckDepth(ctx, req); err != nil {
		return err
	}

	client, err := cr.clusterClient.DispatchLookupSubjects(ctx, req)
	if err != nil {
		return err
	}

	for {
		result, err := client.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return err
		}

		serr := stream.Publish(result)
		if serr != nil {
			return serr
		}
	}

	return nil
}

func (cr *clusterDispatcher) Close() error {
	return nil
}

// IsReady returns whether the underlying dispatch connection is available
func (cr *clusterDispatcher) IsReady() bool {
	state := cr.conn.GetState()
	log.Trace().Interface("connection-state", state).Msg("checked if cluster dispatcher is ready")
	return state == connectivity.Ready || state == connectivity.Idle
}

// Always verify that we implement the interface
var _ dispatch.Dispatcher = &clusterDispatcher{}

var emptyMetadata = &v1.ResponseMeta{
	DispatchCount: 0,
}

var requestFailureMetadata = &v1.ResponseMeta{
	DispatchCount: 1,
}
