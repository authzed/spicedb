package consistentbackend

import (
	"context"

	"google.golang.org/grpc"

	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
)

func (sc *ConsistentBackendClient) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest, opts ...grpc.CallOption) (*v1.DispatchCheckResponse, error) {
	backend, err := sc.getConsistentBackend(req)
	if err != nil {
		return nil, err
	}

	return backend.client.DispatchCheck(ctx, req, opts...)
}

func (sc *ConsistentBackendClient) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest, opts ...grpc.CallOption) (*v1.DispatchExpandResponse, error) {
	backend, err := sc.getConsistentBackend(req)
	if err != nil {
		return nil, err
	}

	return backend.client.DispatchExpand(ctx, req, opts...)
}

func (sc *ConsistentBackendClient) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest, opts ...grpc.CallOption) (*v1.DispatchLookupResponse, error) {
	backend, err := sc.getConsistentBackend(req)
	if err != nil {
		return nil, err
	}

	return backend.client.DispatchLookup(ctx, req, opts...)
}
