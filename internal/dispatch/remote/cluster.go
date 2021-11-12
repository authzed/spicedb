package remote

import (
	"bytes"
	"context"

	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/balancer"
)

type clusterClient interface {
	DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest, opts ...grpc.CallOption) (*v1.DispatchCheckResponse, error)
	DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest, opts ...grpc.CallOption) (*v1.DispatchExpandResponse, error)
	DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest, opts ...grpc.CallOption) (*v1.DispatchLookupResponse, error)
}

// NewClusterDispatcher creates a dispatcher implementation that uses the provided client
// to dispatch requests to peer nodes in the cluster.
func NewClusterDispatcher(client clusterClient) dispatch.Dispatcher {
	return &clusterDispatcher{client}
}

type clusterDispatcher struct {
	clusterClient clusterClient
}

func (cr *clusterDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	err := dispatch.CheckDepth(ctx, req)
	if err != nil {
		return &v1.DispatchCheckResponse{Metadata: emptyMetadata}, err
	}
	requestKey := bytes.Join([][]byte{
		[]byte(req.ObjectAndRelation.String()),
		[]byte(req.Subject.String()),
		[]byte(req.Metadata.AtRevision),
	}, []byte("-"))
	ctx = context.WithValue(ctx, balancer.CtxKey, requestKey)
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

	requestKey := bytes.Join([][]byte{
		[]byte(req.ObjectAndRelation.String()),
		[]byte(req.ExpansionMode.String()),
		[]byte(req.Metadata.AtRevision),
	}, []byte("-"))
	ctx = context.WithValue(ctx, balancer.CtxKey, requestKey)
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

	requestKey := bytes.Join([][]byte{
		[]byte(req.ObjectRelation.String()),
		[]byte(req.Subject.String()),
		[]byte(req.Metadata.AtRevision),
	}, []byte("-"))
	for _, d := range req.DirectStack {
		requestKey = bytes.Join([][]byte{
			requestKey,
			[]byte("d"),
			[]byte(d.String()),
		}, []byte("-"))
	}
	for _, t := range req.TtuStack {
		requestKey = bytes.Join([][]byte{
			requestKey,
			[]byte("t"),
			[]byte(t.String()),
		}, []byte("-"))
	}
	ctx = context.WithValue(ctx, balancer.CtxKey, requestKey)
	resp, err := cr.clusterClient.DispatchLookup(ctx, req)
	if err != nil {
		return &v1.DispatchLookupResponse{Metadata: requestFailureMetadata}, err
	}

	return resp, nil
}

func (cr *clusterDispatcher) Close() error {
	return nil
}

// Always verify that we implement the interface
var _ dispatch.Dispatcher = &clusterDispatcher{}

var emptyMetadata *v1.ResponseMeta = &v1.ResponseMeta{
	DispatchCount: 0,
}

var requestFailureMetadata *v1.ResponseMeta = &v1.ResponseMeta{
	DispatchCount: 1,
}
