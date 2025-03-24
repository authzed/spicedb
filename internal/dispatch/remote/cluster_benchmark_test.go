package remote

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dispatch/keys"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type fakeClusterClient struct {
	isSecondary bool
}

func (fcc fakeClusterClient) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest, opts ...grpc.CallOption) (*v1.DispatchCheckResponse, error) {
	return &v1.DispatchCheckResponse{
		Metadata: &v1.ResponseMeta{DispatchCount: 1, DepthRequired: 1},
	}, nil
}

func (fakeClusterClient) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest, opts ...grpc.CallOption) (*v1.DispatchExpandResponse, error) {
	return &v1.DispatchExpandResponse{}, nil
}

func (fakeClusterClient) DispatchLookupResources2(ctx context.Context, in *v1.DispatchLookupResources2Request, opts ...grpc.CallOption) (v1.DispatchService_DispatchLookupResources2Client, error) {
	return nil, nil
}

func (fakeClusterClient) DispatchLookupSubjects(ctx context.Context, in *v1.DispatchLookupSubjectsRequest, opts ...grpc.CallOption) (v1.DispatchService_DispatchLookupSubjectsClient, error) {
	return nil, nil
}

func BenchmarkSecondaryDispatching(b *testing.B) {
	client := fakeClusterClient{false}
	config := ClusterDispatcherConfig{
		KeyHandler:             &keys.DirectKeyHandler{},
		DispatchOverallTimeout: 30 * time.Second,
	}

	parsed, err := ParseDispatchExpression("check", "['secondary']")
	require.NoError(b, err)

	secondaryDispatch := map[string]SecondaryDispatch{
		"secondary": {Name: "secondary", Client: fakeClusterClient{true}},
	}
	secondaryDispatchExprs := map[string]*DispatchExpr{
		"check": parsed,
	}

	dispatcher, err := NewClusterDispatcher(client, nil, config, secondaryDispatch, secondaryDispatchExprs, 0*time.Second)
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = dispatcher.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
			ResourceRelation: &corev1.RelationReference{Namespace: "sometype", Relation: "somerel"},
			ResourceIds:      []string{"foo"},
			Metadata:         &v1.ResolverMeta{DepthRemaining: 50},
			Subject:          &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
		})
		require.NoError(b, err)
	}
}
