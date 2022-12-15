package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/memdb"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestServerGracefulTermination(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel := context.WithCancel(context.Background())
	ds, err := memdb.NewMemdbDatastore(0, 1*time.Second, 10*time.Second)
	require.NoError(t, err)
	c := ConfigWithOptions(&Config{}, WithPresharedKey("psk"), WithDatastore(ds))
	rs, err := c.Complete(ctx)
	require.NoError(t, err)

	ch := make(chan struct{}, 1)
	go func() {
		require.NoError(t, rs.Run(ctx))
		ch <- struct{}{}
	}()
	cancel()
	<-ch
}

func TestBuildMiddleware(t *testing.T) {
	// Test fully replacing default Middleware
	var replaceUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 1}.unaryIntercept
	var replaceStream grpc.StreamServerInterceptor = mockStreamInterceptor{val: errors.New("1")}.streamIntercept
	replacedUnary := []grpc.UnaryServerInterceptor{replaceUnary}
	replacedStream := []grpc.StreamServerInterceptor{replaceStream}
	c := ConfigWithOptions(&Config{}, SetReplaceDefaultUnaryMiddleware(replacedUnary), SetReplaceDefaultStreamingMiddleware(replacedStream))
	outUnary, outStream := c.buildMiddleware(nil)
	require.Equal(t, replacedUnary, outUnary)
	require.Equal(t, replacedStream, outStream)

	// Test prepending and appending to Default middleware
	var prependUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 2}.unaryIntercept
	var prependStream grpc.StreamServerInterceptor = mockStreamInterceptor{val: errors.New("2")}.streamIntercept
	var appendUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 3}.unaryIntercept
	var appendStream grpc.StreamServerInterceptor = mockStreamInterceptor{val: errors.New("3")}.streamIntercept
	defaultMiddlewareFunc := func() ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
		return replacedUnary, replacedStream
	}
	c = ConfigWithOptions(&Config{},
		SetPrependUnaryMiddleware([]grpc.UnaryServerInterceptor{prependUnary}),
		SetPrependStreamingMiddleware([]grpc.StreamServerInterceptor{prependStream}),
		SetAppendUnaryMiddleware([]grpc.UnaryServerInterceptor{appendUnary}),
		SetAppendStreamingMiddleware([]grpc.StreamServerInterceptor{appendStream}),
	)

	// testing function equality is not possible, so we test the results of executing the functions

	outUnary, outStream = c.buildMiddleware(defaultMiddlewareFunc)
	require.Len(t, outUnary, 3)
	require.Len(t, outStream, 3)
	expectedPrepend, _ := prependUnary(context.Background(), nil, nil, nil)
	receivedPrepend, _ := outUnary[0](context.Background(), nil, nil, nil)
	require.Equal(t, expectedPrepend, receivedPrepend)
	expectedReplace, _ := replaceUnary(context.Background(), nil, nil, nil)
	receivedReplace, _ := outUnary[1](context.Background(), nil, nil, nil)
	require.Equal(t, expectedReplace, receivedReplace)
	expectedAppend, _ := appendUnary(context.Background(), nil, nil, nil)
	receivedAppend, _ := outUnary[2](context.Background(), nil, nil, nil)
	require.Equal(t, expectedAppend, receivedAppend)
	require.NotEqual(t, receivedPrepend, receivedReplace)
	require.NotEqual(t, receivedReplace, receivedAppend)
	require.NotEqual(t, receivedPrepend, receivedAppend)

	expectedPrepend = prependStream(nil, nil, nil, nil)
	receivedPrepend = outStream[0](nil, nil, nil, nil)
	require.Equal(t, expectedPrepend, receivedPrepend)
	expectedReplace = replaceStream(nil, nil, nil, nil)
	receivedReplace = outStream[1](nil, nil, nil, nil)
	require.Equal(t, expectedReplace, receivedReplace)
	expectedAppend = appendStream(nil, nil, nil, nil)
	receivedAppend = outStream[2](nil, nil, nil, nil)
	require.Equal(t, expectedAppend, receivedAppend)
	require.NotEqual(t, receivedPrepend, receivedReplace)
	require.NotEqual(t, receivedReplace, receivedAppend)
	require.NotEqual(t, receivedPrepend, receivedAppend)
}

type mockUnaryInterceptor struct {
	val int
}

func (m mockUnaryInterceptor) unaryIntercept(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	return m.val, nil
}

type mockStreamInterceptor struct {
	val error
}

func (m mockStreamInterceptor) streamIntercept(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return m.val
}
