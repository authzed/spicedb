package dispatcher

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/test/bufconn"

	"github.com/authzed/spicedb/internal/dispatch"
	dispatchermw "github.com/authzed/spicedb/internal/middleware/dispatcher"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

const bufSize = 1024 * 1024

// setupBuffconn creates a buffered connection for testing
func setupBuffconn() (*bufconn.Listener, func(context.Context, string) (net.Conn, error)) {
	lis := bufconn.Listen(bufSize)
	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}
	return lis, dialer
}

// fakeDispatcher implements the dispatch.Dispatcher interface for testing
type fakeDispatcher struct{}

func (m *fakeDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	return &v1.DispatchCheckResponse{}, nil
}

func (m *fakeDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return &v1.DispatchExpandResponse{}, nil
}

func (m *fakeDispatcher) DispatchLookupResources2(req *v1.DispatchLookupResources2Request, stream dispatch.LookupResources2Stream) error {
	return nil
}

func (m *fakeDispatcher) DispatchLookupResources3(req *v1.DispatchLookupResources3Request, stream dispatch.LookupResources3Stream) error {
	return nil
}

func (m *fakeDispatcher) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	return nil
}

func (m *fakeDispatcher) Close() error {
	return nil
}

func (m *fakeDispatcher) ReadyState() dispatch.ReadyState {
	return dispatch.ReadyState{}
}

// testService implements the grpc_testing.TestServiceServer interface
type testService struct {
	grpc_testing.UnimplementedTestServiceServer
	unaryDispatcher     dispatch.Dispatcher
	streamingDispatcher dispatch.Dispatcher
}

func (s *testService) UnaryCall(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	s.unaryDispatcher = FromContext(ctx)
	return &grpc_testing.SimpleResponse{}, nil
}

func (s *testService) StreamingInputCall(stream grpc_testing.TestService_StreamingInputCallServer) error {
	ctx := stream.Context()
	s.streamingDispatcher = FromContext(ctx)

	// Read messages until the client closes the stream
	for {
		_, err := stream.Recv()
		if err != nil {
			// Send response when stream is done
			return stream.SendAndClose(&grpc_testing.StreamingInputCallResponse{})
		}
	}
}

// mustUnaryTestService implements grpc_testing.TestServiceServer and uses MustFromContext
type mustUnaryTestService struct {
	grpc_testing.UnimplementedTestServiceServer
	dispatcher dispatch.Dispatcher
}

func (s *mustUnaryTestService) UnaryCall(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	s.dispatcher = MustFromContext(ctx)
	return &grpc_testing.SimpleResponse{}, nil
}

// mustStreamTestService implements grpc_testing.TestServiceServer and uses MustFromContext in streaming
type mustStreamTestService struct {
	grpc_testing.UnimplementedTestServiceServer
	dispatcher dispatch.Dispatcher
}

func (s *mustStreamTestService) StreamingInputCall(stream grpc_testing.TestService_StreamingInputCallServer) error {
	ctx := stream.Context()
	s.dispatcher = MustFromContext(ctx)

	// Read messages until the client closes the stream
	for {
		_, err := stream.Recv()
		if err != nil {
			// Send response when stream is done
			return stream.SendAndClose(&grpc_testing.StreamingInputCallResponse{})
		}
	}
}

func TestFromContext(t *testing.T) {
	fakeDisp := &fakeDispatcher{}

	// Test with dispatcher in context
	t.Run("with dispatcher", func(t *testing.T) {
		ctx := dispatchermw.ContextWithHandle(t.Context())
		require.NoError(t, dispatchermw.SetInContext(ctx, fakeDisp))

		result := FromContext(ctx)
		require.NotNil(t, result)
		require.Equal(t, fakeDisp, result)
	})

	// Test without dispatcher in context
	t.Run("without dispatcher", func(t *testing.T) {
		ctx := t.Context()
		result := FromContext(ctx)
		require.Nil(t, result)
	})
}

func TestMustFromContext(t *testing.T) {
	fakeDisp := &fakeDispatcher{}

	// Test with dispatcher in context
	t.Run("with dispatcher", func(t *testing.T) {
		ctx := dispatchermw.ContextWithHandle(t.Context())
		require.NoError(t, dispatchermw.SetInContext(ctx, fakeDisp))

		result := MustFromContext(ctx)
		require.NotNil(t, result)
		require.Equal(t, fakeDisp, result)
	})

	// Test without dispatcher in context (should panic)
	t.Run("without dispatcher panics", func(t *testing.T) {
		ctx := t.Context()
		require.Panics(t, func() {
			MustFromContext(ctx)
		})
	})
}

func TestFromContextInUnaryGRPC(t *testing.T) {
	// Create a fake dispatcher
	fakeDisp := &fakeDispatcher{}

	// Set up buffered connection and gRPC server
	lis, dialer := setupBuffconn()
	defer lis.Close()

	server := grpc.NewServer(
		grpc.UnaryInterceptor(dispatchermw.UnaryServerInterceptor(fakeDisp)),
	)

	testSvc := &testService{}
	grpc_testing.RegisterTestServiceServer(server, testSvc)

	go func() {
		_ = server.Serve(lis)
	}()
	defer server.Stop()

	// Set up client with buffered connection
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := grpc_testing.NewTestServiceClient(conn)

	// Make unary call
	_, err = client.UnaryCall(t.Context(), &grpc_testing.SimpleRequest{})
	require.NoError(t, err)

	// Verify dispatcher was available in the service method
	require.NotNil(t, testSvc.unaryDispatcher)
	require.Equal(t, fakeDisp, testSvc.unaryDispatcher)
}

func TestMustFromContextInUnaryGRPC(t *testing.T) {
	// Create a fake dispatcher
	fakeDisp := &fakeDispatcher{}

	// Set up buffered connection and gRPC server
	lis, dialer := setupBuffconn()
	defer lis.Close()

	server := grpc.NewServer(
		grpc.UnaryInterceptor(dispatchermw.UnaryServerInterceptor(fakeDisp)),
	)

	// Test service that uses MustFromContext
	testSvc := &mustUnaryTestService{}

	grpc_testing.RegisterTestServiceServer(server, testSvc)

	go func() {
		_ = server.Serve(lis)
	}()
	defer server.Stop()

	// Set up client with buffered connection
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := grpc_testing.NewTestServiceClient(conn)

	// Make unary call
	_, err = client.UnaryCall(t.Context(), &grpc_testing.SimpleRequest{})
	require.NoError(t, err)

	// Verify dispatcher was available via MustFromContext
	require.NotNil(t, testSvc.dispatcher)
	require.Equal(t, fakeDisp, testSvc.dispatcher)
}

func TestFromContextInStreamingGRPC(t *testing.T) {
	// Create a fake dispatcher
	fakeDisp := &fakeDispatcher{}

	// Set up buffered connection and gRPC server with stream middleware
	lis, dialer := setupBuffconn()
	defer lis.Close()

	server := grpc.NewServer(
		grpc.StreamInterceptor(dispatchermw.StreamServerInterceptor(fakeDisp)),
	)

	testSvc := &testService{}
	grpc_testing.RegisterTestServiceServer(server, testSvc)

	go func() {
		_ = server.Serve(lis)
	}()
	defer server.Stop()

	// Set up client with buffered connection
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := grpc_testing.NewTestServiceClient(conn)

	// Make streaming call
	stream, err := client.StreamingInputCall(t.Context())
	require.NoError(t, err)

	// Send at least one request before closing
	err = stream.Send(&grpc_testing.StreamingInputCallRequest{})
	require.NoError(t, err)

	err = stream.CloseSend()
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	// Verify dispatcher was available in the streaming service method
	require.NotNil(t, testSvc.streamingDispatcher)
	require.Equal(t, fakeDisp, testSvc.streamingDispatcher)
}

func TestMustFromContextInStreamingGRPC(t *testing.T) {
	// Create a fake dispatcher
	fakeDisp := &fakeDispatcher{}

	// Set up buffered connection and gRPC server with stream middleware
	lis, dialer := setupBuffconn()
	defer lis.Close()

	server := grpc.NewServer(
		grpc.StreamInterceptor(dispatchermw.StreamServerInterceptor(fakeDisp)),
	)

	// Test service that uses MustFromContext in streaming
	testSvc := &mustStreamTestService{}

	grpc_testing.RegisterTestServiceServer(server, testSvc)

	go func() {
		_ = server.Serve(lis)
	}()
	defer server.Stop()

	// Set up client with buffered connection
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := grpc_testing.NewTestServiceClient(conn)

	// Make streaming call
	stream, err := client.StreamingInputCall(t.Context())
	require.NoError(t, err)

	// Send at least one request before closing
	err = stream.Send(&grpc_testing.StreamingInputCallRequest{})
	require.NoError(t, err)

	err = stream.CloseSend()
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	// Verify dispatcher was available via MustFromContext in streaming
	require.NotNil(t, testSvc.dispatcher)
	require.Equal(t, fakeDisp, testSvc.dispatcher)
}
