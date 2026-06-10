package grpchelpers

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const waitForReadyConfig = `{"methodConfig": [{"name": [{}], "waitForReady": true}]}`

// NewBufferedClient creates a client connection to an in-memory bufconn listener. Insecure
// transport credentials are used by default; pass grpc.WithTransportCredentials to override.
// RPCs will wait for the connection to be ready, handling the startup race between server
// and client in tests. Additional opts are applied after the defaults, so caller-provided
// options take precedence.
func NewBufferedClient(listener *bufconn.Listener, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts = append([]grpc.DialOption{
		grpc.WithDefaultServiceConfig(waitForReadyConfig),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// This identifies the client to the server as being over the buffered network
		grpc.WithAuthority("buffnet"),
	}, opts...)
	return grpc.NewClient("passthrough:///localhost", opts...)
}
