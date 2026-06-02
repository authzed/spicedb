package testserver

import (
	"context"

	"google.golang.org/grpc/test/bufconn"
)

// TestListeners holds handles to the in-memory listeners created when a test server is
// configured with BufferedNetwork. Use these to construct gRPC client connections in tests.
type TestListeners struct {
	GRPC         *bufconn.Listener // nil if the gRPC server is not in BufferedNetwork mode
	ReadOnlyGRPC *bufconn.Listener // nil if the read-only gRPC server is not in BufferedNetwork mode
}

// CompleteForTesting completes test server configuration and additionally returns TestListeners
// for constructing client connections in tests.
func (c *Config) CompleteForTesting(ctx context.Context) (RunnableTestServer, TestListeners, error) {
	s, err := c.complete(ctx)
	if err != nil {
		return nil, TestListeners{}, err
	}
	return s, TestListeners{
		GRPC:         s.gRPCServer.BufferedListener(),
		ReadOnlyGRPC: s.readOnlyGRPCServer.BufferedListener(),
	}, nil
}
