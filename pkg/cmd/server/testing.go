package server

import (
	"context"

	"google.golang.org/grpc/test/bufconn"
)

// TestListeners holds handles to the in-memory listeners created when a server is configured
// with BufferedNetwork. Use these to construct gRPC client connections in tests without
// any dial methods on the server interface itself.
type TestListeners struct {
	GRPC     *bufconn.Listener // nil if the gRPC server is not in BufferedNetwork mode
	Dispatch *bufconn.Listener // nil if the dispatch server is not in BufferedNetwork mode
}

// CompleteForTesting completes server configuration and additionally returns TestListeners
// for constructing client connections in tests.
// This is currently used only by `internal/testserver/cluster.go`; all other test code
// uses `NewClient` on the `RunnableServer` to get a handle.
func (c *Config) CompleteForTesting(ctx context.Context) (RunnableServer, TestListeners, error) {
	s, err := c.complete(ctx)
	if err != nil {
		return nil, TestListeners{}, err
	}
	return s, TestListeners{
		GRPC:     s.gRPCServer.BufferedListener(),
		Dispatch: s.dispatchGRPCServer.BufferedListener(),
	}, nil
}
