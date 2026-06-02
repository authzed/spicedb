package grpchelpers

import (
	"google.golang.org/grpc"
)

const waitForReadyConfig = `{"methodConfig": [{"name": [{}], "waitForReady": true}]}`

// Dial creates a new client connection to the target. RPCs will wait for the connection to be
// ready rather than failing immediately on transient connectivity issues.
func Dial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts = append([]grpc.DialOption{grpc.WithDefaultServiceConfig(waitForReadyConfig)}, opts...)
	return grpc.NewClient(target, opts...)
}
