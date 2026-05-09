package authzed

import (
	"github.com/jzelinskie/stringz"
	"google.golang.org/grpc"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// Client represents an open connection to Authzed.
//
// Clients are backed by a gRPC client and as such are thread-safe.
type Client struct {
	// Provide a handle on the underlying connection to enable cleanup
	// behaviors (among others)
	conn *grpc.ClientConn
	v1.SchemaServiceClient
	v1.PermissionsServiceClient
	v1.WatchServiceClient
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// ClientWithExperimental represents and open connection to Authzed with
// experimental services available.
//
// Clients are backed by a gRPC client and as such are thread-safe.
type ClientWithExperimental struct {
	Client

	v1.ExperimentalServiceClient
}

// NewClient initializes a brand new client for interacting with Authzed.
func NewClient(endpoint string, opts ...grpc.DialOption) (*Client, error) {
	conn, err := newConn(endpoint, opts...)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn,
		v1.NewSchemaServiceClient(conn),
		v1.NewPermissionsServiceClient(conn),
		v1.NewWatchServiceClient(conn),
	}, nil
}

// NewClientWithExperimentalAPIs initializes a brand new client for interacting
// with Authzed.
func NewClientWithExperimentalAPIs(endpoint string, opts ...grpc.DialOption) (*ClientWithExperimental, error) {
	conn, err := newConn(endpoint, opts...)
	if err != nil {
		return nil, err
	}

	return &ClientWithExperimental{
		Client{
			conn,
			v1.NewSchemaServiceClient(conn),
			v1.NewPermissionsServiceClient(conn),
			v1.NewWatchServiceClient(conn),
		},
		v1.NewExperimentalServiceClient(conn),
	}, nil
}

func newConn(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		stringz.DefaultEmpty(endpoint, "grpc.authzed.com:443"),
		opts...,
	)
}
