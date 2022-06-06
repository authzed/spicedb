package e2e

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"google.golang.org/grpc"
)

// Client holds versioned clients to spicedb that all share the same connection
type Client interface {
	V1Alpha1() v1alpha1Client
	V1() v1Client
}

type v1alpha1Client interface {
	Schema() v1alpha1.SchemaServiceClient
}

type v1Client interface {
	Permissions() v1.PermissionsServiceClient
	Schema() v1.SchemaServiceClient
}

type spiceDBClient struct {
	v1alpha1Client v1alpha1Client
	v1Client       v1Client
}

func (c *spiceDBClient) V1Alpha1() v1alpha1Client {
	return c.v1alpha1Client
}

func (c *spiceDBClient) V1() v1Client {
	return c.v1Client
}

type spiceDBv1alpha1Client struct {
	v1alpha1.SchemaServiceClient
}

func (s *spiceDBv1alpha1Client) Schema() v1alpha1.SchemaServiceClient {
	return s
}

type spiceDBv1Client struct {
	v1.PermissionsServiceClient
	v1.SchemaServiceClient
}

func (s *spiceDBv1Client) Permissions() v1.PermissionsServiceClient {
	return s
}

func (s *spiceDBv1Client) Schema() v1.SchemaServiceClient {
	return s
}

// NewClient returns a spicedb Client for the given grpc connection
func NewClient(conn *grpc.ClientConn) Client {
	return &spiceDBClient{
		v1alpha1Client: &spiceDBv1alpha1Client{
			SchemaServiceClient: v1alpha1.NewSchemaServiceClient(conn),
		},
		v1Client: &spiceDBv1Client{
			PermissionsServiceClient: v1.NewPermissionsServiceClient(conn),
			SchemaServiceClient:      v1.NewSchemaServiceClient(conn),
		},
	}
}
