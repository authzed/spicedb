package newenemy

import (
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"google.golang.org/grpc"
)

type Client interface {
	V0() v0Client
	V1Alpha1() v1alpha1Client
	V1() v1Client
}

type v0Client interface {
	ACL() v0.ACLServiceClient
	Namespace() v0.NamespaceServiceClient
}

type v1alpha1Client interface {
	Schema() v1alpha1.SchemaServiceClient
}

type v1Client interface {
	Permissions() v1.PermissionsServiceClient
	Schema() v1.SchemaServiceClient
}

type spiceDBClient struct {
	v0Client       v0Client
	v1alpha1Client v1alpha1Client
	v1Client       v1Client
}

func (c *spiceDBClient) V0() v0Client {
	return c.v0Client
}

func (c *spiceDBClient) V1Alpha1() v1alpha1Client {
	return c.v1alpha1Client
}

func (c *spiceDBClient) V1() v1Client {
	return c.v1Client
}

type spiceDBv0Client struct {
	v0.ACLServiceClient
	v0.NamespaceServiceClient
}

func (s *spiceDBv0Client) ACL() v0.ACLServiceClient {
	return s
}

func (s *spiceDBv0Client) Namespace() v0.NamespaceServiceClient {
	return s
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

func NewClient(conn *grpc.ClientConn) Client {
	return &spiceDBClient{
		v0Client: &spiceDBv0Client{
			ACLServiceClient:       v0.NewACLServiceClient(conn),
			NamespaceServiceClient: v0.NewNamespaceServiceClient(conn),
		},
		v1alpha1Client: &spiceDBv1alpha1Client{
			SchemaServiceClient: v1alpha1.NewSchemaServiceClient(conn),
		},
		v1Client: &spiceDBv1Client{
			PermissionsServiceClient: v1.NewPermissionsServiceClient(conn),
			SchemaServiceClient:      v1.NewSchemaServiceClient(conn),
		},
	}
}
