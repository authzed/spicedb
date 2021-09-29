package v1

import (
	"context"
	"net"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/namespace"
	tf "github.com/authzed/spicedb/internal/testfixtures"
)

// RunForTesting runs a real gRPC instance of the V1 API and returns a client for testing.
func RunForTesting(t *testing.T, ds datastore.Datastore, nsm namespace.Manager, dispatch dispatch.Dispatcher, defaultDepth uint32) (v1.PermissionsServiceClient, v1.SchemaServiceClient) {
	lis := bufconn.Listen(1024 * 1024)
	s := tf.NewTestServer()
	v1.RegisterPermissionsServiceServer(s, NewPermissionsServer(ds, nsm, dispatch, 50))
	v1.RegisterSchemaServiceServer(s, NewSchemaServer(ds))

	go func() {
		if err := s.Serve(lis); err != nil {
			panic("failed to shutdown cleanly: " + err.Error())
		}
	}()
	t.Cleanup(func() {
		s.Stop()
		lis.Close()
	})

	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
	require.NoError(t, err)

	return v1.NewPermissionsServiceClient(conn), v1.NewSchemaServiceClient(conn)
}
