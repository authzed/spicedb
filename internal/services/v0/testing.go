package v0

import (
	"context"
	"net"
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/namespace"
	tf "github.com/authzed/spicedb/internal/testfixtures"
)

// RunForTesting runs a real gRPC instance of the V1 API and returns a client for testing.
func RunForTesting(t *testing.T, ds datastore.Datastore, nsm namespace.Manager, dispatch dispatch.Dispatcher, defaultDepth uint32) (v0.ACLServiceClient, v0.NamespaceServiceClient) {
	lis := bufconn.Listen(1024 * 1024)
	s := tf.NewTestServer(ds)
	v0.RegisterACLServiceServer(s, NewACLServer(ds, nsm, dispatch, defaultDepth))
	v0.RegisterNamespaceServiceServer(s, NewNamespaceServer(ds))

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
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	return v0.NewACLServiceClient(conn), v0.NewNamespaceServiceClient(conn)
}
