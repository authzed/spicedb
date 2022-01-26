package v1

import (
	"context"
	"net"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v0svc "github.com/authzed/spicedb/internal/services/v0"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSchemaWriteNoPrefix(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	// test relies on middleware
	lis := bufconn.Listen(1024 * 1024)
	s := tf.NewTestServer(ds)
	v1.RegisterSchemaServiceServer(s, NewSchemaServer(ds))

	go func() {
		if err := s.Serve(lis); err != nil {
			panic("failed to shutdown cleanly: " + err.Error())
		}
	}()
	defer func() {
		s.Stop()
		lis.Close()
	}()
	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := v1.NewSchemaServiceClient(conn)
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	require.NoError(t, err)
}

func TestSchemaWriteInvalidSchema(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	// test relies on middleware
	lis := bufconn.Listen(1024 * 1024)
	s := tf.NewTestServer(ds)
	v1.RegisterSchemaServiceServer(s, NewSchemaServer(ds))

	go func() {
		if err := s.Serve(lis); err != nil {
			panic("failed to shutdown cleanly: " + err.Error())
		}
	}()
	defer func() {
		s.Stop()
		lis.Close()
	}()
	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := v1.NewSchemaServiceClient(conn)
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `invalid example/user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	_, err = client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

func TestSchemaWriteAndReadBack(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	// test relies on middleware
	lis := bufconn.Listen(1024 * 1024)
	s := tf.NewTestServer(ds)
	v1.RegisterSchemaServiceServer(s, NewSchemaServer(ds))

	go func() {
		if err := s.Serve(lis); err != nil {
			panic("failed to shutdown cleanly: " + err.Error())
		}
	}()
	defer func() {
		s.Stop()
		lis.Close()
	}()
	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := v1.NewSchemaServiceClient(conn)

	_, err = client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	grpcutil.RequireStatus(t, codes.NotFound, err)

	userSchema := `definition example/user {}`

	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: userSchema,
	})
	require.NoError(t, err)

	readback, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	require.NoError(t, err)
	require.Equal(t, userSchema, readback.SchemaText)
}

func TestSchemaDeleteRelation(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	ns, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, nil)
	require.NoError(t, err)

	dispatch := graph.NewLocalOnlyDispatcher(ns, ds)

	// test relies on middleware
	lis := bufconn.Listen(1024 * 1024)
	s := tf.NewTestServer(ds)
	v1.RegisterSchemaServiceServer(s, NewSchemaServer(ds))
	v0.RegisterACLServiceServer(s, v0svc.NewACLServer(ds, ns, dispatch, 50))

	go func() {
		if err := s.Serve(lis); err != nil {
			panic("failed to shutdown cleanly: " + err.Error())
		}
	}()
	defer func() {
		s.Stop()
		lis.Close()
	}()
	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := v1.NewSchemaServiceClient(conn)
	v0client := v0.NewACLServiceClient(conn)

	// Write a basic schema.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			relation anotherrelation: example/user
		}`,
	})
	require.NoError(t, err)

	// Write a relationship for one of the relations.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Create(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		)},
	})
	require.Nil(t, err)

	// Attempt to delete the `somerelation` relation, which should fail.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation anotherrelation: example/user
		}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	// Attempt to delete the `anotherrelation` relation, which should succeed.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
		}`,
	})
	require.Nil(t, err)

	// Delete the relationship.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Delete(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		)},
	})
	require.Nil(t, err)

	// Attempt to delete the `somerelation` relation, which should succeed.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
		
			definition example/document {}`,
	})
	require.Nil(t, err)
}

func TestSchemaDeleteDefinition(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	ns, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, nil)
	require.NoError(t, err)

	dispatch := graph.NewLocalOnlyDispatcher(ns, ds)

	// test relies on middleware
	lis := bufconn.Listen(1024 * 1024)
	s := tf.NewTestServer(ds)
	v1.RegisterSchemaServiceServer(s, NewSchemaServer(ds))
	v0.RegisterACLServiceServer(s, v0svc.NewACLServer(ds, ns, dispatch, 50))

	go func() {
		if err := s.Serve(lis); err != nil {
			panic("failed to shutdown cleanly: " + err.Error())
		}
	}()
	defer func() {
		s.Stop()
		lis.Close()
	}()
	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := v1.NewSchemaServiceClient(conn)
	v0client := v0.NewACLServiceClient(conn)

	// Write a basic schema.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			relation anotherrelation: example/user
		}`,
	})
	require.NoError(t, err)

	// Write a relationship for one of the relations.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Create(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		)},
	})
	require.Nil(t, err)

	// Attempt to delete the `document` type, which should fail.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	// Delete the relationship.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Delete(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		)},
	})
	require.Nil(t, err)

	// Attempt to  delete the `document` type, which should succeed.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}`,
	})
	require.Nil(t, err)

	// Ensure it was deleted.
	readback, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	require.NoError(t, err)
	require.Equal(t, `definition example/user {}`, readback.SchemaText)
}
