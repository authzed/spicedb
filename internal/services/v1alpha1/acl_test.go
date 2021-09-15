package v1alpha1

import (
	"context"
	"testing"
	"time"

	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v0 "github.com/authzed/spicedb/internal/proto/authzed/api/v0"
	v1alpha1 "github.com/authzed/spicedb/internal/proto/authzed/api/v1alpha1"
	v0svc "github.com/authzed/spicedb/internal/services/v0"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestAttemptWriteRelationshipToPermission(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	// Write the schema.
	srv := NewSchemaServer(ds, PrefixRequired)
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}

		definition example/document {
			relation reader: example/user
			permission read = reader
		}
		`,
	})
	require.NoError(t, err)

	// Write a relationship to the relation.
	ns, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, nil)
	require.NoError(t, err)

	dispatch := graph.NewLocalOnlyDispatcher(ns, ds)
	aclSrv := v0svc.NewACLServer(ds, ns, dispatch, 50)

	_, err = aclSrv.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Create(
			tuple.Scan("example/document:somedoc#reader@example/user:someuser#..."),
		)},
	})
	require.NoError(t, err)

	// Attempt to write a relation to the permission, which should fail.
	_, err = aclSrv.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Create(
			tuple.Scan("example/document:somedoc#read@example/user:someuser#..."),
		)},
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}
