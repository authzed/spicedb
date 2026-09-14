//go:build datastore

package integrationtesting_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TestDeleteRelationshipsCursorAPIAgainstCockroachDB exercises the cursored
// DeleteRelationships API end-to-end against a real CockroachDB: a batched,
// partial deletion resumes across calls using the after_result_cursor it hands
// back, deletes everything, and the resulting cursor is rejected by
// ReadRelationships (confirming the per-API cursor namespacing).
func TestDeleteRelationshipsCursorAPIAgainstCockroachDB(t *testing.T) {
	ctx := context.Background()

	b := testdatastore.RunDatastoreEngine(t, "cockroachdb")
	ds := b.NewDatastore(t, config.DatastoreConfigInitFunc(t,
		dsconfig.WithWatchBufferLength(0),
		dsconfig.WithGCWindow(90*time.Minute),
		dsconfig.WithRevisionQuantization(10),
		dsconfig.WithMaxRetries(50),
		dsconfig.WithWriteAcquisitionTimeout(5*time.Second),
	))

	conns := testserver.TestClusterWithDispatch(t, 1, ds)
	client := v1.NewPermissionsServiceClient(conns[0])
	schemaClient := v1.NewSchemaServiceClient(conns[0])

	_, err := schemaClient.WriteSchema(ctx, &v1.WriteSchemaRequest{
		Schema: "definition user {}\ndefinition resource {\n\trelation viewer: user\n}",
	})
	require.NoError(t, err)

	const total = 25
	updates := make([]*v1.RelationshipUpdate, 0, total)
	for i := range total {
		updates = append(updates, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: tuple.ToV1Relationship(tuple.MustParse(fmt.Sprintf("resource:res%02d#viewer@user:someuser", i))),
		})
	}
	_, err = client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{Updates: updates})
	require.NoError(t, err)

	filter := &v1.RelationshipFilter{ResourceType: "resource"}

	// Batched cursored deletion: follow after_result_cursor until complete.
	var resumeCursor *v1.Cursor
	var lastDeleteCursor *v1.Cursor
	deleted := uint64(0)
	sawCursor := false
	for i := 0; ; i++ {
		require.Less(t, i, 100, "cursored delete failed to terminate")

		resp, err := client.DeleteRelationships(ctx, &v1.DeleteRelationshipsRequest{
			RelationshipFilter:            filter,
			OptionalLimit:                 5,
			OptionalAllowPartialDeletions: true,
			OptionalCursor:                resumeCursor,
		})
		require.NoError(t, err)

		deleted += resp.RelationshipsDeletedCount

		if resp.DeletionProgress == v1.DeleteRelationshipsResponse_DELETION_PROGRESS_COMPLETE {
			break
		}

		require.Equal(t, v1.DeleteRelationshipsResponse_DELETION_PROGRESS_PARTIAL, resp.DeletionProgress)
		// CockroachDB supports cursored deletion, so a partial delete must return
		// a cursor to resume after.
		require.NotNil(t, resp.AfterResultCursor)
		sawCursor = true
		lastDeleteCursor = resp.AfterResultCursor
		resumeCursor = resp.AfterResultCursor
	}

	require.Equal(t, uint64(total), deleted)
	require.True(t, sawCursor, "expected at least one partial batch with a resume cursor")
	require.NotNil(t, lastDeleteCursor)

	fullyConsistent := &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}}

	// Everything should be gone.
	readStream, err := client.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
		Consistency:        fullyConsistent,
		RelationshipFilter: filter,
	})
	require.NoError(t, err)
	_, err = readStream.Recv()
	require.ErrorIs(t, err, io.EOF, "expected no relationships to remain")

	// The delete cursor must not be usable as a ReadRelationships cursor.
	rejectStream, err := client.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
		Consistency:        fullyConsistent,
		RelationshipFilter: filter,
		OptionalLimit:      1,
		OptionalCursor:     lastDeleteCursor,
	})
	require.NoError(t, err)
	_, err = rejectStream.Recv()
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a delete cursor must be rejected by read: %v", err)
}
