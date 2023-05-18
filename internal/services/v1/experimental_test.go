package v1_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
)

func TestBulkImportRelationships(t *testing.T) {
	testCases := []struct {
		name       string
		batchSize  func() int
		numBatches int
	}{
		{"one small batch", constBatch(1), 1},
		{"one big batch", constBatch(10_000), 1},
		{"many small batches", constBatch(5), 1_000},
		{"one empty batch", constBatch(0), 1},
		{"small random batches", randomBatch(1, 10), 100},
		{"big random batches", randomBatch(1_000, 3_000), 50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithSchema)
			client := v1.NewExperimentalServiceClient(conn)
			t.Cleanup(cleanup)

			ctx := context.Background()

			writer, err := client.BulkImportRelationships(ctx)
			require.NoError(err)

			var expectedTotal uint64
			for batchNum := 0; batchNum < tc.numBatches; batchNum++ {
				batchSize := tc.batchSize()
				batch := make([]*v1.Relationship, 0, batchSize)

				for i := 0; i < batchSize; i++ {
					batch = append(batch, rel(
						tf.DocumentNS.Name,
						strconv.Itoa(batchNum)+"_"+strconv.Itoa(i),
						"viewer",
						tf.UserNS.Name,
						strconv.Itoa(i),
						"",
					))
				}

				err := writer.Send(&v1.BulkImportRelationshipsRequest{
					Relationships: batch,
				})
				require.NoError(err)

				expectedTotal += uint64(batchSize)
			}

			resp, err := writer.CloseAndRecv()
			require.NoError(err)
			require.Equal(expectedTotal, resp.NumLoaded)

			readerClient := v1.NewPermissionsServiceClient(conn)
			stream, err := readerClient.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType: tf.DocumentNS.Name,
				},
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
			})
			require.NoError(err)

			var readBack uint64
			for _, err = stream.Recv(); err == nil; _, err = stream.Recv() {
				readBack++
			}
			require.ErrorIs(err, io.EOF)
			require.Equal(expectedTotal, readBack)
		})
	}
}

func constBatch(size int) func() int {
	return func() int {
		return size
	}
}

func randomBatch(min, max int) func() int {
	return func() int {
		return rand.Intn(max-min) + min
	}
}

func TestBulkExportRelationships(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.StandardDatastoreWithSchema)
	client := v1.NewExperimentalServiceClient(conn)
	t.Cleanup(cleanup)

	nsAndRels := []struct {
		namespace string
		relation  string
	}{
		{tf.DocumentNS.Name, "viewer"},
		{tf.FolderNS.Name, "viewer"},
		{tf.DocumentNS.Name, "owner"},
		{tf.FolderNS.Name, "owner"},
		{tf.DocumentNS.Name, "editor"},
		{tf.FolderNS.Name, "editor"},
	}

	totalToWrite := uint64(1_000)
	batch := make([]*v1.Relationship, totalToWrite)
	for i := range batch {
		nsAndRel := nsAndRels[i%len(nsAndRels)]
		batch[i] = rel(
			nsAndRel.namespace,
			strconv.Itoa(i),
			nsAndRel.relation,
			tf.UserNS.Name,
			strconv.Itoa(i),
			"",
		)
	}

	ctx := context.Background()
	writer, err := client.BulkImportRelationships(ctx)
	require.NoError(t, err)

	require.NoError(t, writer.Send(&v1.BulkImportRelationshipsRequest{
		Relationships: batch,
	}))

	resp, err := writer.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, totalToWrite, resp.NumLoaded)

	testCases := []struct {
		batchSize      uint32
		paginateEveryN int
	}{
		{1_000, math.MaxInt},
		{10, math.MaxInt},
		{1_000, 1},
		{100, 5},
		{97, 7},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d-%d", tc.batchSize, tc.paginateEveryN), func(t *testing.T) {
			require := require.New(t)

			var totalRead uint64
			var cursor *v1.Cursor

			var done bool
			for !done {
				streamCtx, cancel := context.WithCancel(ctx)

				stream, err := client.BulkExportRelationships(streamCtx, &v1.BulkExportRelationshipsRequest{
					OptionalLimit:  tc.batchSize,
					OptionalCursor: cursor,
				})
				require.NoError(err)

				for i := 0; i < tc.paginateEveryN; i++ {
					batch, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						done = true
						break
					}

					require.NoError(err)
					require.LessOrEqual(uint32(len(batch.Relationships)), tc.batchSize)
					require.NotNil(batch.AfterResultCursor)
					require.NotEmpty(batch.AfterResultCursor.Token)

					cursor = batch.AfterResultCursor
					totalRead += uint64(len(batch.Relationships))
				}

				cancel()
			}

			require.Equal(totalToWrite, totalRead)
		})
	}
}
