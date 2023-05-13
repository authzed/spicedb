package v1_test

import (
	"context"
	"io"
	"math/rand"
	"strconv"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
)

func TestBulkLoadRelationships(t *testing.T) {
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

			writer, err := client.BulkLoadRelationships(ctx)
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

				err := writer.Send(&v1.BulkLoadRelationshipsRequest{
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
