//go:build ci && docker

package crdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	partitionedexportv1 "github.com/authzed/spicedb/internal/services/partitionedexport/v1"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	pev1 "github.com/authzed/spicedb/pkg/proto/partitionedexport/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPartitionedExportEndToEnd(t *testing.T) {
	b := testdatastore.RunCRDBClusterForTesting(t, 3, crdbTestVersion())

	t.Run("Plan + Stream covers all relationships", func(t *testing.T) {
		ctx := context.Background()

		var connectStr string
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			connectStr = uri
			ds, err := NewCRDBDatastore(ctx, uri,
				GCWindow(veryLargeGCWindow),
				RevisionQuantization(0),
				WithAcquireTimeout(30*time.Second),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ds.Close() })
			return ds
		})

		// Write relationships across multiple namespaces.
		totalWritten := 0
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			var updates []tuple.RelationshipUpdate
			for ns := 0; ns < 10; ns++ {
				for i := 0; i < 100; i++ {
					updates = append(updates, tuple.Create(tuple.MustParse(
						fmt.Sprintf("resource%d:%d#viewer@user:%d", ns, i, i),
					)))
					totalWritten++
				}
			}
			return rwt.WriteRelationships(ctx, updates)
		})
		require.NoError(t, err)

		// Force range splits so we get real multi-partition plans.
		conn, err := pgx.Connect(ctx, connectStr)
		require.NoError(t, err)
		defer conn.Close(ctx)

		for _, ns := range []string{"resource3", "resource5", "resource7"} {
			_, err := conn.Exec(ctx, fmt.Sprintf(
				`ALTER TABLE relation_tuple SPLIT AT VALUES ('%s', '0', 'viewer', 'user', '0', '...')`, ns,
			))
			require.NoError(t, err)
		}

		// Phase 1: Plan via the service API.
		srv := partitionedexportv1.NewPartitionedExportServer(ds)
		planResp, err := srv.PlanPartitionedExport(ctx, &pev1.PlanPartitionedExportRequest{
			DesiredPartitions: 4,
		})
		require.NoError(t, err)
		require.Greater(t, len(planResp.Partitions), 1, "expected multiple partitions from forced splits")
		require.NotEmpty(t, planResp.Revision)

		t.Logf("Plan returned %d partitions at revision %s", len(planResp.Partitions), planResp.Revision)

		// Phase 2: Stream each partition via the service API and collect all relationships.
		seen := make(map[string]int) // relationship key → partition index
		for pi, partition := range planResp.Partitions {
			stream := &mockStreamCollector{ctx: ctx}
			err := srv.StreamPartitionedExport(&pev1.StreamPartitionedExportRequest{
				Partition: partition,
				Revision:  planResp.Revision,
				BatchSize: 1000,
			}, stream)
			require.NoError(t, err)

			partitionCount := 0
			for _, resp := range stream.responses {
				require.NotEmpty(t, resp.AfterResultCursor, "response should have a cursor")
				for _, rel := range resp.Relationships {
					key := fmt.Sprintf("%s:%s#%s@%s:%s#%s",
						rel.ResourceType, rel.ResourceId, rel.Relation,
						rel.SubjectType, rel.SubjectId, rel.SubjectRelation)
					prevPartition, duplicate := seen[key]
					require.False(t, duplicate,
						"relationship %s appeared in both partition %d and %d", key, prevPartition, pi)
					seen[key] = pi
					partitionCount++
				}
			}
			t.Logf("Partition %d: %d relationships", pi, partitionCount)
		}

		require.Equal(t, totalWritten, len(seen),
			"all relationships should be covered across partitions (no gaps)")
	})

	t.Run("cursor resumability through service API", func(t *testing.T) {
		ctx := context.Background()

		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewCRDBDatastore(ctx, uri,
				GCWindow(veryLargeGCWindow),
				RevisionQuantization(0),
				WithAcquireTimeout(30*time.Second),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ds.Close() })
			return ds
		})

		// Write relationships.
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			var updates []tuple.RelationshipUpdate
			for i := 0; i < 50; i++ {
				updates = append(updates, tuple.Create(tuple.MustParse(
					fmt.Sprintf("resource:%d#viewer@user:%d", i, i),
				)))
			}
			return rwt.WriteRelationships(ctx, updates)
		})
		require.NoError(t, err)

		srv := partitionedexportv1.NewPartitionedExportServer(ds)
		planResp, err := srv.PlanPartitionedExport(ctx, &pev1.PlanPartitionedExportRequest{
			DesiredPartitions: 1,
		})
		require.NoError(t, err)
		require.Len(t, planResp.Partitions, 1)

		// Stream first 10 rows.
		stream1 := &mockStreamCollector{ctx: ctx}
		err = srv.StreamPartitionedExport(&pev1.StreamPartitionedExportRequest{
			Partition: planResp.Partitions[0],
			Revision:  planResp.Revision,
			BatchSize: 10,
		}, stream1)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(stream1.responses), 1)

		// The first response contains batch_size (10) rows.
		require.GreaterOrEqual(t, len(stream1.responses), 1)
		firstBatchCount := len(stream1.responses[0].Relationships)
		require.Equal(t, 10, firstBatchCount)

		// Resume from the first batch's cursor — should get the remaining 40.
		cursorToken := stream1.responses[0].AfterResultCursor
		stream2 := &mockStreamCollector{ctx: ctx}
		err = srv.StreamPartitionedExport(&pev1.StreamPartitionedExportRequest{
			Partition: planResp.Partitions[0],
			Revision:  planResp.Revision,
			BatchSize: 1000,
			Cursor:    &cursorToken,
		}, stream2)
		require.NoError(t, err)

		resumedCount := 0
		for _, resp := range stream2.responses {
			resumedCount += len(resp.Relationships)
		}

		require.Equal(t, 40, resumedCount)
		require.Equal(t, 50, firstBatchCount+resumedCount, "resume should cover all remaining rows")
	})
}

// mockStreamCollector implements grpc.ServerStreamingServer for testing.
type mockStreamCollector struct {
	pev1.PartitionedExportService_StreamPartitionedExportServer
	ctx       context.Context
	responses []*pev1.StreamPartitionedExportResponse
}

func (m *mockStreamCollector) Context() context.Context { return m.ctx }
func (m *mockStreamCollector) Send(resp *pev1.StreamPartitionedExportResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}
