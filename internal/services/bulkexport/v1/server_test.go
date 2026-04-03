package v1

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datastore"
	bulkexportv1 "github.com/authzed/spicedb/pkg/proto/bulkexport/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func newTestDatastore(t *testing.T) datastore.Datastore {
	t.Helper()
	ds, err := memdb.NewMemdbDatastore(0, 0, 1*time.Hour)
	require.NoError(t, err)
	return ds
}

func writeTestRelationships(t *testing.T, ds datastore.Datastore, count int) datastore.Revision {
	t.Helper()
	ctx := context.Background()
	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		var updates []tuple.RelationshipUpdate
		for i := 0; i < count; i++ {
			updates = append(updates, tuple.Create(
				tuple.MustParse(fmt.Sprintf("resource:%d#viewer@user:%d", i, i)),
			))
		}
		return rwt.WriteRelationships(ctx, updates)
	})
	require.NoError(t, err)
	return rev
}

// mockStream implements grpc.ServerStreamingServer for testing.
type mockStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*bulkexportv1.StreamBulkExportResponse
}

func (m *mockStream) Context() context.Context { return m.ctx }
func (m *mockStream) Send(resp *bulkexportv1.StreamBulkExportResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func TestPlanBulkExport(t *testing.T) {
	t.Run("returns single partition for non-partitioner datastore", func(t *testing.T) {
		ds := newTestDatastore(t)
		srv := NewBulkExportServer(ds)

		resp, err := srv.PlanBulkExport(context.Background(), &bulkexportv1.PlanBulkExportRequest{
			DesiredPartitions: 4,
		})
		require.NoError(t, err)
		require.Len(t, resp.Partitions, 1)
		require.Nil(t, resp.Partitions[0].LowerBound)
		require.Nil(t, resp.Partitions[0].UpperBound)
		require.NotEmpty(t, resp.Revision)
	})

	t.Run("desiredCount=0 defaults to 1", func(t *testing.T) {
		ds := newTestDatastore(t)
		srv := NewBulkExportServer(ds)

		resp, err := srv.PlanBulkExport(context.Background(), &bulkexportv1.PlanBulkExportRequest{
			DesiredPartitions: 0,
		})
		require.NoError(t, err)
		require.Len(t, resp.Partitions, 1)
	})

	t.Run("revision is valid and usable", func(t *testing.T) {
		ds := newTestDatastore(t)
		writeTestRelationships(t, ds, 10)
		srv := NewBulkExportServer(ds)

		resp, err := srv.PlanBulkExport(context.Background(), &bulkexportv1.PlanBulkExportRequest{
			DesiredPartitions: 1,
		})
		require.NoError(t, err)

		// The returned revision should be parseable and usable.
		rev, err := ds.RevisionFromString(resp.Revision)
		require.NoError(t, err)
		require.NoError(t, ds.CheckRevision(context.Background(), rev))
	})
}

func TestStreamBulkExport(t *testing.T) {
	t.Run("streams all relationships with no partition", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 50)
		srv := NewBulkExportServer(ds)

		stream := &mockStream{ctx: context.Background()}
		err := srv.StreamBulkExport(&bulkexportv1.StreamBulkExportRequest{
			Revision:  rev.String(),
			BatchSize: 100,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)
		require.Len(t, stream.responses[0].Relationships, 50)
	})

	t.Run("batches correctly with small batch size", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 50)
		srv := NewBulkExportServer(ds)

		stream := &mockStream{ctx: context.Background()}
		err := srv.StreamBulkExport(&bulkexportv1.StreamBulkExportRequest{
			Revision:  rev.String(),
			BatchSize: 10,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 5)
		for _, resp := range stream.responses {
			require.Len(t, resp.Relationships, 10)
			require.NotEmpty(t, resp.AfterResultCursor)
		}
	})

	t.Run("batch_size=0 uses default", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 10)
		srv := NewBulkExportServer(ds)

		stream := &mockStream{ctx: context.Background()}
		err := srv.StreamBulkExport(&bulkexportv1.StreamBulkExportRequest{
			Revision:  rev.String(),
			BatchSize: 0,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)
		require.Len(t, stream.responses[0].Relationships, 10)
	})

	t.Run("batch_size=1 sends one per response", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 5)
		srv := NewBulkExportServer(ds)

		stream := &mockStream{ctx: context.Background()}
		err := srv.StreamBulkExport(&bulkexportv1.StreamBulkExportRequest{
			Revision:  rev.String(),
			BatchSize: 1,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 5)
	})

	t.Run("batch larger than total rows", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 5)
		srv := NewBulkExportServer(ds)

		stream := &mockStream{ctx: context.Background()}
		err := srv.StreamBulkExport(&bulkexportv1.StreamBulkExportRequest{
			Revision:  rev.String(),
			BatchSize: 1000,
		}, stream)
		require.NoError(t, err)
		require.Len(t, stream.responses, 1)
		require.Len(t, stream.responses[0].Relationships, 5)
	})

	t.Run("empty table produces no responses", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev, err := ds.HeadRevision(context.Background())
		require.NoError(t, err)
		srv := NewBulkExportServer(ds)

		stream := &mockStream{ctx: context.Background()}
		err = srv.StreamBulkExport(&bulkexportv1.StreamBulkExportRequest{
			Revision:  rev.String(),
			BatchSize: 100,
		}, stream)
		require.NoError(t, err)
		require.Empty(t, stream.responses)
	})

	t.Run("cursor resumability", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 30)
		srv := NewBulkExportServer(ds)

		// First stream: get 10 rows.
		stream1 := &mockStream{ctx: context.Background()}
		err := srv.StreamBulkExport(&bulkexportv1.StreamBulkExportRequest{
			Revision:  rev.String(),
			BatchSize: 10,
		}, stream1)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(stream1.responses), 1)

		// Resume from the first batch's cursor.
		cursor := stream1.responses[0].AfterResultCursor

		stream2 := &mockStream{ctx: context.Background()}
		err = srv.StreamBulkExport(&bulkexportv1.StreamBulkExportRequest{
			Revision:  rev.String(),
			BatchSize: 100,
			Cursor:    &cursor,
		}, stream2)
		require.NoError(t, err)

		// Total should be 30: 10 from first batch + 20 from resumed stream.
		totalFromResume := 0
		for _, resp := range stream2.responses {
			totalFromResume += len(resp.Relationships)
		}
		require.Equal(t, 20, totalFromResume)
	})

	t.Run("invalid revision returns error", func(t *testing.T) {
		ds := newTestDatastore(t)
		srv := NewBulkExportServer(ds)

		stream := &mockStream{ctx: context.Background()}
		err := srv.StreamBulkExport(&bulkexportv1.StreamBulkExportRequest{
			Revision:  "invalid-revision",
			BatchSize: 100,
		}, stream)
		require.Error(t, err)
	})

	t.Run("empty revision returns error", func(t *testing.T) {
		ds := newTestDatastore(t)
		srv := NewBulkExportServer(ds)

		stream := &mockStream{ctx: context.Background()}
		err := srv.StreamBulkExport(&bulkexportv1.StreamBulkExportRequest{
			Revision:  "",
			BatchSize: 100,
		}, stream)
		require.Error(t, err)
	})

	t.Run("invalid cursor returns error", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 10)
		srv := NewBulkExportServer(ds)

		badCursor := "not-valid-base64!!!"
		stream := &mockStream{ctx: context.Background()}
		err := srv.StreamBulkExport(&bulkexportv1.StreamBulkExportRequest{
			Revision:  rev.String(),
			BatchSize: 100,
			Cursor:    &badCursor,
		}, stream)
		require.Error(t, err)
	})
}
