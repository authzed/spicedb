package v1

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPlanPartitionedExport(t *testing.T) {
	t.Run("returns error for non-partitioner datastore", func(t *testing.T) {
		ds := newTestDatastore(t)
		_, err := PlanPartitionedExport(t.Context(), ds, 4)
		require.ErrorIs(t, err, ErrPartitioningNotSupported)
	})

	t.Run("desiredCount=0 returns error for non-partitioner", func(t *testing.T) {
		ds := newTestDatastore(t)
		_, err := PlanPartitionedExport(t.Context(), ds, 0)
		require.ErrorIs(t, err, ErrPartitioningNotSupported)
	})
}

func TestStreamPartitionedExport(t *testing.T) {
	t.Run("iterates all relationships", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 50)

		iter, err := StreamPartitionedExport(t.Context(), ds, StreamRequest{
			Revision: rev,
		})
		require.NoError(t, err)

		count := 0
		for _, err := range iter {
			require.NoError(t, err)
			count++
		}
		require.Equal(t, 50, count)
	})

	t.Run("empty table returns empty iterator", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev, err := ds.HeadRevision(t.Context())
		require.NoError(t, err)

		iter, err := StreamPartitionedExport(t.Context(), ds, StreamRequest{
			Revision: rev.Revision,
		})
		require.NoError(t, err)

		count := 0
		for _, err := range iter {
			require.NoError(t, err)
			count++
		}
		require.Equal(t, 0, count)
	})

	t.Run("nil revision returns error", func(t *testing.T) {
		ds := newTestDatastore(t)
		_, err := StreamPartitionedExport(t.Context(), ds, StreamRequest{})
		require.Error(t, err)
	})
}

func newTestDatastore(t *testing.T) datastore.Datastore {
	t.Helper()
	ds, err := memdb.NewMemdbDatastore(0, 0, 1*time.Hour)
	require.NoError(t, err)
	return ds
}

func writeTestRelationships(t *testing.T, ds datastore.Datastore, count int) datastore.Revision {
	t.Helper()
	ctx := t.Context()
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
