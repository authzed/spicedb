package poolsim

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

func newTestPool(t *testing.T, size int, opts ...Option) (*Datastore, datastore.Revision) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(t, rawDS)
	return New(ds, size, opts...), rev
}

func TestIteratorHoldsConnectionUntilIterationEnds(t *testing.T) {
	pool, rev := newTestPool(t, 2)
	reader := pool.SnapshotReader(rev)

	it, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceType: "document"}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(t, err)
	require.Equal(t, int64(0), pool.InUse(), "query should not hold a connection until iterated")

	count := 0
	for _, err := range it {
		require.NoError(t, err)
		require.Equal(t, int64(1), pool.InUse())
		count++
	}
	require.Positive(t, count)
	require.Equal(t, int64(0), pool.InUse())

	// Breaking out early must also release.
	it, err = reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceType: "document"}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(t, err)
	for range it {
		break
	}
	require.Equal(t, int64(0), pool.InUse())
}

func TestNestedAcquireFailsFast(t *testing.T) {
	pool, rev := newTestPool(t, 2)
	reader := pool.SnapshotReader(rev)

	it, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceType: "document"}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(t, err)

	for _, err := range it {
		require.NoError(t, err)
		_, _, err := reader.LegacyReadNamespaceByName(t.Context(), "document")
		require.ErrorIs(t, err, ErrNestedAcquire)
		break
	}

	require.Equal(t, int64(1), pool.NestedAcquisitions())
	require.Equal(t, int64(0), pool.InUse())
}

func TestNestedAcquireDeadlocksWhenAllowed(t *testing.T) {
	pool, rev := newTestPool(t, 1, WithAllowNestedAcquire(), WithAcquireTimeout(50*time.Millisecond))
	reader := pool.SnapshotReader(rev)

	it, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceType: "document"}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(t, err)

	for _, err := range it {
		require.NoError(t, err)
		_, _, err := reader.LegacyReadNamespaceByName(t.Context(), "document")
		require.ErrorIs(t, err, ErrPoolExhausted)
		break
	}

	require.Equal(t, int64(1), pool.NestedAcquisitions())
	require.Equal(t, int64(1), pool.AcquireTimeouts())
}

func TestSequentialAccessDoesNotNest(t *testing.T) {
	pool, rev := newTestPool(t, 1)
	reader := pool.SnapshotReader(rev)

	it, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceType: "document"}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(t, err)

	rels, err := datastore.IteratorToSlice(it)
	require.NoError(t, err)
	require.NotEmpty(t, rels)

	_, _, err = reader.LegacyReadNamespaceByName(t.Context(), "document")
	require.NoError(t, err)
	require.Equal(t, int64(0), pool.NestedAcquisitions())
	require.Equal(t, int64(1), pool.MaxInUse())
}
