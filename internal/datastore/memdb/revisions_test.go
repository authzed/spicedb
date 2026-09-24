package memdb

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestHeadRevision(t *testing.T) {
	ds, err := NewMemdbDatastore(0, 0, 500*time.Millisecond)
	require.NoError(t, err)

	olderResult, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)
	err = ds.CheckRevision(t.Context(), olderResult.Revision)
	require.NoError(t, err)

	time.Sleep(550 * time.Millisecond)

	// GC window elapsed, last revision is returned even if outside GC window
	newerResult, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)
	err = ds.CheckRevision(t.Context(), newerResult.Revision)
	require.NoError(t, err)
}

// TestOptimizedRevisionSeesFirstWrite asserts that the first write to a freshly
// created datastore is visible at the optimized revision, even when a
// quantization boundary happens to fall between the datastore's creation and
// that write. Rounding down to such a boundary would otherwise serve the
// still-empty snapshot taken at creation. The synctest bubble's clock is fake,
// so the boundary lands between the two exactly rather than by timing luck.
func TestOptimizedRevisionSeesFirstWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const quantization = 50 * time.Millisecond

		boundary := nextQuantizationBoundary(quantization)
		time.Sleep(time.Until(boundary.Add(-5 * time.Millisecond)))

		ds, err := NewMemdbDatastore(0, quantization, time.Hour)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = ds.Close()
		})
		require.True(t, time.Now().Before(boundary))

		time.Sleep(time.Until(boundary.Add(time.Millisecond)))

		_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Touch(tuple.MustParse("document:doc#viewer@user:tom")),
			})
		})
		require.NoError(t, err)

		optimized, err := ds.OptimizedRevision(t.Context())
		require.NoError(t, err)

		iter, err := ds.SnapshotReader(optimized.Revision).QueryRelationships(t.Context(), datastore.RelationshipsFilter{
			OptionalResourceType: "document",
		})
		require.NoError(t, err)

		rels, err := datastore.IteratorToSlice(iter)
		require.NoError(t, err)
		require.Len(t, rels, 1, "the optimized revision did not see the first write")
	})
}

// TestOptimizedRevisionFallbackReportsNoValidity covers the validity reported
// alongside the fallback above. When rounding down would hide the first write,
// OptimizedRevision answers with head instead — but the validity it computed
// belongs to the quantized revision it discarded. Handing that validity out
// lets a caller cache head for the rest of the interval and miss every write
// made during it, which is the staleness the fallback exists to avoid.
func TestOptimizedRevisionFallbackReportsNoValidity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const quantization = 50 * time.Millisecond

		boundary := nextQuantizationBoundary(quantization)
		time.Sleep(time.Until(boundary.Add(-5 * time.Millisecond)))

		ds, err := NewMemdbDatastore(0, quantization, time.Hour)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = ds.Close()
		})

		time.Sleep(time.Until(boundary.Add(time.Millisecond)))

		_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Touch(tuple.MustParse("document:doc#viewer@user:tom")),
			})
		})
		require.NoError(t, err)

		// The fallback is active: rounding down lands before the write.
		fallback, err := ds.OptimizedRevision(t.Context())
		require.NoError(t, err)
		require.Zero(t, fallback.ValidFor,
			"head was substituted for the quantized revision, so the quantized revision's validity must not be reported")

		// Past the next boundary the quantized revision is itself servable, the
		// fallback no longer applies, and a real validity is expected again.
		time.Sleep(time.Until(nextQuantizationBoundary(quantization).Add(time.Millisecond)))

		quantized, err := ds.OptimizedRevision(t.Context())
		require.NoError(t, err)
		require.Positive(t, quantized.ValidFor,
			"the quantized revision is servable here, so it should carry its own validity")
	})
}

// nextQuantizationBoundary returns the next instant that OptimizedRevision
// would round down to for the given quantization period.
func nextQuantizationBoundary(quantization time.Duration) time.Time {
	q := quantization.Nanoseconds()
	return time.Unix(0, (time.Now().UTC().UnixNano()/q+1)*q)
}
