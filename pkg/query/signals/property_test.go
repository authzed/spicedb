package signals_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestRevisionEvictionPreservesPlanningSnapshots(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		observations := rapid.SliceOfN(rapid.IntRange(-100, 100), 1, 20).Draw(rt, "observations")
		revision := rapid.Uint64Range(1, 1000).Draw(rt, "revision")
		key := rapid.StringMatching(`[a-z]{1,20}`).Draw(rt, "population")
		r, err := signals.NewRegistry(definition("sum"))

		require.NoError(rt, err)
		o, err := signals.NewOwner(r, signals.Options{MaxRevisions: 1, MaxBytes: 4096})

		require.NoError(rt, err)
		a, err := o.Bind(stored("schema"), signal.Scope{
			SchemaHash: "schema",
			Revision:   revisions.NewForTransactionID(revision),
		})

		require.NoError(rt, err)
		total := 0

		for _, observation := range observations {
			require.NoError(rt, signal.Observe(a, sumInput, key, observation))
			total += observation
		}

		plan := a.ForPlan()
		before, err := signal.Lookup(plan, sumRef, key)

		require.NoError(rt, err)
		require.Equal(rt, total, before.Value)
		b, err := o.Bind(stored("schema"), signal.Scope{
			SchemaHash: "schema",
			Revision:   revisions.NewForTransactionID(revision + 1),
		})

		require.NoError(rt, err)
		require.NoError(rt, signal.Observe(b, sumInput, key, 1))
		after, err := signal.Lookup(plan, sumRef, key)

		require.NoError(rt, err)
		require.Equal(rt, before, after)
		evicted, err := signal.Lookup(a, sumRef, key)

		require.NoError(rt, err)
		require.False(rt, evicted.IsAvailable())
		require.LessOrEqual(rt, o.ResidentBytes(), int64(4096))
	})
}
