package signals_test

import (
	"testing"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/branchoutcome"
	"github.com/authzed/spicedb/pkg/query/signals/contextfield"
	"github.com/authzed/spicedb/pkg/query/signals/historicalresultsize"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestRevisionSchemaEvictionStateMachine(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		registry, err := signals.NewRegistry(definition("sum"))
		require.NoError(t, err)
		o, err := signals.NewOwner(registry, signals.Options{MaxSchemas: 1, MaxRevisions: 1})
		require.NoError(t, err)
		schema := "a"
		revision := int64(1)
		model := 0

		for range rapid.IntRange(10, 80).Draw(t, "steps") {
			switch rapid.IntRange(0, 4).Draw(t, "action") {
			case 0:
				schema = map[bool]string{true: "a", false: "b"}[schema != "a"]
				model = 0
			case 1:
				// Revisit revisions in either order; eviction is LRU, never numeric ordering.
				if revision == 1 {
					revision = 2
				} else {
					revision = 1
				}

				model = 0
			}

			v, err := o.Bind(
				stored(schema),
				signal.Scope{SchemaHash: schema, Revision: revisions.NewForTransactionID(safecast.MustConvert[uint64](revision))},
			)
			require.NoError(t, err)

			if rapid.Bool().Draw(t, "observe") {
				n := rapid.IntRange(1, 9).Draw(t, "delta")
				require.NoError(t, signal.Observe(v, sumInput, "x", n))
				model += n
			}

			plan := v.ForPlan()
			e, err := signal.Lookup(plan, sumRef, "x")
			require.NoError(t, err)

			if model == 0 {
				require.Equal(t, signal.Missing, e.Availability)
			} else {
				require.Equal(t, model, e.Value)
				require.Equal(t, schema, e.Evidence.SourceSchemaHash)
			}

			// An already-acquired plan snapshot must remain unchanged after a later observation.
			require.NoError(t, signal.Observe(v, sumInput, "x", 1))
			model++
			again, err := signal.Lookup(plan, sumRef, "x")
			require.NoError(t, err)
			require.Equal(t, e, again)
			require.LessOrEqual(t, o.ResidentBytes(), int64(16<<20))
		}
	})
}

func TestOptionalLearningProfile(t *testing.T) {
	r, err := signals.NewDefaultRegistry()
	require.NoError(t, err)
	o, err := signals.NewOwner(r, signals.Options{Enabled: []string{string(historicalresultsize.Name)}})
	require.NoError(t, err)
	v := view(t, o, "a", 1)
	require.True(t, v.Enabled(resultsize.Name))
	require.True(t, v.Enabled(historicalresultsize.Name))
	require.False(t, v.Enabled(contextfield.Name))
	require.False(t, v.Enabled(branchoutcome.Name))
}
