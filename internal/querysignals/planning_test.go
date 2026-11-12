package querysignals_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/querysignals"
	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/historicalresultsize"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

type loadedSchema struct {
	stored *datastore.ReadOnlyStoredSchema
}

func (s loadedSchema) StoredSchema() *datastore.ReadOnlyStoredSchema { return s.stored }
func TestBoundRevisionAndHistoricalReuse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry, err := signals.NewDefaultRegistry()
		require.NoError(t, err)
		owner, err := signals.NewOwner(registry, signals.Options{})
		require.NoError(t, err)
		schema := loadedSchema{querysignalstest.Stored("a")}
		rev := revisions.NewForTransactionID(1)
		a, err := querysignals.Bind(owner, schema, rev, "a")
		require.NoError(t, err)
		k := signal.ExecutionKey{Identity: "x", Operation: signal.IterSubjects}
		require.NoError(t, signal.Observe(a, resultsize.Input, k, resultsize.Observation{Count: 12, Started: time.Now()}))
		b, err := querysignals.Bind(owner, schema, revisions.NewForTransactionID(2), "a")
		require.NoError(t, err)
		exact, err := signal.Lookup(b, resultsize.Ref, k)
		require.NoError(t, err)
		require.Equal(t, signal.Missing, exact.Availability)
		old, err := signal.Lookup(b, historicalresultsize.Ref, k)
		require.NoError(t, err)
		require.InDelta(t, 12.0, old.Value.Mean, 1e-9)
		a2, err := querysignals.Bind(owner, schema, rev, "a")
		require.NoError(t, err)
		same, err := signal.Lookup(a2, resultsize.Ref, k)
		require.NoError(t, err)
		require.Equal(t, uint64(1), same.Value.Samples)

		// Legacy schema readers are not supported.
		_, err = querysignals.Bind(owner, struct{}{}, rev, "a")
		require.ErrorIs(t, err, querysignals.ErrLegacySchemaUnsupported)

		// A sentinel hash, such as one from a legacy ZedToken, binds to the loaded schema's hash.
		legacy, err := querysignals.Bind(owner, schema, rev, string(datalayer.NoSchemaHashInLegacyZedToken))
		require.NoError(t, err)
		shared, err := signal.Lookup(legacy, resultsize.Ref, k)
		require.NoError(t, err)
		require.Equal(t, uint64(1), shared.Value.Samples)

		// A real hash that does not match the loaded schema is still rejected.
		_, err = querysignals.Bind(owner, schema, rev, "b")
		require.Error(t, err)
	})
}
