package signals_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query/signals/historicalresultsize"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

const original = "definition user {}\ndefinition doc {\n relation viewer: user\n permission read = viewer\n}"

func TestHistoricalSelectiveSchemaTransfer(t *testing.T) {
	for _, tc := range []struct {
		name, next string
		want       signal.Availability
	}{
		{"unrelated", original + "\ndefinition other {}", signal.Available},
		{
			"changed",
			"definition user {}\ndefinition doc {\n relation viewer: user\n relation editor: user\n permission read = editor\n}",
			signal.Missing,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := querysignalstest.NewClock()
			o := querysignalstest.Owner(t, c, resultsize.New(), historicalresultsize.New())
			a, err := o.Bind(
				querysignalstest.Schema(t, "a", original),
				signal.Scope{SchemaHash: "a", Revision: revisions.NewForTransactionID(1)},
			)
			require.NoError(t, err)
			k := signal.ExecutionKey{
				Identity:  "read",
				Operation: signal.IterSubjects,
				Dependencies: signal.EncodeFootprint(signal.Footprint{
					Elements: []signal.Element{{Namespace: "doc", Relation: "read"}, {Namespace: "doc", Relation: "viewer"}},
				}),
			}

			for range 4 {
				require.NoError(t, signal.Observe(a, resultsize.Input, k, resultsize.Observation{Count: 7, Started: c.Now()}))
			}

			before, _ := signal.Lookup(a, historicalresultsize.Ref, k)
			c.Advance(30 * time.Second)
			b, err := o.Bind(
				querysignalstest.Schema(t, "b", tc.next),
				signal.Scope{SchemaHash: "b", Revision: revisions.NewForTransactionID(2)},
			)
			require.NoError(t, err)
			next, _ := signal.Lookup(b, historicalresultsize.Ref, k)
			require.Equal(t, tc.want, next.Availability)

			if next.IsAvailable() {
				require.InDelta(t, before.Value.Mean, next.Value.Mean, 1e-9)
				require.Equal(t, before.Evidence.LastObserved, next.Evidence.LastObserved)
				require.Equal(t, before.Evidence.PublishedAt, next.Evidence.PublishedAt)
			}

			exact, _ := signal.Lookup(b, resultsize.Ref, k)
			require.Equal(t, signal.Missing, exact.Availability)
			old, _ := signal.Lookup(a, historicalresultsize.Ref, k)
			require.Equal(t, uint64(4), old.Value.Samples)
			require.NoError(t, signal.Observe(a, resultsize.Input, k, resultsize.Observation{Count: 70, Started: c.Now()}))
			again, _ := signal.Lookup(b, historicalresultsize.Ref, k)
			require.Equal(t, next.Value.Samples, again.Value.Samples)
		})
	}
}

func TestSchemaABADoesNotDoubleSeed(t *testing.T) {
	c := querysignalstest.NewClock()
	o := querysignalstest.Owner(t, c, resultsize.New(), historicalresultsize.New())
	a, err := o.Bind(querysignalstest.Schema(t, "a", original), signal.Scope{SchemaHash: "a", Revision: revisions.NewForTransactionID(1)})
	require.NoError(t, err)
	k := signal.ExecutionKey{Identity: "x", Operation: signal.IterSubjects}

	for range 2 {
		require.NoError(t, signal.Observe(a, resultsize.Input, k, resultsize.Observation{Count: 7, Started: c.Now()}))
	}

	for range 4 {
		_, err = o.Bind(
			querysignalstest.Schema(t, "b", original+"\ndefinition other {}"),
			signal.Scope{SchemaHash: "b", Revision: revisions.NewForTransactionID(2)},
		)
		require.NoError(t, err)
		_, err = o.Bind(
			querysignalstest.Schema(t, "a", original),
			signal.Scope{SchemaHash: "a", Revision: revisions.NewForTransactionID(1)},
		)
		require.NoError(t, err)
	}

	e, _ := signal.Lookup(a, historicalresultsize.Ref, k)
	require.Equal(t, uint64(2), e.Value.Samples)
}

func TestSchemaDiscountPreservesValueAndAge(t *testing.T) {
	c := querysignalstest.NewClock()
	d := historicalresultsize.New()
	d.Transfer = func(signal.SchemaChange, signal.Footprint) signal.TransferDecision {
		return signal.TransferDecision{Action: signal.Discount, Factor: .5, Reason: "test-compatible-change"}
	}
	o := querysignalstest.Owner(t, c, resultsize.New(), d)
	a, err := o.Bind(querysignalstest.Schema(t, "a", original), signal.Scope{SchemaHash: "a", Revision: revisions.NewForTransactionID(1)})
	require.NoError(t, err)
	k := signal.ExecutionKey{Identity: "x", Operation: signal.IterSubjects}

	for range 8 {
		require.NoError(t, signal.Observe(a, resultsize.Input, k, resultsize.Observation{Count: 10, Started: c.Now()}))
	}

	before, _ := signal.Lookup(a, historicalresultsize.Ref, k)
	c.Advance(30 * time.Second)
	b, err := o.Bind(
		querysignalstest.Schema(t, "b", original+"\ndefinition other {}"),
		signal.Scope{SchemaHash: "b", Revision: revisions.NewForTransactionID(2)},
	)
	require.NoError(t, err)
	after, _ := signal.Lookup(b, historicalresultsize.Ref, k)
	old, _ := signal.Lookup(a, historicalresultsize.Ref, k)
	require.Equal(t, signal.Available, after.Availability)
	require.InDelta(t, 10.0, after.Value.Mean, 1e-9)
	require.InDelta(t, old.Value.Weight*.5, after.Value.Weight, 1e-9)
	require.Equal(t, before.Evidence.PublishedAt, after.Evidence.PublishedAt)
}
