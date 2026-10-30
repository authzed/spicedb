package signals_test

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestNonreflexiveKeyIsRejected(t *testing.T) {
	type key struct{ Value [1]float64 }

	d := signal.NewSource[key, int, int]("sum", signal.SourceConfig[key, int, int]{
		New: func() signal.Accumulator[int, int] { return &sum{} },
	})
	r, err := signals.NewRegistry(d)
	require.NoError(t, err)
	o, err := signals.NewOwner(r, signals.Options{})
	require.NoError(t, err)
	v := view(t, o, "a", 1)
	invalid := key{Value: [1]float64{math.NaN()}}

	require.ErrorContains(t, signal.Observe(v, signal.Input[key, int]{Name: "sum"}, invalid, 1), "key")
	_, err = signal.Lookup(v, signal.Reference[key, int]{Name: "sum"}, invalid)
	require.ErrorContains(t, err, "key")
	_, err = signal.Lookup(v.ForPlan(), signal.Reference[key, int]{Name: "sum"}, invalid)
	require.ErrorContains(t, err, "key")
}

func TestRegistryRejectsReferenceKeys(t *testing.T) {
	for _, keyType := range []reflect.Type{
		reflect.TypeFor[*string](),
		reflect.TypeFor[any](),
		reflect.TypeFor[struct{ Value any }](),
		reflect.TypeFor[[1]chan int](),
	} {
		t.Run(keyType.String(), func(t *testing.T) {
			d := definition("sum")
			d.KeyType = keyType
			_, err := signals.NewRegistry(d)

			require.ErrorContains(t, err, "key")
		})
	}
}

func TestSourceRequiresConstructor(t *testing.T) {
	d := signal.NewSource[string, int, int]("sum", signal.SourceConfig[string, int, int]{})
	_, err := signals.NewRegistry(d)

	require.Error(t, err)
}

func TestSchemaTransferHasIndependentPublicationIdentity(t *testing.T) {
	d := definition("sum")
	d.Policy = signal.CrossRevision
	d.Transfer = func(signal.SchemaChange, signal.Footprint) signal.TransferDecision {
		return signal.TransferDecision{Action: signal.Retain}
	}
	r, err := signals.NewRegistry(d)
	require.NoError(t, err)
	o, err := signals.NewOwner(r, signals.Options{})
	require.NoError(t, err)
	a := view(t, o, "a", 1)

	require.NoError(t, signal.Observe(a, sumInput, "x", 1))
	b := view(t, o, "b", 2)
	require.NoError(t, signal.Observe(a, sumInput, "x", 2))
	require.NoError(t, signal.Observe(b, sumInput, "x", 5))
	av, err := signal.Lookup(a, sumRef, "x")
	require.NoError(t, err)
	bv, err := signal.Lookup(b, sumRef, "x")
	require.NoError(t, err)
	require.Equal(t, 3, av.Value)
	require.Equal(t, 6, bv.Value)
	require.NotEqual(t, av.Evidence.ID, bv.Evidence.ID)
}

func TestFootprintCountsTowardEntryBudget(t *testing.T) {
	d := definition("sum")
	d.Footprint = func(any) signal.Footprint {
		return signal.Footprint{Caveats: []string{strings.Repeat("x", 4096)}}
	}
	r, err := signals.NewRegistry(d)
	require.NoError(t, err)
	o, err := signals.NewOwner(r, signals.Options{MaxEntryBytes: 1024})
	require.NoError(t, err)
	v := view(t, o, "a", 1)

	require.NoError(t, signal.Observe(v, sumInput, "x", 1))
	e, err := signal.Lookup(v, sumRef, "x")

	require.NoError(t, err)
	require.False(t, e.IsAvailable())
}

func TestFootprintIsOwnedByEntry(t *testing.T) {
	footprint := signal.Footprint{Elements: []signal.Element{{Namespace: "doc", Relation: "read"}}}
	d := definition("sum")
	d.Policy = signal.CrossRevision
	d.Footprint = func(any) signal.Footprint { return footprint }
	d.Transfer = func(_ signal.SchemaChange, f signal.Footprint) signal.TransferDecision {
		require.Equal(t, "read", f.Elements[0].Relation)
		return signal.TransferDecision{Action: signal.Retain}
	}
	r, err := signals.NewRegistry(d)
	require.NoError(t, err)
	o, err := signals.NewOwner(r, signals.Options{})
	require.NoError(t, err)
	a := view(t, o, "a", 1)

	require.NoError(t, signal.Observe(a, sumInput, "x", 1))
	footprint.Elements[0].Relation = "edited"
	b := view(t, o, "b", 2)
	e, err := signal.Lookup(b, sumRef, "x")

	require.NoError(t, err)
	require.Equal(t, 1, e.Value)
}

func TestBuildOnlySource(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		d := signal.NewSource[string, int, int]("sum", signal.SourceConfig[string, int, int]{
			Build: func(now func() time.Time) signal.Accumulator[int, int] {
				require.Equal(t, started, now())
				return &sum{}
			},
		})
		o := querysignalstest.Owner(t, d)
		v := querysignalstest.View(t, o, "a", 1)

		require.NoError(t, signal.Observe(v, sumInput, "x", 4))
		e, err := signal.Lookup(v, sumRef, "x")

		require.NoError(t, err)
		require.Equal(t, 4, e.Value)
	})
}

func TestDerivedSignalsReceiveIncrementsAcrossRevisions(t *testing.T) {
	source := definition("sum")
	derived := signal.NewDerived("history", sumInput, signal.SourceConfig[string, int, int]{
		Policy: signal.CrossRevision,
		New:    func() signal.Accumulator[int, int] { return &sum{} },
	})
	o := querysignalstest.Owner(t, source, derived)
	a := querysignalstest.View(t, o, "a", 1)
	b := querysignalstest.View(t, o, "a", 2)

	require.NoError(t, signal.Observe(a, sumInput, "x", 3))
	require.NoError(t, signal.Observe(a, sumInput, "x", 5))
	require.NoError(t, signal.Observe(b, sumInput, "x", 7))
	history, err := signal.Lookup(b, signal.Reference[string, int]{Name: "history"}, "x")

	require.NoError(t, err)
	require.Equal(t, 15, history.Value)
	require.True(t, history.Evidence.Historical)
	require.Error(t, signal.Observe(b, signal.Input[string, int]{Name: "history"}, "x", 1))
}

func TestSelectiveSchemaTransfer(t *testing.T) {
	const original = "definition user {}\ndefinition doc {\n relation viewer: user\n permission read = viewer\n}"

	for _, tc := range []struct {
		name string
		next string
		keep bool
	}{
		{"unrelated", original + "\ndefinition other {}", true},
		{"changed", "definition user {}\ndefinition doc {\n relation editor: user\n permission read = editor\n}", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := definition("sum")
				d.Policy = signal.CrossRevision
				d.Footprint = func(any) signal.Footprint {
					return signal.Footprint{Elements: []signal.Element{{Namespace: "doc", Relation: "read"}, {Namespace: "doc", Relation: "viewer"}}}
				}
				d.Transfer = signal.Unchanged
				o := querysignalstest.Owner(t, d)
				a, err := o.Bind(querysignalstest.Schema(t, "a", original), signal.Scope{
					SchemaHash: "a", Revision: revisions.NewForTransactionID(1),
				})

				require.NoError(t, err)
				require.NoError(t, signal.Observe(a, sumInput, "x", 7))
				before, err := signal.Lookup(a, sumRef, "x")

				require.NoError(t, err)
				time.Sleep(time.Minute)
				b, err := o.Bind(querysignalstest.Schema(t, "b", tc.next), signal.Scope{
					SchemaHash: "b", Revision: revisions.NewForTransactionID(2),
				})

				require.NoError(t, err)
				after, err := signal.Lookup(b, sumRef, "x")

				require.NoError(t, err)
				require.Equal(t, tc.keep, after.IsAvailable())

				if tc.keep {
					require.Equal(t, 7, after.Value)
					require.Equal(t, before.Evidence.LastObserved, after.Evidence.LastObserved)
					require.Equal(t, before.Evidence.PublishedAt, after.Evidence.PublishedAt)
				}
			})
		})
	}
}
