package signals_test

import (
	"runtime"
	"sync"
	"testing"
	"time"
	"weak"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func stored(hash string) *datastore.ReadOnlyStoredSchema {
	return datastore.NewReadOnlyStoredSchema(
		&core.StoredSchema{VersionOneof: &core.StoredSchema_V1{V1: &core.StoredSchema_V1StoredSchema{SchemaHash: hash}}},
	)
}

func owner(t *testing.T, opts signals.Options) *signals.Owner {
	t.Helper()
	r, err := signals.NewRegistry(definition("sum"))
	require.NoError(t, err)
	o, err := signals.NewOwner(r, opts)
	require.NoError(t, err)
	return o
}

func view(t *testing.T, o *signals.Owner, hash string, rev int64) *signals.View {
	t.Helper()
	v, err := o.Bind(
		stored(hash),
		signal.Scope{SchemaHash: hash, Revision: revisions.NewForTransactionID(safecast.MustConvert[uint64](rev))},
	)
	require.NoError(t, err)
	return v
}

var (
	sumRef   = signal.Reference[string, int]{Name: "sum"}
	sumInput = signal.Input[string, int]{Name: "sum"}
)

func TestExactRevisionIsolation(t *testing.T) {
	o := owner(t, signals.Options{})
	a, b := view(t, o, "a", 9), view(t, o, "a", 10)
	require.NoError(t, signal.Observe(a, sumInput, "x", 7))
	v, err := signal.Lookup(a, sumRef, "x")
	require.NoError(t, err)
	require.Equal(t, 7, v.Value)
	v, err = signal.Lookup(b, sumRef, "x")
	require.NoError(t, err)
	require.Equal(t, signal.Missing, v.Availability)
}

func TestOwnerAndSchemaIsolation(t *testing.T) {
	a, b := owner(t, signals.Options{}), owner(t, signals.Options{})
	v := view(t, a, "a", 1)
	require.NoError(t, signal.Observe(v, sumInput, "x", 7))

	for _, other := range []*signals.View{view(t, b, "a", 1), view(t, a, "b", 1)} {
		e, err := signal.Lookup(other, sumRef, "x")
		require.NoError(t, err)
		require.Equal(t, signal.Missing, e.Availability)
	}
}

func TestPlanReadsRemainStable(t *testing.T) {
	v := view(t, owner(t, signals.Options{}), "a", 1)
	require.NoError(t, signal.Observe(v, sumInput, "x", 2))
	plan := v.ForPlan()
	before, err := signal.Lookup(plan, sumRef, "x")
	require.NoError(t, err)
	require.NoError(t, signal.Observe(v, sumInput, "x", 5))
	after, err := signal.Lookup(plan, sumRef, "x")
	require.NoError(t, err)
	require.Equal(t, before, after)
	fresh, _ := signal.Lookup(v.ForPlan(), sumRef, "x")
	require.Equal(t, 7, fresh.Value)
}

func TestSchemaBindingRejectsHashMismatch(t *testing.T) {
	o := owner(t, signals.Options{})
	_, err := o.Bind(stored("a"), signal.Scope{SchemaHash: "b", Revision: revisions.NewForTransactionID(1)})
	require.Error(t, err)
}

func TestEvictionDoesNotMutateSnapshot(t *testing.T) {
	o := owner(t, signals.Options{MaxRevisions: 1})
	a := view(t, o, "a", 1)
	require.NoError(t, signal.Observe(a, sumInput, "x", 7))
	p := a.ForPlan()
	before, _ := signal.Lookup(p, sumRef, "x")
	b := view(t, o, "a", 2)
	require.NoError(t, signal.Observe(b, sumInput, "x", 9))
	after, _ := signal.Lookup(p, sumRef, "x")
	require.Equal(t, before, after)
	old, _ := signal.Lookup(a, sumRef, "x")
	require.Equal(t, signal.Missing, old.Availability)
}

func TestTypedBindingAndConcurrentUpdates(t *testing.T) {
	v := view(t, owner(t, signals.Options{}), "a", 1)
	bad := signal.Reference[int, int]{Name: "sum"}
	_, err := signal.Lookup(v, bad, 1)
	require.Error(t, err)
	var wg sync.WaitGroup

	for range 20 {
		wg.Go(func() {
			for range 10 {
				require.NoError(t, signal.Observe(v, sumInput, "x", 1))
			}
		})
	}

	wg.Wait()
	e, err := signal.Lookup(v, sumRef, "x")
	require.NoError(t, err)
	require.Equal(t, 200, e.Value)
}

func TestOversizeAdmissionIsUnavailable(t *testing.T) {
	o := owner(t, signals.Options{MaxEntryBytes: 1})
	v := view(t, o, "a", 1)
	require.NoError(t, signal.Observe(v, sumInput, "x", 7))
	e, err := signal.Lookup(v, sumRef, "x")
	require.NoError(t, err)
	require.NotEqual(t, signal.Available, e.Availability)
	require.LessOrEqual(t, o.ResidentBytes(), int64(16<<20))
}

func TestFactoryCanInspectOwner(t *testing.T) {
	var o *signals.Owner
	d := definition("sum")
	original := d.New
	d.New = func() signal.State { _ = o.ResidentBytes(); return original() }
	r, err := signals.NewRegistry(d)
	require.NoError(t, err)
	o, err = signals.NewOwner(r, signals.Options{})
	require.NoError(t, err)
	v := view(t, o, "a", 1)
	done := make(chan error, 1)
	go func() { done <- signal.Observe(v, sumInput, "x", 1) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("provider factory held owner mutex")
	}
}

func TestSchemaAttachmentDoesNotRetainOwner(t *testing.T) {
	schema := stored("shared")
	create := func() weak.Pointer[signals.Owner] {
		o := owner(t, signals.Options{})
		v, err := o.Bind(schema, signal.Scope{SchemaHash: "shared", Revision: revisions.NewForTransactionID(1)})
		require.NoError(t, err)
		require.NoError(t, signal.Observe(v, sumInput, "x", 1))
		return weak.Make(o)
	}
	discarded := create()

	for range 20 {
		runtime.GC()

		if discarded.Value() == nil {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	require.Nil(t, discarded.Value())
	runtime.KeepAlive(schema)
}
