package signal_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

type lookupView struct {
	values map[signal.Name]signal.Estimate[int]
	errors map[signal.Name]error
	reads  []signal.Name
}

// Read supplies controlled estimates and records lookup order.
func (v *lookupView) Read(name signal.Name, key any, kt, vt reflect.Type) (any, signal.Availability, signal.Evidence, error) {
	v.reads = append(v.reads, name)

	if key != "population" || kt != reflect.TypeFor[string]() || vt != reflect.TypeFor[int]() {
		return nil, signal.Missing, signal.Evidence{}, errors.New("wrong lookup binding")
	}

	e, ok := v.values[name]
	if !ok {
		e.Availability = signal.Missing
	}

	return e.Value, e.Availability, e.Evidence, v.errors[name]
}

func TestLookupFirst(t *testing.T) {
	first := signal.Reference[string, int]{Name: "first"}
	second := signal.Reference[string, int]{Name: "second"}
	provenance := signal.Evidence{ID: signal.EvidenceID{Epoch: 10, Version: 3}, Historical: true}

	t.Run("available first avoids reading fallback", func(t *testing.T) {
		view := &lookupView{values: map[signal.Name]signal.Estimate[int]{
			first.Name: {Value: 7, Availability: signal.Available, Evidence: provenance},
		}}
		e, name, err := signal.LookupFirst(view, "population", first, second)
		require.NoError(t, err)
		require.Equal(t, first.Name, name)
		require.Equal(t, 7, e.Value)
		require.Equal(t, provenance, e.Evidence)
		require.Equal(t, []signal.Name{first.Name}, view.reads)
	})

	for _, status := range []signal.Availability{signal.Missing, signal.Expired, signal.Evicted, signal.Unsupported} {
		t.Run("unavailable fallback", func(t *testing.T) {
			view := &lookupView{values: map[signal.Name]signal.Estimate[int]{
				first.Name:  {Availability: status},
				second.Name: {Value: 11, Availability: signal.Available, Evidence: provenance},
			}}
			e, name, err := signal.LookupFirst(view, "population", first, second)
			require.NoError(t, err)
			require.Equal(t, second.Name, name)
			require.Equal(t, 11, e.Value)
			require.Equal(t, provenance, e.Evidence)
			require.Equal(t, []signal.Name{first.Name, second.Name}, view.reads)
		})
	}

	t.Run("no available signal", func(t *testing.T) {
		view := &lookupView{}
		e, name, err := signal.LookupFirst(view, "population", first, second)
		require.NoError(t, err)
		require.False(t, e.IsAvailable())
		require.Equal(t, signal.Missing, e.Availability)
		require.Empty(t, name)
		require.Equal(t, []signal.Name{first.Name, second.Name}, view.reads)
	})

	t.Run("empty candidates", func(t *testing.T) {
		view := &lookupView{}
		e, name, err := signal.LookupFirst[string, int](view, "population")
		require.NoError(t, err)
		require.False(t, e.IsAvailable())
		require.Empty(t, name)
		require.Empty(t, view.reads)
	})

	t.Run("lookup error prevents fallback", func(t *testing.T) {
		failure := errors.New("binding failure")
		view := &lookupView{errors: map[signal.Name]error{first.Name: failure}}
		_, name, err := signal.LookupFirst(view, "population", first, second)
		require.ErrorIs(t, err, failure)
		require.Equal(t, first.Name, name)
		require.Equal(t, []signal.Name{first.Name}, view.reads)
	})
}
