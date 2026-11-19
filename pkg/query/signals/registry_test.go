package signals_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

type sum struct{ n int }

func (s *sum) Add(v int) error                            { s.n += v; return nil }
func (s *sum) Value(time.Time) (int, signal.Availability) { return s.n, signal.Available }
func (s *sum) Size() int64                                { return 8 }
func (s *sum) Clone() signal.Accumulator[int, int]        { return &sum{s.n} }
func definition(name signal.Name) signal.Descriptor {
	return signal.NewSource[string, int, int](name, signal.SourceConfig[string, int, int]{
		Policy: signal.RevisionBound,
		New:    func() signal.Accumulator[int, int] { return &sum{} },
	})
}

func TestRegistryRejectsDuplicateName(t *testing.T) {
	_, err := signals.NewRegistry(definition("a"), definition("a"))
	require.ErrorContains(t, err, "duplicate")
}

func TestRegistryRejectsWrongBinding(t *testing.T) {
	d := definition("a")
	d.ValueType = nil
	_, err := signals.NewRegistry(d)
	require.Error(t, err)
}

func TestRegistryRejectsMissingDependency(t *testing.T) {
	d := definition("a")
	d.Dependencies = []signal.Name{"missing"}
	_, err := signals.NewRegistry(d)
	require.ErrorContains(t, err, "missing")
}

func TestRegistryRejectsCycle(t *testing.T) {
	a, b := definition("a"), definition("b")
	a.Dependencies = []signal.Name{"b"}
	b.Dependencies = []signal.Name{"a"}
	_, err := signals.NewRegistry(a, b)
	require.ErrorContains(t, err, "cycle")
}

func TestFrozenRegistryRejectsMutation(t *testing.T) {
	r, err := signals.NewRegistry(definition("a"))
	require.NoError(t, err)
	require.NoError(t, r.Freeze())
	require.Error(t, r.Register(definition("b")))
}
