package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCombinePlanAdvisors_GetHints(t *testing.T) {
	t.Run("returns first non-nil hints", func(t *testing.T) {
		advisor1 := &mockAdvisor{
			hints: []Hint{func(Iterator) error { return nil }},
		}
		advisor2 := &mockAdvisor{
			hints: []Hint{func(Iterator) error { return nil }},
		}

		combined := CombinePlanAdvisors(advisor1, advisor2)

		hints, err := combined.GetHints(Outline{}, CanonicalOutline{})
		require.NoError(t, err)
		require.Len(t, hints, 1)
	})

	t.Run("falls back to second advisor if first returns empty", func(t *testing.T) {
		advisor1 := &mockAdvisor{
			hints: []Hint{},
		}
		advisor2 := &mockAdvisor{
			hints: []Hint{func(Iterator) error { return nil }},
		}

		combined := CombinePlanAdvisors(advisor1, advisor2)

		hints, err := combined.GetHints(Outline{}, CanonicalOutline{})
		require.NoError(t, err)
		require.Len(t, hints, 1)
	})

	t.Run("returns nil if all advisors return empty", func(t *testing.T) {
		advisor1 := &mockAdvisor{
			hints: []Hint{},
		}
		advisor2 := &mockAdvisor{
			hints: []Hint{},
		}

		combined := CombinePlanAdvisors(advisor1, advisor2)

		hints, err := combined.GetHints(Outline{}, CanonicalOutline{})
		require.NoError(t, err)
		require.Nil(t, hints)
	})

	t.Run("returns error from first advisor that errors", func(t *testing.T) {
		testErr := errors.New("test error")
		advisor1 := &mockAdvisor{
			err: testErr,
		}
		advisor2 := &mockAdvisor{
			hints: []Hint{func(Iterator) error { return nil }},
		}

		combined := CombinePlanAdvisors(advisor1, advisor2)

		hints, err := combined.GetHints(Outline{}, CanonicalOutline{})
		require.Error(t, err)
		require.Nil(t, hints)
	})

	t.Run("runs through all advisors until non-empty", func(t *testing.T) {
		advisor1 := &mockAdvisor{
			hints: []Hint{},
		}
		advisor2 := &mockAdvisor{
			hints: []Hint{},
		}
		advisor3 := &mockAdvisor{
			hints: []Hint{func(Iterator) error { return nil }},
		}

		combined := CombinePlanAdvisors(advisor1, advisor2, advisor3)

		hints, err := combined.GetHints(Outline{}, CanonicalOutline{})
		require.NoError(t, err)
		require.Len(t, hints, 1)
	})
}

func TestCombinePlanAdvisors_GetMutations(t *testing.T) {
	t.Run("returns first non-nil mutations", func(t *testing.T) {
		advisor1 := &mockAdvisor{
			mutations: []OutlineMutation{func(o Outline) Outline { return o }},
		}
		advisor2 := &mockAdvisor{
			mutations: []OutlineMutation{func(o Outline) Outline { return o }},
		}

		combined := CombinePlanAdvisors(advisor1, advisor2)

		mutations, err := combined.GetMutations(Outline{}, CanonicalOutline{})
		require.NoError(t, err)
		require.Len(t, mutations, 1)
	})

	t.Run("falls back to second advisor if first returns empty", func(t *testing.T) {
		advisor1 := &mockAdvisor{
			mutations: []OutlineMutation{},
		}
		advisor2 := &mockAdvisor{
			mutations: []OutlineMutation{func(o Outline) Outline { return o }},
		}

		combined := CombinePlanAdvisors(advisor1, advisor2)

		mutations, err := combined.GetMutations(Outline{}, CanonicalOutline{})
		require.NoError(t, err)
		require.Len(t, mutations, 1)
	})

	t.Run("returns nil if all advisors return empty", func(t *testing.T) {
		advisor1 := &mockAdvisor{
			mutations: []OutlineMutation{},
		}
		advisor2 := &mockAdvisor{
			mutations: []OutlineMutation{},
		}

		combined := CombinePlanAdvisors(advisor1, advisor2)

		mutations, err := combined.GetMutations(Outline{}, CanonicalOutline{})
		require.NoError(t, err)
		require.Nil(t, mutations)
	})

	t.Run("returns error from first advisor that errors", func(t *testing.T) {
		testErr := errors.New("test error")
		advisor1 := &mockAdvisor{
			err: testErr,
		}
		advisor2 := &mockAdvisor{
			mutations: []OutlineMutation{func(o Outline) Outline { return o }},
		}

		combined := CombinePlanAdvisors(advisor1, advisor2)

		mutations, err := combined.GetMutations(Outline{}, CanonicalOutline{})
		require.Error(t, err)
		require.Nil(t, mutations)
	})

	t.Run("runs through all advisors until non-empty", func(t *testing.T) {
		advisor1 := &mockAdvisor{
			mutations: []OutlineMutation{},
		}
		advisor2 := &mockAdvisor{
			mutations: []OutlineMutation{},
		}
		advisor3 := &mockAdvisor{
			mutations: []OutlineMutation{func(o Outline) Outline { return o }},
		}

		combined := CombinePlanAdvisors(advisor1, advisor2, advisor3)

		mutations, err := combined.GetMutations(Outline{}, CanonicalOutline{})
		require.NoError(t, err)
		require.Len(t, mutations, 1)
	})
}

type mockAdvisor struct {
	hints     []Hint
	mutations []OutlineMutation
	err       error
}

func (m *mockAdvisor) GetHints(outline Outline, keySource CanonicalKeySource) ([]Hint, error) {
	return m.hints, m.err
}

func (m *mockAdvisor) GetMutations(outline Outline, keySource CanonicalKeySource) ([]OutlineMutation, error) {
	return m.mutations, m.err
}
