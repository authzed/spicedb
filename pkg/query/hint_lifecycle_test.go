package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query/planhint"
)

func TestCompilerAccountsForUnusableHints(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fingerprint string
		outcome     planhint.Outcome
		reason      planhint.ReasonCode
	}{
		{"stale subtree", "old-subtree", planhint.Invalidated, planhint.StaleTarget},
		{"unsupported node", "", planhint.Declined, planhint.UnsupportedTarget},
	} {
		t.Run(tc.name, func(t *testing.T) {
			co, err := CanonicalizeOutline(Outline{Type: NullIteratorType})

			require.NoError(t, err)
			ledger := planhint.NewLedger(nil)
			hint := ArrowDirectionHint(rightToLeft)
			hint.Target = planhint.Target{Node: uint64(co.Root.ID), Fingerprint: tc.fingerprint}
			hint.ID, err = ledger.Issue(hint)

			require.NoError(t, err)
			co.Hints = map[OutlineNodeID][]Hint{co.Root.ID: {hint}}
			co.HintLedger = ledger
			_, err = co.Compile()

			require.NoError(t, err)
			decisions := ledger.Decisions()

			require.Len(t, decisions, 1)
			require.Equal(t, "iterator-compiler", decisions[0].Consumer)
			require.Equal(t, tc.outcome, decisions[0].Outcome)
			require.Equal(t, tc.reason, decisions[0].Reason)
		})
	}
}

func TestCompilerBindsUnregisteredAdvisorHint(t *testing.T) {
	co, err := CanonicalizeOutline(Outline{Type: NullIteratorType})

	require.NoError(t, err)
	co.HintLedger = planhint.NewLedger(nil)
	co.Hints = map[OutlineNodeID][]Hint{co.Root.ID: {{Payload: planhint.BranchOrder{Order: []int{0}}}}}
	_, err = co.Compile()

	require.NoError(t, err)
	pending := co.HintLedger.Hints()

	require.Len(t, pending, 1)
	require.Equal(t, "legacy-advisor", pending[0].Producer)
	require.Equal(t, uint64(co.Root.ID), pending[0].Target.Node)
	require.Equal(t, string(co.Root.Serialize()), pending[0].Target.Fingerprint)
	decisions := co.HintLedger.Decisions()

	require.Len(t, decisions, 1)
	require.Equal(t, planhint.Unconsumed, decisions[0].Outcome)
}

func TestInvalidAdvisorDirectionCannotCompile(t *testing.T) {
	co, err := CanonicalizeOutline(Outline{Type: NullIteratorType})

	require.NoError(t, err)
	hint := ArrowDirectionHint(arrowDirection(99))
	require.Equal(t, planhint.InvalidDirection, hint.Payload.(planhint.ArrowDirection).Direction)
	co.Hints = map[OutlineNodeID][]Hint{co.Root.ID: {hint}}
	_, err = co.Compile()

	require.ErrorContains(t, err, "invalid arrow direction")
}

func TestExecutionDescriptorsSeparateStructuralIdentityFromStrategy(t *testing.T) {
	left := NewFixedIterator(*MustPathFromString("document:d#parent@folder:f"))
	right := NewFixedIterator(*MustPathFromString("folder:f#viewer@user:a"))
	arrow := NewArrowIterator(left, right)
	parent := NewUnionIterator(arrow, NewFixedIterator())
	forward, err := ExecutionDescriptors(parent)

	require.NoError(t, err)
	require.Equal(t, "default", forward[arrow].Strategy)
	require.Equal(t, "body", forward[right].Role)
	require.NotEmpty(t, forward[left].Identity)
	arrow.direction = rightToLeft
	reversed, err := ExecutionDescriptors(parent)

	require.NoError(t, err)
	require.Equal(t, forward[arrow].Identity, reversed[arrow].Identity)
	require.Equal(t, "default/right-to-left", reversed[arrow].Strategy)
	require.Equal(t, "default/default/right-to-left", reversed[parent].Strategy)
	require.Equal(t, "default", reversed[left].Strategy)
}
