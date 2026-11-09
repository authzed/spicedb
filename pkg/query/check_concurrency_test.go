package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query/planhint"
)

type concurrencyTrace struct{ responses []planhint.Disposition }

func (s *concurrencyTrace) Record(response planhint.Disposition) {
	s.responses = append(s.responses, response)
}

func TestCheckConcurrencyRuntimeAccounting(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		option   ContextOption
		frontier bool
		expected uint16
		reason   planhint.ReasonCode
	}{
		{name: "ceiling", expected: 2},
		{name: "observer", option: WithObserver(NewAnalyzeObserver()), expected: 1, reason: planhint.UnsupportedObserver},
		{name: "trace", option: WithTraceLogger(NewTraceLogger()), expected: 1, reason: planhint.UnsupportedObserver},
		{name: "frontier", frontier: true, expected: 1, reason: planhint.ActiveFrontier},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			trace := &concurrencyTrace{}
			ledger := planhint.NewLedger(trace)
			id, err := ledger.Issue(planhint.Hint{
				Producer: "fixed", Target: planhint.Target{Node: 1}, Payload: planhint.CheckConcurrency{Limit: 4},
			})
			require.NoError(t, err)
			ctx := NewLocalContext(t.Context(), WithCheckConcurrency(2), WithHintLedger(ledger))

			if scenario.option != nil {
				scenario.option(ctx)
			}

			if scenario.frontier {
				ctx.recursiveFrontierCollectors = map[uint64][]Object{1: {}}
			}

			for range 2 {
				width, err := ctx.setCheckConcurrency(4, id)
				require.NoError(t, err)
				require.Equal(t, scenario.expected, width)
			}

			require.Len(t, trace.responses, 2)
			response := trace.responses[0]
			require.Equal(t, "query-executor", response.Consumer)
			require.Equal(t, scenario.reason, response.Reason)
			require.Equal(t, "4", response.Details["requested"])
			require.Equal(t, "2", response.Details["ceiling"])
		})
	}
}

func TestCompileCheckConcurrencyTargets(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		kind        IteratorType
		fingerprint string
		expected    planhint.Outcome
	}{
		{name: "union", kind: UnionIteratorType, expected: planhint.Accepted},
		{name: "intersection", kind: IntersectionIteratorType, expected: planhint.Accepted},
		{name: "exclusion", kind: ExclusionIteratorType, expected: planhint.Accepted},
		{name: "unsupported", kind: ArrowIteratorType, expected: planhint.Declined},
		{name: "stale", kind: UnionIteratorType, fingerprint: "stale", expected: planhint.Invalidated},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			keys := map[OutlineNodeID]CanonicalKey{}
			root := FillMissingNodeIDs(
				Outline{Type: scenario.kind, SubOutlines: []Outline{{Type: NullIteratorType}, {Type: NullIteratorType}}},
				keys,
			)
			ledger := planhint.NewLedger(nil)
			id, err := ledger.Issue(planhint.Hint{
				Producer: "fixed", Target: planhint.Target{Node: uint64(root.ID), Fingerprint: scenario.fingerprint},
				Payload: planhint.CheckConcurrency{Limit: 3},
			})
			require.NoError(t, err)
			co := CanonicalOutline{
				Root: root, CanonicalKeys: keys, HintLedger: ledger,
				Hints: map[OutlineNodeID][]Hint{root.ID: ledger.Hints()},
			}
			compiled, err := co.Compile()
			require.NoError(t, err)
			require.Equal(t, scenario.expected, ledger.Decisions()[0].Outcome)

			if scenario.expected == planhint.Accepted {
				ctx := NewLocalContext(t.Context(), WithCheckConcurrency(3), WithHintLedger(ledger))
				ctx.MarkAsOperation(compiled, OperationCheck)
				replaced, err := compiled.ReplaceSubiterators(compiled.Subiterators())
				require.NoError(t, err)

				for _, iterator := range []Iterator{compiled, compiled.Clone(), replaced} {
					_, err := ctx.Check(iterator, Object{}, ObjectAndRelation{})
					require.NoError(t, err)
				}

				require.Equal(t, id, ledger.Decisions()[1].HintID)
			}
		})
	}

	require.Error(t, (planhint.CheckConcurrency{}).Validate())
}
