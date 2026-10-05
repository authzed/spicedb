package querysignalstest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// ExecutionKey describes an unfiltered, single-operation body in the default test strategy.
// Use this same builder in the optimization context and observation fixtures. Tests exercising
// filters, dispatch roles or batched execution should construct those populations explicitly.
func ExecutionKey(node query.Outline, operation query.Operation) signal.ExecutionKey {
	facts := query.DescribeOutline(node)[node.ID]
	return signal.ExecutionKey{
		Identity:     facts.Identity,
		Operation:    query.SignalOperation(operation),
		Mode:         "single",
		Strategy:     "default",
		Role:         "body",
		Dependencies: signal.EncodeFootprint(facts.Footprint),
	}
}

// Record publishes typed observations through the real signal writer and fails on rejected input.
// The writer supplies schema/revision scope; this helper does not bypass provider validation.
func Record[K comparable, O any](t testing.TB, writer signal.Writer, input signal.Input[K, O], key K, observations ...O) {
	t.Helper()

	for i, observation := range observations {
		require.NoError(t, signal.Observe(writer, input, key, observation), "record %s observation %d", input.Name, i)
	}
}

// ResultSizes records one completed enumeration per count at the supplied start time.
// Counts are raw path counts, including zero; use Record for other completion states or timestamps.
func ResultSizes(
	t testing.TB,
	writer signal.Writer,
	node query.Outline,
	operation query.Operation,
	started time.Time,
	counts ...uint64,
) {
	t.Helper()
	key := ExecutionKey(node, operation)

	for _, count := range counts {
		Record(t, writer, resultsize.Input, key, resultsize.Observation{Count: count, Started: started})
	}
}
