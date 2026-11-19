package querysignalstest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestRecordingKeepsRevisionAndOperationPopulationsSeparate(t *testing.T) {
	owner := querysignalstest.Owner(t, resultsize.New())
	first := querysignalstest.View(t, owner, "schema", 1)
	second := querysignalstest.View(t, owner, "schema", 2)
	node := query.Outline{Type: query.NullIteratorType}

	// Zero is a completed empty enumeration, not a missing observation.
	querysignalstest.ResultSizes(t, first, node, query.OperationIterSubjects, time.Now(), 0, 8)
	key := querysignalstest.ExecutionKey(node, query.OperationIterSubjects)
	estimate, err := signal.Lookup(first.ForPlan(), resultsize.Ref, key)
	require.NoError(t, err)
	require.True(t, estimate.IsAvailable())
	require.Equal(t, uint64(2), estimate.Value.Samples)
	require.InDelta(t, 4, estimate.Value.Mean, 0.0001)

	// Recording through a scoped writer must not leak into another revision or operation.
	otherRevision, err := signal.Lookup(second.ForPlan(), resultsize.Ref, key)
	require.NoError(t, err)
	require.False(t, otherRevision.IsAvailable())
	otherOperation, err := signal.Lookup(first.ForPlan(), resultsize.Ref,
		querysignalstest.ExecutionKey(node, query.OperationIterResources))
	require.NoError(t, err)
	require.False(t, otherOperation.IsAvailable())
}
