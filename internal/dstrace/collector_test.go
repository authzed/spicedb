package dstrace

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCollectorFromContextAbsentIsNilSafe(t *testing.T) {
	c := CollectorFromContext(context.Background())
	require.Nil(t, c)

	// A nil collector must be safe to use.
	c.Record("shape", time.Now(), time.Millisecond, 1)
	require.Nil(t, c.Queries())
}

func TestWithCollectorRecordsQueries(t *testing.T) {
	ctx, c := WithCollector(context.Background())
	require.NotNil(t, c)
	require.Same(t, c, CollectorFromContext(ctx))

	start := time.Now()
	c.Record("shape-a", start, 2*time.Millisecond, 3)
	c.Record("shape-b", start, 5*time.Millisecond, 0)

	queries := c.Queries()
	require.Len(t, queries, 2)

	require.Equal(t, "shape-a", queries[0].QueryShape)
	require.Equal(t, uint64(3), queries[0].RelationshipCount)
	require.Equal(t, 2*time.Millisecond, queries[0].Duration.AsDuration())
	require.WithinDuration(t, start, queries[0].StartTime.AsTime(), time.Microsecond)

	require.Equal(t, "shape-b", queries[1].QueryShape)
	require.Equal(t, uint64(0), queries[1].RelationshipCount)
}

func TestCollectorConcurrentRecord(t *testing.T) {
	_, c := WithCollector(context.Background())

	const goroutines = 16
	const perGoroutine = 64

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				c.Record("shape", time.Now(), time.Microsecond, 1)
			}
		}()
	}
	wg.Wait()

	require.Len(t, c.Queries(), goroutines*perGoroutine)
}

// Each child context gets its own collector, so a parent never sees a child's
// recorded queries.
func TestNestedCollectorsAreIndependent(t *testing.T) {
	parentCtx, parent := WithCollector(context.Background())
	parent.Record("parent-query", time.Now(), time.Millisecond, 1)

	childCtx, child := WithCollector(parentCtx)
	child.Record("child-query", time.Now(), time.Millisecond, 2)

	require.Len(t, parent.Queries(), 1)
	require.Equal(t, "parent-query", parent.Queries()[0].QueryShape)

	require.Same(t, child, CollectorFromContext(childCtx))
	require.Len(t, child.Queries(), 1)
	require.Equal(t, "child-query", child.Queries()[0].QueryShape)
}
