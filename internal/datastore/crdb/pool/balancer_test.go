package pool

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNodeConnectionBalancerPrune(t *testing.T) {
	tests := []struct {
		name       string
		maxConns   uint32
		nodes      []uint32
		conns      []uint32
		expectedGC []uint32
	}{
		{
			name:       "no extra, no pruning needed",
			nodes:      []uint32{1, 2, 3},
			maxConns:   9,
			conns:      []uint32{1, 1, 1, 2, 2, 2, 3, 3, 3},
			expectedGC: []uint32{},
		},
		{
			name:       "no extra, max 1",
			nodes:      []uint32{1, 2, 3},
			maxConns:   1,
			conns:      []uint32{1},
			expectedGC: []uint32{},
		},
		{
			name:       "prune 1, max 1",
			nodes:      []uint32{1, 2, 3},
			maxConns:   1,
			conns:      []uint32{1, 2},
			expectedGC: []uint32{2},
		},
		{
			name:       "no extra, max 2",
			nodes:      []uint32{1, 2, 3},
			maxConns:   2,
			conns:      []uint32{1, 2},
			expectedGC: []uint32{},
		},
		{
			name:       "prune 1, max 2",
			nodes:      []uint32{1, 2, 3},
			maxConns:   2,
			conns:      []uint32{1, 2, 3},
			expectedGC: []uint32{3},
		},
		{
			name:       "no extra, max 1 per node",
			nodes:      []uint32{1, 2, 3},
			maxConns:   3,
			conns:      []uint32{1, 2, 3},
			expectedGC: []uint32{},
		},
		{
			name:       "1 extra, max 1 per node",
			nodes:      []uint32{1, 2, 3},
			maxConns:   3,
			conns:      []uint32{1, 2, 2, 3},
			expectedGC: []uint32{2},
		},
		{
			name:       "no extra, max 2 per node",
			nodes:      []uint32{1, 2, 3},
			maxConns:   6,
			conns:      []uint32{1, 1, 2, 2, 3, 3},
			expectedGC: []uint32{},
		},
		{
			name:       "1 extra, max 2 per node",
			nodes:      []uint32{1, 2, 3},
			maxConns:   6,
			conns:      []uint32{1, 1, 2, 2, 3, 3, 3},
			expectedGC: []uint32{3},
		},
		{
			name:       "1 extra, prune 1",
			nodes:      []uint32{1, 2, 3},
			maxConns:   9,
			conns:      []uint32{1, 1, 1, 1, 2, 3},
			expectedGC: []uint32{1},
		},
		{
			name:       "2 extra, prune 1",
			nodes:      []uint32{1, 2, 3},
			maxConns:   9,
			conns:      []uint32{1, 1, 1, 1, 1, 2, 3},
			expectedGC: []uint32{1},
		},
		{
			name:       "5 extra, prune 2",
			nodes:      []uint32{1, 2, 3},
			maxConns:   9,
			conns:      []uint32{1, 1, 1, 1, 1, 1, 1, 1, 2, 3},
			expectedGC: []uint32{1, 1},
		},
		{
			name:       "7 extra, prune 3",
			nodes:      []uint32{1, 2, 3},
			maxConns:   9,
			conns:      []uint32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3},
			expectedGC: []uint32{1, 1, 1},
		},
		{
			name:     "prune 2 from each node",
			nodes:    []uint32{1, 2, 3},
			maxConns: 9,
			conns: []uint32{
				1, 1, 1, 1, 1, 1, 1, 1,
				2, 2, 2, 2, 2, 2, 2, 2,
				3, 3, 3, 3, 3, 3, 3, 3,
			},
			expectedGC: []uint32{1, 1, 2, 2, 3, 3},
		},
		{
			name:     "uneven split should fill max pool exactly",
			nodes:    []uint32{1, 2, 3},
			maxConns: 10,
			// note that node 2 gets the extra connection due to shuffling.
			// this is randomized between runs of the server but we pin
			// the seed for the tests
			conns:      []uint32{1, 1, 1, 2, 2, 2, 2, 3, 3, 3},
			expectedGC: []uint32{},
		},
		{
			name:       "uneven split should prune to max pool exactly",
			nodes:      []uint32{1, 2, 3},
			maxConns:   11,
			conns:      []uint32{1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3},
			expectedGC: []uint32{3},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tracker, err := NewNodeHealthChecker("")
			require.NoError(t, err)
			for _, n := range tt.nodes {
				tracker.healthyNodes[n] = struct{}{}
			}

			pool := NewFakePool(tt.maxConns)

			p := newNodeConnectionBalancer[*FakePoolConn[*FakeConn], *FakeConn](pool, tracker, 1*time.Minute)
			p.seed = 0
			p.rnd = rand.New(rand.NewSource(0))

			for _, n := range tt.conns {
				pool.nodeForConn[NewFakeConn()] = n
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			p.pruneConnections(ctx)
			require.Equal(t, len(tt.expectedGC), len(pool.gc))
			gcFromNodes := make([]uint32, 0, len(tt.expectedGC))
			for _, n := range pool.gc {
				gcFromNodes = append(gcFromNodes, n)
			}
			require.ElementsMatch(t, tt.expectedGC, gcFromNodes)
		})
	}
}
