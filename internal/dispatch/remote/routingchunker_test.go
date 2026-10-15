package remote

import (
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/authzed/consistent"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/frequency"
)

// ownerGroupsSamples returns the sample count and the sample sum of the owner groups histogram.
func ownerGroupsSamples(t *testing.T) (uint64, float64) {
	t.Helper()
	var m dto.Metric
	require.NoError(t, ownerGroupsPerChunkCall.Write(&m))
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

// hint returns the hint for the object key of document id and owner.
func hint(id, owner string) dispatch.RoutingHint {
	return dispatch.RoutingHint{Key: objectRoutingKey("document", id), Owner: owner}
}

// countingEstimator counts the Touch calls of each key.
type countingEstimator struct {
	frequency.Estimator

	mu      sync.Mutex
	touches map[string]int // GUARDED_BY(mu)
}

func (c *countingEstimator) Touch(key string) uint64 {
	c.mu.Lock()
	c.touches[key]++
	c.mu.Unlock()
	return c.Estimator.Touch(key)
}

func (c *countingEstimator) counts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.touches)
}

// countTouches enables spread for r with an estimator that counts touches.
func countTouches(t *testing.T, r *objectRouter) *countingEstimator {
	t.Helper()
	enableSpread(t, r, 0.5)
	counting := &countingEstimator{Estimator: r.spreadEstimator, touches: map[string]int{}}
	r.spreadEstimator = counting
	return counting
}

func TestRoutingChunker(t *testing.T) {
	for _, tc := range []struct {
		name string
		view *fakeRingView

		// nilRing removes the ring view of the router.
		nilRing bool
		ids     []string
		size    uint16
		want    []dispatch.Chunk

		// wantSamples is the number of owner group histogram samples, and wantGroups is their sum.
		wantSamples   uint64
		wantGroups    float64
		wantFallbacks float64
	}{
		{
			name: "groups by owner and splits by size",
			view: &fakeRingView{owners: map[string]string{
				"document:a": "n1", "document:b": "n2", "document:c": "n1", "document:d": "n1",
			}},
			ids:  []string{"a", "b", "c", "d"},
			size: 2,
			want: []dispatch.Chunk{
				{ResourceIDs: []string{"a", "c"}, Hint: hint("a", "n1")},
				{ResourceIDs: []string{"d"}, Hint: hint("a", "n1")},
				{ResourceIDs: []string{"b"}, Hint: hint("b", "n2")},
			},
			wantSamples: 1,
			wantGroups:  2,
		},
		{
			name: "keeps input order",
			view: &fakeRingView{owners: map[string]string{
				"document:a": "n1", "document:b": "n2", "document:c": "n1",
			}},
			ids:  []string{"c", "b", "a"},
			size: 100,
			want: []dispatch.Chunk{
				{ResourceIDs: []string{"c", "a"}, Hint: hint("c", "n1")},
				{ResourceIDs: []string{"b"}, Hint: hint("b", "n2")},
			},
			wantSamples: 1,
			wantGroups:  2,
		},
		{
			// Each chunk routes by the object key of its first ID, and no ID is dropped.
			name: "ring error falls back to first ID hints",
			view: &fakeRingView{
				owners: map[string]string{"document:a": "n1"},
				// The second lookup fails.
				err: consistent.ErrNoRing, errAfter: 2,
			},
			ids:  []string{"a", "b", "c"},
			size: 2,
			want: []dispatch.Chunk{
				{ResourceIDs: []string{"a", "b"}, Hint: hint("a", "")},
				{ResourceIDs: []string{"c"}, Hint: hint("c", "")},
			},
			wantFallbacks: 1,
		},
		{
			name:          "nil ring view is a counted fallback",
			view:          &fakeRingView{},
			nilRing:       true,
			ids:           []string{"a", "b"},
			size:          100,
			want:          []dispatch.Chunk{{ResourceIDs: []string{"a", "b"}, Hint: hint("a", "")}},
			wantFallbacks: 1,
		},
		{
			name: "empty input records no sample",
			view: &fakeRingView{},
			size: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestObjectRouter(tc.view)
			if tc.nilRing {
				r.ringView = nil
			}
			count, sum := ownerGroupsSamples(t)
			fallbacks := testutil.ToFloat64(ringFallbackTotal)

			chunks := r.Chunk(t.Context(), "document", tc.ids, tc.size)
			if len(tc.want) == 0 {
				require.Empty(t, chunks)
			} else {
				require.Equal(t, tc.want, chunks)
			}

			gotCount, gotSum := ownerGroupsSamples(t)
			require.Equal(t, count+tc.wantSamples, gotCount, "one histogram sample for each Chunk call that groups by owner")
			require.InDelta(t, sum+tc.wantGroups, gotSum, 0, "the sample is the number of owner groups")
			require.InDelta(t, fallbacks+tc.wantFallbacks, testutil.ToFloat64(ringFallbackTotal), 0)
		})
	}
}

func TestRoutingChunkerSpreadHintsResolveToChunkOwner(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h", "some-longer-document-id_with|chars=+"}
	r := newTestObjectRouter(hotOwnerView(ids))
	// A share of 0.01 and a warm estimator make every key hot.
	enableSpread(t, r, 0.01)
	warm(r, ids)

	sawBothOwners := false
	for range 20 {
		chunks := r.Chunk(t.Context(), "document", ids, 3)
		owners := map[string]bool{}
		var all []string
		for _, chunk := range chunks {
			require.LessOrEqual(t, len(chunk.ResourceIDs), 3)
			members, err := r.ringView.FindN(chunk.Hint.Key, 1)
			require.NoError(t, err)
			require.Equal(t, members[0].Key(), chunk.Hint.Owner, "the hint key resolves to the owner of the chunk")
			require.Len(t, chunk.Hint.Key, len("document:a")+2, "every key is hot, so every hint key has a salt")
			owners[chunk.Hint.Owner] = true
			all = append(all, chunk.ResourceIDs...)
		}
		slices.Sort(all)
		require.Equal(t, ids, all, "each ID is in exactly one chunk")
		sawBothOwners = sawBothOwners || len(owners) == 2
	}
	require.True(t, sawBothOwners)
}

func TestRoutingChunkerLatencyGateBlocksSpread(t *testing.T) {
	ids := []string{"a"}
	r := newGatedRouter(t, ids)
	recordLatency(r, "n0", time.Millisecond, minimumDigestCount)
	recordLatency(r, "n1", time.Millisecond, minimumDigestCount)
	recordLatency(r, "n2", 10*time.Millisecond, minimumDigestCount)
	warm(r, ids)
	chunks := r.Chunk(t.Context(), "document", ids, 100)
	require.Equal(t, []dispatch.Chunk{{ResourceIDs: ids, Hint: hint("a", "n0")}}, chunks,
		"an owner with normal latency keeps the unsalted key")
}

func TestRoutingChunkerTouchesEachKeyOnce(t *testing.T) {
	r := newTestObjectRouter(&fakeRingView{owners: map[string]string{
		"document:a": "n1", "document:b": "n2", "document:c": "n1",
	}, memberCount: 2})
	counting := countTouches(t, r)
	r.Chunk(t.Context(), "document", []string{"a", "b", "c"}, 1)
	require.Equal(t, map[string]int{"document:a": 1, "document:b": 1, "document:c": 1}, counting.counts())
}

func TestNewClusterDispatcherWithChunker(t *testing.T) {
	fb := &fakeBuilder{
		Builder: consistent.NewBuilder(xxhash.Sum64),
		view:    &fakeRingView{owners: map[string]string{"document:a": "n1"}},
	}

	d, chunker, err := NewClusterDispatcherWithChunker(&fakeCheckClient{}, newTestConn(t), ClusterDispatcherConfig{
		KeyHandler:            &keys.DirectKeyHandler{},
		ObjectAffinityRouting: true,
		HashringBuilder:       fb,
	}, nil, nil, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	require.Equal(t, []dispatch.Chunk{{ResourceIDs: []string{"a"}, Hint: hint("a", "n1")}},
		chunker.Chunk(t.Context(), "document", []string{"a"}, 100))

	off, offChunker, err := NewClusterDispatcherWithChunker(&fakeCheckClient{}, newTestConn(t), ClusterDispatcherConfig{
		KeyHandler:      &keys.DirectKeyHandler{},
		HashringBuilder: fb,
	}, nil, nil, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = off.Close() })
	require.IsType(t, &requestHashRouter{}, offChunker)
	ids := []string{"a", "b", "c"}
	require.Equal(t, dispatch.DefaultChunker.Chunk(t.Context(), "document", ids, 2),
		offChunker.Chunk(t.Context(), "document", ids, 2), "with routing off, chunking does not change")
}
