package postgres

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestSnapshotDecodeEncode(t *testing.T) {
	testCases := []struct {
		snapshot    string
		expected    pgSnapshot
		expectError bool
	}{
		{"415:415:", snap(415, 415), false},
		{"123:456:124,126,168", snap(123, 456, 124, 126, 168), false},

		{"", invalidSnapshot, true},
		{"123", invalidSnapshot, true},
		{"123:456", invalidSnapshot, true},
		{"123:456:789:10", invalidSnapshot, true},
		{"abc:456:124,126,168", invalidSnapshot, true},
	}

	for _, tc := range testCases {
		t.Run(tc.snapshot, func(t *testing.T) {
			require := require.New(t)

			var decoded pgSnapshot
			err := decoded.ScanText(pgtype.Text{String: tc.snapshot, Valid: true})
			if tc.expectError {
				require.Error(err)
				return
			}
			require.NoError(err)
			require.Equal(tc.expected, decoded)

			reEncoded, err := decoded.TextValue()
			require.NoError(err)
			require.Equal(tc.snapshot, reEncoded.String)
		})
	}
}

func TestMarkComplete(t *testing.T) {
	testCases := []struct {
		snapshot pgSnapshot
		toMark   uint64
		expected pgSnapshot
	}{
		{snap(0, 0), 0, snap(1, 1)},
		{snap(0, 5, 0, 3), 3, snap(0, 5, 0)},
		{snap(3, 5, 3), 3, snap(5, 5)},
		{snap(0, 0), 5, snap(0, 6, 0, 1, 2, 3, 4)},
		{snap(5, 5), 4, snap(5, 5)},
		{snap(3, 5, 4), 5, snap(4, 6, 4)},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s+%d=>%s", tc.snapshot, tc.toMark, tc.expected), func(t *testing.T) {
			require := require.New(t)
			completed := tc.snapshot.markComplete(tc.toMark)
			require.Equal(tc.expected, completed, "%s != %s", tc.expected, completed)
		})
	}
}

func TestVisible(t *testing.T) {
	testCases := []struct {
		snapshot pgSnapshot
		txID     uint64
		visible  bool
	}{
		{snap(840, 842, 840), 841, true},
		{snap(840, 842, 840), 840, false},
		{snap(840, 842, 840), 842, false},
		{snap(840, 842, 840), 839, true},
	}

	f := func(b bool) string {
		if b {
			return "visible"
		}
		return "not visible"
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d %s %s", tc.txID, f(tc.visible)+" in", tc.snapshot), func(t *testing.T) {
			require := require.New(t)
			result := tc.snapshot.txVisible(tc.txID)
			require.Equal(tc.visible, result, "expected %s but got %s", f(tc.visible), f(result))
		})
	}
}

func TestCompare(t *testing.T) {
	testCases := []struct {
		snapshot    pgSnapshot
		compareWith pgSnapshot
		result      comparisonResult
	}{
		// === Identity / equality ===
		{snap(0, 0), snap(0, 0), equal},
		{snap(1, 1), snap(1, 1), equal},
		{snap(5, 5), snap(5, 5), equal},
		{snap(100, 100), snap(100, 100), equal},
		{snap(0, 4, 2), snap(0, 4, 2), equal},
		{snap(10, 20, 12, 15, 18), snap(10, 20, 12, 15, 18), equal},

		// Same visible set expressed differently: all xipList entries fill [xmin,xmax)
		{snap(1, 1), snap(1, 5, 1, 2, 3, 4), equal},
		{snap(1, 5, 1, 2, 3, 4), snap(1, 1), equal},
		{snap(5, 5), snap(5, 8, 5, 6, 7), equal},
		{snap(5, 8, 5, 6, 7), snap(5, 5), equal},

		// === Strict ordering: one has more info, no conflict ===
		// RHS committed one more tx (tx 3)
		{snap(0, 4, 2), snap(0, 4, 2, 3), gt},
		{snap(0, 4, 2, 3), snap(0, 4, 2), lt},

		// Higher xmin means more committed
		{snap(5, 10, 7), snap(3, 10, 3, 5, 7), gt},
		{snap(3, 10, 3, 5, 7), snap(5, 10, 7), lt},

		// Higher xmax with no new in-progress means strictly more info
		{snap(1, 1), snap(1, 3), lt},
		{snap(1, 3), snap(1, 1), gt},
		{snap(5, 5), snap(5, 10), lt},
		{snap(5, 10), snap(5, 5), gt},

		// One snapshot is a strict superset of the other's knowledge
		{snap(10, 10), snap(10, 15, 12), lt},
		{snap(10, 15, 12), snap(10, 10), gt},
		{snap(10, 10), snap(10, 15), lt},
		{snap(10, 15), snap(10, 10), gt},

		// xmin advanced past the other's in-progress
		{snap(10, 10), snap(8, 10, 8, 9), gt},
		{snap(8, 10, 8, 9), snap(10, 10), lt},

		// Both see same xmax but one has fewer in-progress
		{snap(5, 10, 5, 7), snap(5, 10, 5), lt},
		{snap(5, 10, 5), snap(5, 10, 5, 7), gt},
		{snap(5, 10, 5, 6, 7, 8, 9), snap(5, 10, 5, 6, 7), lt},
		{snap(5, 10, 5, 6, 7), snap(5, 10, 5, 6, 7, 8, 9), gt},

		// One sees everything the other does plus a higher xmax range
		{snap(0, 5), snap(0, 10), lt},
		{snap(0, 10), snap(0, 5), gt},

		// === Concurrent: each knows something the other doesn't ===
		// Original bug case: LHS knows tx 100 done, RHS knows tx 102 done
		{snap(101, 101), snap(100, 104, 100, 101), concurrent},
		{snap(100, 104, 100, 101), snap(101, 101), concurrent},

		// LHS knows tx 5 committed, RHS knows tx 7 committed
		{snap(6, 8, 6, 7), snap(5, 9, 5, 8), concurrent},
		{snap(5, 9, 5, 8), snap(6, 8, 6, 7), concurrent},

		// Disjoint xmax ranges with each having committed txs the other hasn't seen
		{snap(3, 5, 4), snap(2, 7, 2, 3, 6), concurrent},
		{snap(2, 7, 2, 3, 6), snap(3, 5, 4), concurrent},

		// Each has one committed tx the other considers in-progress
		{snap(10, 14, 10, 12), snap(10, 14, 11, 13), concurrent},
		{snap(10, 14, 11, 13), snap(10, 14, 10, 12), concurrent},

		// LHS advanced xmin past RHS's in-progress, but RHS has higher xmax with committed txs
		{snap(20, 20), snap(18, 25, 18, 19), concurrent},
		{snap(18, 25, 18, 19), snap(20, 20), concurrent},

		// Both have different in-progress sets that overlap in complex ways
		{snap(5, 12, 5, 8, 10), snap(5, 12, 6, 9, 11), concurrent},
		{snap(5, 12, 6, 9, 11), snap(5, 12, 5, 8, 10), concurrent},

		// Wide gap: LHS committed low txs, RHS committed high txs
		{snap(50, 50), snap(40, 60, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49), concurrent},
		{snap(40, 60, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49), snap(50, 50), concurrent},

		// === Large xid gaps: must not iterate the range ===
		{snap(0, 4, 2), snap(1<<63, 1<<63), lt},
		{snap(1<<63, 1<<63), snap(0, 4, 2), gt},
		{snap(1, 1), snap(2, 1<<63), lt},
		{snap(2, 1<<63), snap(1, 1), gt},
		{snap(1<<62, 1<<62), snap(1<<63, 1<<63), lt},
		{snap(1<<63, 1<<63), snap(1<<62, 1<<62), gt},

		// Large gap concurrent: each has info the other doesn't
		{snap(1<<62, 1<<62), snap(1<<61, 1<<63, 1<<61), concurrent},
		{snap(1<<61, 1<<63, 1<<61), snap(1<<62, 1<<62), concurrent},

		// === Edge cases ===
		// xmin=0, xmax=0: empty snapshot
		{snap(0, 0), snap(0, 1), lt},
		{snap(0, 1), snap(0, 0), gt},

		// Single tx difference
		{snap(0, 1), snap(0, 2), lt},
		{snap(0, 2), snap(0, 1), gt},
		{snap(0, 2, 1), snap(0, 2), lt},
		{snap(0, 2), snap(0, 2, 1), gt},

		// Adjacent xmax values
		{snap(5, 6), snap(5, 7), lt},
		{snap(5, 7), snap(5, 6), gt},

		// All txs in-progress in range = same knowledge as lower xmax
		{snap(3, 3), snap(3, 6, 3, 4, 5), equal},
		{snap(3, 6, 3, 4, 5), snap(3, 3), equal},

		// One committed tx among many in-progress
		{snap(0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8), snap(0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), gt},
		{snap(0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), snap(0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8), lt},

		// markComplete scenario: snapshot after marking own tx complete
		{snap(100, 100).markComplete(100), snap(100, 100), gt},
		{snap(100, 100), snap(100, 100).markComplete(100), lt},

		// markComplete advances past other's in-progress: strictly greater
		{snap(100, 102, 100).markComplete(100), snap(100, 102, 101), gt},
		{snap(100, 102, 101), snap(100, 102, 100).markComplete(100), lt},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s %s %s", tc.snapshot, tc.result, tc.compareWith), func(t *testing.T) {
			require := require.New(t)
			result := tc.snapshot.compare(tc.compareWith)
			require.Equal(tc.result, result, "expected %s got %s", tc.result, result)
		})
	}
}

func TestMarkInProgress(t *testing.T) {
	testCases := []struct {
		snapshot pgSnapshot
		toMark   uint64
		expected pgSnapshot
	}{
		{snap(1, 1), 0, snap(0, 0)},
		{snap(0, 5, 0), 3, snap(0, 5, 0, 3)},
		{snap(5, 5), 3, snap(3, 5, 3)},
		{snap(0, 6, 0, 1, 2, 3, 4), 5, snap(0, 0)},
		{snap(5, 5), 4, snap(4, 4)},
		{snap(5, 5), 10, snap(5, 5)},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%d=>%s", tc.snapshot, tc.toMark, tc.expected), func(t *testing.T) {
			require := require.New(t)
			withInProgress := tc.snapshot.markInProgress(tc.toMark)
			require.Equal(tc.expected, withInProgress, "%s != %s", tc.expected, withInProgress)
		})
	}
}

func snap(xmin, xmax uint64, xips ...uint64) pgSnapshot {
	return pgSnapshot{
		xmin, xmax, xips,
	}
}

var invalidSnapshot = snap(0, 0)
