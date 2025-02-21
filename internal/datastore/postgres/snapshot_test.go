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
		tc := tc
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
		tc := tc
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
		tc := tc

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
		{snap(0, 4, 2), snap(0, 4, 2, 3), gt},
	}

	for _, tc := range testCases {
		tc := tc
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
		tc := tc
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
