package postgres

import (
	"fmt"
	"testing"

	"github.com/jackc/pgtype"
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
			err := decoded.DecodeText(nil, []byte(tc.snapshot))
			if tc.expectError {
				require.Error(err)
				return
			}
			require.NoError(err)
			require.Equal(tc.expected, decoded)

			reEncoded, err := decoded.EncodeText(nil, nil)
			require.NoError(err)
			require.Equal(tc.snapshot, string(reEncoded))
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
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s+%d=>%s", tc.snapshot, tc.toMark, tc.expected), func(t *testing.T) {
			require := require.New(t)
			completed := tc.snapshot.markComplete(tc.toMark)
			require.Equal(tc.expected, completed, "%s != %s", tc.expected, completed)
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
		xmin, xmax, xips, pgtype.Present,
	}
}

var invalidSnapshot = snap(0, 0)
