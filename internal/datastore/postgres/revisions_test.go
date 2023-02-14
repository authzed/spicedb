package postgres

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	maxInt = int64(^uint64(0) >> 1)
)

func TestRevisionOrdering(t *testing.T) {
	testCases := []struct {
		lhsSnapshot  pgSnapshot
		rhsSnapshot  pgSnapshot
		relationship comparisonResult
	}{
		{snap(0, 0), snap(0, 0), equal},
		{snap(0, 5, 1), snap(0, 5, 1), equal},
		{snap(0, 0), snap(1, 1), lt},
		{snap(1, 1), snap(0, 0), gt},
		{snap(1, 3, 1), snap(2, 3, 2), concurrent},
		{snap(1, 2, 1), snap(2, 2), lt},
		{snap(2, 2), snap(1, 2, 1), gt},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s:%s", tc.lhsSnapshot, tc.rhsSnapshot), func(t *testing.T) {
			require := require.New(t)

			lhs := postgresRevision{tc.lhsSnapshot}
			rhs := postgresRevision{tc.rhsSnapshot}

			require.Equal(tc.relationship == equal, lhs.Equal(rhs))
			require.Equal(tc.relationship == equal, rhs.Equal(lhs))

			require.Equal(tc.relationship == lt, lhs.LessThan(rhs))
			require.Equal(tc.relationship == gt, lhs.GreaterThan(rhs))

			require.Equal(tc.relationship == concurrent, !lhs.LessThan(rhs) && !lhs.GreaterThan(rhs) && !lhs.Equal(rhs))
		})
	}
}

func TestRevisionSerDe(t *testing.T) {
	maxSizeList := make([]uint64, 20)
	for i := range maxSizeList {
		maxSizeList[i] = uint64(maxInt - int64(len(maxSizeList)) + int64(i))
	}

	testCases := []struct {
		snapshot    pgSnapshot
		expectedStr string
	}{
		{snap(0, 0), ""},
		{snap(0, 5, 1), "EAUaAQE="},
		{snap(1, 1), "CAE="},
		{snap(1, 3, 1), "CAEQAhoBAA=="},
		{snap(1, 2, 1), "CAEQARoBAA=="},
		{snap(2, 2), "CAI="},
		{snap(123, 123), "CHs="},
		{snap(100, 150, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110), "CGQQMhoKAQIDBAUGBwgJCg=="},
		{snap(maxSizeList[0], maxSizeList[len(maxSizeList)-1], maxSizeList...), "COv/////////fxATGhQAAQIDBAUGBwgJCgsMDQ4PEBESEw=="},
	}

	for _, tc := range testCases {
		t.Run(tc.snapshot.String(), func(t *testing.T) {
			require := require.New(t)

			rev := postgresRevision{tc.snapshot}
			serialized := rev.String()
			require.Equal(tc.expectedStr, serialized)

			parsed, err := parseRevision(serialized)
			require.NoError(err)
			require.Equal(rev, parsed)
		})
	}
}
