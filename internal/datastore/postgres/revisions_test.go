package postgres

import (
	"fmt"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
)

const (
	maxInt = ^uint64(0) >> 1
)

func TestRevisionOrdering(t *testing.T) {
	testCases := []struct {
		lhsTx, lhsXmin, rhsTx, rhsXmin uint64
		relationship                   ComparisonResult
	}{
		{0, 0, 0, 0, Equal},
		{0, 0, 5, 5, LessThan},
		{5, 5, 0, 0, GreaterThan},
		{5, 5, 6, 5, Concurrent},
		{6, 5, 5, 5, Concurrent},
		{5, 0, 0, 0, Concurrent},
		{6, 5, 8, 7, LessThan},
		{8, 7, 7, 6, Concurrent},
		{7, 6, 8, 7, Concurrent},
		{maxInt, 6, 6, 6, Concurrent},
		{maxInt, maxInt, maxInt - 1, maxInt - 1, GreaterThan},
		{maxInt, maxInt - 1, maxInt - 1, maxInt - 1, Concurrent},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d.%d:%d.%d", tc.lhsTx, tc.lhsXmin, tc.rhsTx, tc.rhsXmin), func(t *testing.T) {
			require := require.New(t)

			lhs := revisionFromTransaction(xid8{tc.lhsTx, pgtype.Present}, xid8{tc.lhsXmin, pgtype.Present})
			require.Equal(tc.lhsXmin, uint64(xminFromRevision(lhs)))
			require.Equal(tc.lhsTx, uint64(lhs.IntPart()))

			rhs := revisionFromTransaction(xid8{tc.rhsTx, pgtype.Present}, xid8{tc.rhsXmin, pgtype.Present})
			require.Equal(tc.rhsXmin, uint64(xminFromRevision(rhs)))
			require.Equal(tc.rhsTx, uint64(rhs.IntPart()))

			require.Equal(tc.relationship, ComparePostgresRevisions(lhs, rhs))
		})
	}
}
