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

type comparisonResult uint8

const (
	equal comparisonResult = iota
	lt
	gt
	concurrent
)

func TestRevisionOrdering(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		lhsTx        uint64
		lhsXmin      int64
		rhsTx        uint64
		rhsXmin      int64
		relationship comparisonResult
	}{
		{0, 0, 0, 0, equal},
		{8, -1, 8, 5, equal},
		{8, 8, 8, -1, equal},
		{8, -1, 8, -1, equal},
		{5, 5, 5, 5, equal},
		{0, 0, 5, 5, lt},
		{0, 0, 5, -1, lt},
		{0, -1, 5, 5, lt},
		{0, -1, 5, -1, lt},
		{5, 5, 0, 0, gt},
		{5, -1, 0, 0, gt},
		{5, 5, 0, -1, gt},
		{5, -1, 0, -1, gt},
		{5, 5, 6, 5, concurrent},
		{5, -1, 6, 5, concurrent},
		{6, 5, 5, 5, concurrent},
		{6, 5, 5, -1, concurrent},
		{5, 0, 0, 0, concurrent},
		{5, 0, 0, -1, concurrent},
		{6, 5, 8, 7, lt},
		{6, 5, 8, -1, lt},
		{6, -1, 8, 7, lt},
		{6, -1, 8, -1, lt},
		{8, 7, 7, 6, concurrent},
		{8, 7, 7, -1, concurrent},
		{7, 6, 8, 7, concurrent},
		{7, -1, 8, 7, concurrent},
		{maxInt, 6, 6, 6, concurrent},
		{maxInt, int64(maxInt), maxInt - 1, int64(maxInt - 1), gt},
		{maxInt, int64(maxInt - 1), maxInt - 1, int64(maxInt - 1), concurrent},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("%d.%d:%d.%d", tc.lhsTx, tc.lhsXmin, tc.rhsTx, tc.rhsXmin), func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			lhs := testRevision(tc.lhsTx, tc.lhsXmin)
			rhs := testRevision(tc.rhsTx, tc.rhsXmin)

			require.Equal(tc.relationship == equal, lhs.Equal(rhs))
			require.Equal(tc.relationship == equal, rhs.Equal(lhs))

			require.Equal(tc.relationship == lt, lhs.LessThan(rhs))
			require.Equal(tc.relationship == gt, lhs.GreaterThan(rhs))

			require.Equal(tc.relationship == concurrent, !lhs.LessThan(rhs) && !lhs.GreaterThan(rhs) && !lhs.Equal(rhs))
		})
	}
}

func TestRevisionSerDe(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		tx          uint64
		xmin        int64
		expectedStr string
	}{
		{0, -1, "0"},
		{0, 0, "0.0"},
		{500, -1, "500"},
		{500, 499, "500.499"},
		{500, 499, "500.499"},
		{maxInt, -1, "9223372036854775807"},
		{maxInt, int64(maxInt - 1), "9223372036854775807.9223372036854775806"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("%d.%d", tc.tx, tc.xmin), func(t *testing.T) {
			t.Parallel()
			require := require.New(t)
			rev := testRevision(tc.tx, tc.xmin)
			serialized := rev.String()
			require.Equal(tc.expectedStr, serialized)

			parsed, err := parseRevision(serialized)
			require.NoError(err)
			require.Equal(rev, parsed)
		})
	}
}

func TestRevisionDeserializationErrors(t *testing.T) {
	t.Parallel()
	testCases := []string{
		"1:0",
		"-1.0",
		"-1",
		"0.-1",
		"abc",
		"0.1.2",
		"",
		"92233720368547758079223372036854775807922337203685477580792233720368547758079233720368547",
		"0.abc",
	}

	for _, tc := range testCases { // nolint: paralleltest
		t.Run(tc, func(t *testing.T) {
			_, err := parseRevision(tc)
			require.Error(t, err)
		})
	}
}

func testRevision(tx uint64, xmin int64) postgresRevision {
	revXmin := noXmin
	if xmin >= 0 {
		revXmin = xid8{Uint: uint64(xmin), Status: pgtype.Present}
	}

	return postgresRevision{xid8{tx, pgtype.Present}, revXmin}
}
