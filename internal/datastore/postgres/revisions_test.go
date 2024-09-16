package postgres

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/stretchr/testify/require"
)

const (
	maxInt = ^uint64(0) >> 1
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
		tc := tc
		t.Run(fmt.Sprintf("%s:%s", tc.lhsSnapshot, tc.rhsSnapshot), func(t *testing.T) {
			require := require.New(t)

			lhs := postgresRevision{snapshot: tc.lhsSnapshot}
			rhs := postgresRevision{snapshot: tc.rhsSnapshot}

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
		// i should be nonnegative
		index, _ := safecast.ToUint64(i)
		maxSizeList[i] = maxInt - uint64(len(maxSizeList)) + index
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
		tc := tc
		t.Run(tc.snapshot.String(), func(t *testing.T) {
			require := require.New(t)

			rev := postgresRevision{snapshot: tc.snapshot}
			serialized := rev.String()
			require.Equal(tc.expectedStr, serialized)

			parsed, err := parseRevisionProto(serialized)
			require.NoError(err)
			require.Equal(rev, parsed)
		})
	}
}

func TestTxIDTimestampAvailable(t *testing.T) {
	// Timestamps should be non-negative
	testTimestamp, _ := safecast.ToUint64(time.Now().Unix())
	snapshot := snap(0, 5, 1)
	pgr := postgresRevision{snapshot: snapshot, optionalTxID: newXid8(1), optionalNanosTimestamp: testTimestamp}
	receivedTimestamp, ok := pgr.OptionalNanosTimestamp()
	require.True(t, ok)
	require.Equal(t, receivedTimestamp, testTimestamp)
	txid, ok := pgr.OptionalTransactionID()
	require.True(t, ok)
	require.Equal(t, newXid8(1), txid)

	anotherRev := postgresRevision{snapshot: snapshot}
	_, ok = anotherRev.OptionalNanosTimestamp()
	require.False(t, ok)
	_, ok = anotherRev.OptionalTransactionID()
	require.False(t, ok)

	pgr.Equal(anotherRev)
}

func TestRevisionParseOldDecimalFormat(t *testing.T) {
	testCases := []struct {
		snapshot  pgSnapshot
		inputStr  string
		expectErr bool
	}{
		{snap(1, 1), "0", false},
		{snap(1, 1), "0.0", false},
		{snap(501, 501), "500", false},
		{snap(499, 501, 499), "500.499", false},
		{snap(499, 507, 499, 500, 501, 502, 503, 504, 505), "506.499", false},
		{snap(maxInt+1, maxInt+1), "9223372036854775807", false},
		{snap(maxInt-1, maxInt+1, maxInt-1), "9223372036854775807.9223372036854775806", false},
		{snap(0, 0), "-500", true},
		{snap(0, 0), "", true},
		{snap(0, 0), "deadbeef", true},
		{snap(0, 0), "dead.beef", true},
		{snap(0, 0), ".12345", true},
		{snap(0, 0), "12345.", true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.snapshot.String(), func(t *testing.T) {
			require := require.New(t)

			parsed, err := parseRevisionDecimal(tc.inputStr)
			if tc.expectErr {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(postgresRevision{snapshot: tc.snapshot}, parsed)
			}
		})
	}
}

func TestCombinedRevisionParsing(t *testing.T) {
	testCases := []struct {
		snapshot  pgSnapshot
		inputStr  string
		expectErr bool
	}{
		{snap(0, 0), "", false},
		{snap(0, 5, 1), "EAUaAQE=", false},
		{snap(1, 1), "CAE=", false},
		{snap(1, 3, 1), "CAEQAhoBAA==", false},
		{snap(1, 2, 1), "CAEQARoBAA==", false},
		{snap(2, 2), "CAI=", false},
		{snap(123, 123), "CHs=", false},
		{snap(100, 150, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110), "CGQQMhoKAQIDBAUGBwgJCg==", false},
		{snap(1, 1), "0", false},
		{snap(1, 1), "0.0", false},
		{snap(501, 501), "500", false},
		{snap(499, 501, 499), "500.499", false},
		{snap(499, 507, 499, 500, 501, 502, 503, 504, 505), "506.499", false},
		{snap(maxInt+1, maxInt+1), "9223372036854775807", false},
		{snap(maxInt-1, maxInt+1, maxInt-1), "9223372036854775807.9223372036854775806", false},
		{snap(0, 0), "-500", true},
		{snap(0, 0), "gobbleygook", true},
		{snap(0, 0), "CAEQARoBAA", true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.snapshot.String(), func(t *testing.T) {
			require := require.New(t)

			parsed, err := ParseRevisionString(tc.inputStr)
			if tc.expectErr {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(postgresRevision{snapshot: tc.snapshot}, parsed)
			}
		})
	}
}

func TestBrokenInvalidRevision(t *testing.T) {
	_, err := ParseRevisionString("1693540940373045727.0000000001")
	require.Error(t, err)
}

func FuzzRevision(f *testing.F) {
	// Attempt to find a decimal revision that is a valid base64 encoded proto revision
	f.Add(uint64(0), -1)
	f.Add(uint64(0), 0)
	f.Add(uint64(500), -1)
	f.Add(uint64(500), 499)
	f.Add(uint64(506), 499)
	f.Add(uint64(9223372036854775807), -1)
	f.Add(uint64(9223372036854775807), 9223372036854775806)

	f.Fuzz(func(t *testing.T, a uint64, b int) {
		decimalRev := strconv.FormatUint(a, 10)
		if b >= 0 {
			decimalRev = decimalRev + "." + strconv.Itoa(b)
		}
		rev, err := parseRevisionProto(decimalRev)
		if err == nil && decimalRev != "" {
			t.Errorf("decimal revision \"%s\" is a valid proto revision %#v", decimalRev, rev)
		}
	})
}
