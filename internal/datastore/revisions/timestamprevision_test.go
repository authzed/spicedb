package revisions

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestZeroTimestampRevision(t *testing.T) {
	require.False(t, TimestampRevision(0).LessThan(zeroTimestampRevision))
	require.True(t, TimestampRevision(0).Equal(zeroTimestampRevision))
	require.False(t, TimestampRevision(0).GreaterThan(zeroTimestampRevision))

	require.False(t, TimestampRevision(1).LessThan(zeroTimestampRevision))
	require.False(t, TimestampRevision(1).Equal(zeroTimestampRevision))
	require.True(t, TimestampRevision(1).GreaterThan(zeroTimestampRevision))
}

// TestTimestampStringOrderDisagreesWithRevisionOrder shows why sort keys are needed: String() is a
// decimal, so it does not sort in revision order. These are what a "sort key is just String()"
// implementation would get wrong.
func TestTimestampStringOrderDisagreesWithRevisionOrder(t *testing.T) {
	t.Run("digit count", func(t *testing.T) {
		nine := TimestampRevision(9)
		ten := TimestampRevision(10)

		require.True(t, nine.LessThan(ten))
		require.Positive(t, bytes.Compare([]byte(nine.String()), []byte(ten.String())))
	})

	t.Run("negative values", func(t *testing.T) {
		// A leading '-' sorts below every digit, so a negative against a positive happens to work ...
		neg := TimestampRevision(-1)
		pos := TimestampRevision(1)

		require.True(t, neg.LessThan(pos))
		require.Negative(t, bytes.Compare([]byte(neg.String()), []byte(pos.String())))

		// ... but among negatives the order flips: "-2" sorts below "-1" while -2 < -1 is what we
		// want, and "-1" below "-2" is what we get.
		require.True(t, TimestampRevision(-2).LessThan(TimestampRevision(-1)))
		require.Positive(t, bytes.Compare([]byte(TimestampRevision(-2).String()), []byte(TimestampRevision(-1).String())))
	})
}

// TestTimestampSortKeyOrderMatchesRevisionOrder is the positive counterpart: for the cases above
// and for the boundary values, sort keys sort the way the revisions compare.
func TestTimestampSortKeyOrderMatchesRevisionOrder(t *testing.T) {
	requireSortKeyOrder(t, []datastore.SortKeyRevision{
		TimestampRevision(math.MinInt64),
		TimestampRevision(-1000),
		TimestampRevision(-100),
		TimestampRevision(-10),
		TimestampRevision(-9),
		TimestampRevision(-1),
		TimestampRevision(0),
		TimestampRevision(9),
		TimestampRevision(10),
		TimestampRevision(100),
		TimestampRevision(1000),
		TimestampRevision(math.MaxInt64),
	}, timestampSortKeyLength)
}

// TestTimestampSortKeyMatchesComparatorsRandomized repeats the ordering check on random values
// rather than hand-picked ones.
func TestTimestampSortKeyMatchesComparatorsRandomized(t *testing.T) {
	rng := newTestRNG()

	for range sortKeyRandomIterations {
		requireSortKeyAgreesWithComparators(t, TimestampRevision(rng.Int64()), TimestampRevision(rng.Int64()))
	}
}

// TestTimestampSortKeyEncodingIsStable pins the exact bytes. Sort keys may be stored durably, so
// changing any value here breaks existing data and must be deliberate.
func TestTimestampSortKeyEncodingIsStable(t *testing.T) {
	tcs := []struct {
		name     string
		rev      TimestampRevision
		expected []byte
	}{
		{
			"zero",
			TimestampRevision(0),
			[]byte{0x80, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			"min",
			TimestampRevision(math.MinInt64),
			[]byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			"max",
			TimestampRevision(math.MaxInt64),
			[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.rev.AppendSortKey(nil))
		})
	}
}

// TestTimestampSortKeyWidthIsFixed checks that every key is the same width whatever the revision,
// which is what keeps one key from being a prefix of another.
func TestTimestampSortKeyWidthIsFixed(t *testing.T) {
	require.Equal(t, 8, timestampSortKeyLength)

	for _, rev := range []TimestampRevision{
		TimestampRevision(math.MinInt64),
		TimestampRevision(-1),
		TimestampRevision(0),
		TimestampRevision(9),
		TimestampRevision(1000),
		TimestampRevision(math.MaxInt64),
	} {
		require.Len(t, rev.AppendSortKey(nil), timestampSortKeyLength, "revision %s", rev)
	}

	// String() has no such property: it runs from 1 byte to 20.
	require.Len(t, TimestampRevision(0).String(), 1)
	require.Len(t, TimestampRevision(math.MinInt64).String(), 20)
}
