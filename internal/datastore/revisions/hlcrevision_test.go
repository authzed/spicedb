package revisions

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestNewForHLC(t *testing.T) {
	tcs := map[string]string{
		"1":                              "1.0000000000",
		"2":                              "2.0000000000",
		"42":                             "42.0000000000",
		"1257894000000000000":            "1257894000000000000.0000000000",
		"-1":                             "-1.0000000000",
		"1.0000000023":                   "1.0000000023",
		"1703283409994227985.0000000004": "1703283409994227985.0000000004",
		"1703283409994227985.0000000040": "1703283409994227985.0000000040",
		"1703283409994227985.0010000000": "1703283409994227985.0010000000",
		"1730898575294981085.0000000000": "1730898575294981085.0000000000",
	}

	for inputTimestamp, expectedTimestamp := range tcs {
		t.Run(inputTimestamp, func(t *testing.T) {
			d, err := decimal.NewFromString(inputTimestamp)
			require.NoError(t, err)

			rev, err := NewForHLC(d)
			require.NoError(t, err)
			revFromString, err := HLCRevisionFromString(inputTimestamp)
			require.NoError(t, err)
			require.True(t, rev.Equal(revFromString), "expected equal, got %v and %v", rev, revFromString)

			require.Equal(t, expectedTimestamp, rev.String())
			require.Equal(t, expectedTimestamp, revFromString.String())
		})
	}
}

func TestTimestampNanoSec(t *testing.T) {
	tcs := map[string]int64{
		"1":                              1,
		"2":                              2,
		"42":                             42,
		"1257894000000000000":            1257894000000000000,
		"-1":                             -1,
		"1.0000000023":                   1,
		"9223372036854775807.0000000002": 9223372036854775807,
		"1703283409994227985.0000000004": 1703283409994227985,
		"1703283409994227985.0000000040": 1703283409994227985,
	}

	for tc, nano := range tcs {
		t.Run(tc, func(t *testing.T) {
			rev, err := HLCRevisionFromString(tc)
			require.NoError(t, err)

			require.Equal(t, nano, rev.TimestampNanoSec())
		})
	}
}

func TestConstructForTimestamp(t *testing.T) {
	tcs := map[int64]string{
		1:                   "1.0000000000",
		2:                   "2.0000000000",
		42:                  "42.0000000000",
		1257894000000000000: "1257894000000000000.0000000000",
		-1:                  "-1.0000000000",
		9223372036854775807: "9223372036854775807.0000000000",
		1703283409994227985: "1703283409994227985.0000000000",
	}

	for input, output := range tcs {
		t.Run(strconv.Itoa(int(input)), func(t *testing.T) {
			rev := zeroHLC
			withTimestamp := rev.ConstructForTimestamp(input)
			require.Equal(t, output, withTimestamp.String())
			require.Equal(t, input, withTimestamp.TimestampNanoSec())
		})
	}
}

func TestInexactFloat64(t *testing.T) {
	tcs := map[string]float64{
		"1":                              1,
		"2":                              2,
		"42":                             42,
		"1257894000000000000":            1257894000000000000,
		"-1":                             -1,
		"1.0000000023":                   1.0000000023,
		"9223372036854775807.0000000002": 9223372036854775807.0000000002,
		"1703283409994227985.0000000004": 1703283409994227985.0000000004,
		"1703283409994227985.0000000040": 1703283409994227985.000000004,
		"1703283409994227985.000000004":  1703283409994227985.000000004,
		"1703283409994227985.0010":       1703283409994227985.001,
		"1703283409994227985.0010000000": 1703283409994227985.001,
		"1703283409994227985.001":        1703283409994227985.001,
	}

	for tc, floatValue := range tcs {
		t.Run(tc, func(t *testing.T) {
			rev, err := HLCRevisionFromString(tc)
			require.NoError(t, err)

			require.Equal(t, floatValue, rev.InexactFloat64()) //nolint:testifylint // this is based on a parsed value, so we expect it to be exactly equal.
		})
	}
}

func TestNewHLCForTime(t *testing.T) {
	time := time.Now()
	rev := NewHLCForTime(time)
	require.Equal(t, time.UnixNano(), rev.TimestampNanoSec())
}

func TestNoRevision(t *testing.T) {
	rev, err := HLCRevisionFromString("0")
	require.NoError(t, err)
	require.False(t, rev.Equal(datastore.NoRevision))
	require.True(t, rev.GreaterThan(datastore.NoRevision))
	require.False(t, rev.LessThan(datastore.NoRevision))
}

func TestHLCKeyEquals(t *testing.T) {
	tcs := []struct {
		left    string
		right   string
		isEqual bool
	}{
		{
			"1", "2", false,
		},
		{
			"2", "1", false,
		},
		{
			"2", "2", true,
		},
		{
			"1", "1.0000000005", false,
		},
		{
			"1.0000000001", "1.0000000001", true,
		},
		{
			"1.0000000001", "1", false,
		},
		{
			"1703283409994227985.0000000004", "1703283409994227985.0000000005", false,
		},
		{
			"1703283409994227985.0000000005", "1703283409994227985.0000000004", false,
		},
		{
			"1703283409994227985.0000000014", "1703283409994227985.0000000005", false,
		},
		{
			"1703283409994227985.0000000005", "1703283409994227985.0000000005", true,
		},
		{
			"1703283409994227985.0000000050", "1703283409994227985.0000000050", true,
		},
		{
			"1703283409994227985.0000000050", "1703283409994227985.0000000005", false,
		},
		{
			"1703283409994227985.000000005", "1703283409994227985.0000000050", true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.left+"-"+tc.right, func(t *testing.T) {
			left, err := HLCRevisionFromString(tc.left)
			require.NoError(t, err)

			right, err := HLCRevisionFromString(tc.right)
			require.NoError(t, err)

			lk := HLCKeyFunc(left)
			rk := HLCKeyFunc(right)

			require.Equal(t, tc.isEqual, lk == rk)
		})
	}
}

func TestHLCKeyLessThanFunc(t *testing.T) {
	tcs := []struct {
		left       string
		right      string
		isLessThan bool
	}{
		{
			"1", "2", true,
		},
		{
			"2", "1", false,
		},
		{
			"2", "2", false,
		},
		{
			"1", "1.0000000005", true,
		},
		{
			"1.0000000001", "1.0000000001", false,
		},
		{
			"1.0000000001", "1", false,
		},
		{
			"1703283409994227985.0000000004", "1703283409994227985.0000000005", true,
		},
		{
			"1703283409994227985.0000000005", "1703283409994227985.0000000004", false,
		},
		{
			"1703283409994227985.0000000014", "1703283409994227985.0000000005", false,
		},
		{
			"1703283409994227985.0000000005", "1703283409994227985.0000000014", true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.left+"-"+tc.right, func(t *testing.T) {
			left, err := HLCRevisionFromString(tc.left)
			require.NoError(t, err)

			right, err := HLCRevisionFromString(tc.right)
			require.NoError(t, err)

			lk := HLCKeyFunc(left)
			rk := HLCKeyFunc(right)

			require.Equal(t, tc.isLessThan, HLCKeyLessThanFunc(lk, rk))
		})
	}
}

func TestHLCFromStringError(t *testing.T) {
	tcs := map[string]string{
		"1a":    "invalid revision string",
		"1.0.0": "invalid revision string",
		"1a.0":  "invalid revision string",
		"1.0a":  "invalid revision string",
	}

	for tc, expectedErr := range tcs {
		t.Run(tc, func(t *testing.T) {
			_, err := HLCRevisionFromString(tc)
			require.ErrorContains(t, err, expectedErr)
		})
	}
}

func TestHLCToFromDecimal(t *testing.T) {
	tcs := []string{
		"1",
		"2",
		"42",
		"1257894000000000000",
		"-1",
		"1.0000000023",
		"1703283409994227985.0000000004",
		"1703283409994227985.0000000040",
		"1703283409994227985.0010000000",
	}

	for _, tc := range tcs {
		t.Run(tc, func(t *testing.T) {
			rev, err := HLCRevisionFromString(tc)
			require.NoError(t, err)

			d, err := rev.AsDecimal()
			require.NoError(t, err)

			rev2, err := NewForHLC(d)
			require.NoError(t, err)

			require.Equal(t, rev, rev2)
		})
	}
}

func TestFailsIfLogicalClockExceedsMaxUin32(t *testing.T) {
	expectedError := "received logical lock that exceeds MaxUint32 (9999999999 > 4294967295): revision \"0.9999999999\""
	require.PanicsWithValue(t, expectedError, func() {
		_, _ = HLCRevisionFromString("0.9999999999")
	})
}

// maxLogicalClockInString is the largest logical clock a revision string can carry before the
// stored value wraps: the parser adds logicalClockOffset to whatever it reads and keeps the sum in
// a uint32. Larger values wrap around and then compare as small ones - a parser quirk that predates
// sort keys, not something they introduce.
var maxLogicalClockInString = math.MaxUint32 - logicalClockOffset

// hlcFromString builds an HLCRevision the way a datastore does, by parsing the decimal form that
// CockroachDB emits - the same path NewForHLC takes. Tests about ordering use this rather than
// setting fields directly, so that the parser and the encoder are held against each other and
// cannot drift apart unnoticed.
func hlcFromString(t *testing.T, walltime int64, logical uint32) HLCRevision {
	t.Helper()

	rev, err := HLCRevisionFromString(fmt.Sprintf("%d.%010d", walltime, logical))
	require.NoError(t, err)

	return rev
}

// mustHLC builds an HLCRevision from its fields, skipping the parser. Use it only where the test is
// about what the encoder does with particular field values - TestHLCSortKeyEncodingIsStable, whose
// whole point is that these fields produce these bytes, and the randomized ordering test, which
// wants wall times the parser would be a detour to reach. Everywhere else, prefer hlcFromString.
func mustHLC(walltime int64, logical uint32) HLCRevision {
	return HLCRevision{walltime, logicalClockOffset + logical}
}

// TestHLCStringOrderDisagreesWithRevisionOrder shows why sort keys are needed: String() is a
// decimal, so it does not sort in revision order. This is what a "sort key is just String()"
// implementation would get wrong.
func TestHLCStringOrderDisagreesWithRevisionOrder(t *testing.T) {
	// 9 < 10, but "9..." starts with '9' and "10..." with '1', so the bytes say otherwise.
	nine := hlcFromString(t, 9, 0)
	ten := hlcFromString(t, 10, 0)

	require.True(t, nine.LessThan(ten))
	require.Positive(t, bytes.Compare([]byte(nine.String()), []byte(ten.String())),
		"expected String() to invert the order of %s and %s", nine, ten)
}

// TestHLCSortKeyOrderMatchesRevisionOrder is the positive counterpart: for the case above and for
// the boundary values, sort keys sort the way the revisions compare.
func TestHLCSortKeyOrderMatchesRevisionOrder(t *testing.T) {
	requireSortKeyOrder(t, []datastore.SortKeyRevision{
		hlcFromString(t, math.MinInt64, 0),
		hlcFromString(t, -10, 0),
		hlcFromString(t, -9, 0),
		hlcFromString(t, -1, maxLogicalClockInString),
		hlcFromString(t, 0, 0),
		hlcFromString(t, 0, 1),
		hlcFromString(t, 9, 0),
		hlcFromString(t, 9, 1),
		hlcFromString(t, 10, 0),
		hlcFromString(t, 100, 0),
		hlcFromString(t, 1000, 0),
		hlcFromString(t, 1739232000000000000, 0),
		hlcFromString(t, math.MaxInt64, maxLogicalClockInString),
	}, hlcSortKeyLength)
}

// TestHLCSortKeyMatchesComparatorsRandomized repeats the ordering check on random values rather
// than hand-picked ones.
func TestHLCSortKeyMatchesComparatorsRandomized(t *testing.T) {
	rng := newTestRNG()

	for range sortKeyRandomIterations {
		// Small logical clocks, so the stored value cannot overflow uint32.
		left := mustHLC(rng.Int64(), rng.Uint32N(1000))
		right := mustHLC(rng.Int64(), rng.Uint32N(1000))
		requireSortKeyAgreesWithComparators(t, left, right)
	}
}

// FuzzHLCSortKeyMatchesComparators checks the sort key's central property across the whole field
// space: for any two revisions, comparing their keys byte by byte must give the same answer as the
// revisions' own comparators.
//
// Revisions are built from their stored fields here rather than parsed, deliberately and unlike the
// ordering tests above. The property is about the encoder agreeing with the comparators for every
// (int64, uint32) pair, and the parser reaches only part of that space. It also rejects some
// well-formed strings by panicking under test, which the fuzzer would report as a crash in code
// this test is not about.
//
// Failures report the fields rather than String(), both because the fields are what reproduces a
// case and because String() panics for stored logical clocks below about 2.2e8, the zero value
// included.
//
// Without -fuzz this runs the seed corpus only, so the seeds carry the boundaries that matter;
// TestHLCSortKeyMatchesComparatorsRandomized covers breadth on every ordinary test run.
func FuzzHLCSortKeyMatchesComparators(f *testing.F) {
	seeds := []struct {
		aWall    int64
		aLogical uint32
		bWall    int64
		bLogical uint32
	}{
		// Wall time boundaries, including the sign change the encoding has to get right.
		{math.MinInt64, 0, math.MaxInt64, 0},
		{math.MinInt64, 0, math.MinInt64, math.MaxUint32},
		{-1, 0, 0, 0},
		{-1, math.MaxUint32, 1, 0},
		{-2, 0, -1, 0},
		{0, 0, 1, 0},
		{math.MaxInt64, math.MaxUint32, math.MaxInt64, 0},
		// Equal wall times, so that only the logical clock separates the pair.
		{7, 0, 7, 1},
		{7, 1, 7, math.MaxUint32},
		{7, math.MaxUint32, 7, math.MaxUint32},
		// The decimal digit-count cases that String() gets wrong.
		{9, 0, 10, 0},
		{100, 0, 1000, 0},
	}

	for _, seed := range seeds {
		f.Add(seed.aWall, seed.aLogical, seed.bWall, seed.bLogical)
	}

	f.Fuzz(func(t *testing.T, aWall int64, aLogical uint32, bWall int64, bLogical uint32) {
		a := HLCRevision{aWall, aLogical}
		b := HLCRevision{bWall, bLogical}

		cmp := bytes.Compare(a.AppendSortKey(nil), b.AppendSortKey(nil))
		switch {
		case a.LessThan(b):
			require.Negative(t, cmp, "(%d,%d) < (%d,%d), but their sort keys do not say so",
				aWall, aLogical, bWall, bLogical)
		case a.GreaterThan(b):
			require.Positive(t, cmp, "(%d,%d) > (%d,%d), but their sort keys do not say so",
				aWall, aLogical, bWall, bLogical)
		default:
			require.True(t, a.Equal(b))
			require.Zero(t, cmp, "(%d,%d) == (%d,%d), but their sort keys differ",
				aWall, aLogical, bWall, bLogical)
		}
	})
}

// TestHLCSortKeyEncodingIsStable pins the exact bytes. Sort keys may be stored durably, so changing
// any value here breaks existing data and must be deliberate.
func TestHLCSortKeyEncodingIsStable(t *testing.T) {
	tcs := []struct {
		name     string
		rev      HLCRevision
		expected []byte
	}{
		{
			"zero",
			mustHLC(0, 0),
			[]byte{0x80, 0, 0, 0, 0, 0, 0, 0, 0x48, 0x76, 0xe8, 0x00},
		},
		{
			"one nanosecond, logical one",
			mustHLC(1, 1),
			[]byte{0x80, 0, 0, 0, 0, 0, 0, 0x01, 0x48, 0x76, 0xe8, 0x01},
		},
		{
			"negative one",
			mustHLC(-1, 0),
			[]byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x48, 0x76, 0xe8, 0x00},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.rev.AppendSortKey(nil))
		})
	}
}

// TestHLCSortKeyWidthIsFixed checks that every key is the same width whatever the revision, which
// is what keeps one key from being a prefix of another.
func TestHLCSortKeyWidthIsFixed(t *testing.T) {
	require.Equal(t, 12, hlcSortKeyLength)

	for _, rev := range []HLCRevision{
		hlcFromString(t, math.MinInt64, 0),
		hlcFromString(t, -1, 0),
		hlcFromString(t, 0, 0),
		hlcFromString(t, 9, 0),
		hlcFromString(t, 1000, 5),
		hlcFromString(t, math.MaxInt64, 0),
	} {
		require.Len(t, rev.AppendSortKey(nil), hlcSortKeyLength, "revision %s", rev)
	}

	// String() has no such property: it runs from 12 bytes to 31.
	require.Len(t, hlcFromString(t, 0, 0).String(), 12)
	require.Len(t, hlcFromString(t, math.MinInt64, 0).String(), 31)
}
