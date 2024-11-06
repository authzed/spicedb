package revisions

import (
	"strconv"
	"testing"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
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

			require.Equal(t, floatValue, rev.InexactFloat64())
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

func BenchmarkHLCParsing(b *testing.B) {
	tcs := []string{
		"1",
		"2",
		"42",
		"1257894000000000000",
		"-1",
		"9223372036854775807.1000000025",
		"1703283409994227985.0000000004",
	}

	for _, tc := range tcs {
		b.Run(tc, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := HLCRevisionFromString(tc)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkHLCLessThan(b *testing.B) {
	tcs := []struct {
		left  string
		right string
	}{
		{
			"1", "2",
		},
		{
			"2", "1",
		},
		{
			"2", "2",
		},
		{
			"1703283409994227985.0000000004", "1703283409994227985.0000000005",
		},
		{
			"1703283409994227985.0000000005", "1703283409994227985.0000000004",
		},
	}

	for _, tc := range tcs {
		b.Run(tc.left+"-"+tc.right, func(b *testing.B) {
			left, err := HLCRevisionFromString(tc.left)
			if err != nil {
				b.Fatal(err)
			}

			right, err := HLCRevisionFromString(tc.right)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			left.LessThan(right)
		})
	}
}

func BenchmarkHLCLessThanFunc(b *testing.B) {
	tcs := []struct {
		left  string
		right string
	}{
		{
			"1", "2",
		},
		{
			"2", "1",
		},
		{
			"2", "2",
		},
		{
			"1703283409994227985.0000000001", "1703283409994227985.0000000001",
		},
		{
			"1703283409994227985.0000000004", "1703283409994227985.0000000005",
		},
		{
			"1703283409994227985.0000000005", "1703283409994227985.0000000004",
		},
	}

	for _, tc := range tcs {
		b.Run(tc.left+"-"+tc.right, func(b *testing.B) {
			left, err := HLCRevisionFromString(tc.left)
			if err != nil {
				b.Fatal(err)
			}

			right, err := HLCRevisionFromString(tc.right)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			lk := HLCKeyFunc(left)
			rk := HLCKeyFunc(right)

			for i := 0; i < b.N; i++ {
				HLCKeyLessThanFunc(lk, rk)
			}
		})
	}
}
