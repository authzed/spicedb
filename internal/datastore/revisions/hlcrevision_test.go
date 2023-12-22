package revisions

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestNewForHLC(t *testing.T) {
	tcs := []string{
		"1",
		"2",
		"42",
		"1257894000000000000",
		"-1",
		"1.23",
		"9223372036854775807.2",
		"9223372036854775807.2345987348543",
		"1703283409994227985.0000000004",
	}

	for _, tc := range tcs {
		t.Run(tc, func(t *testing.T) {
			d, err := decimal.NewFromString(tc)
			require.NoError(t, err)

			rev := NewForHLC(d)
			require.Equal(t, tc, rev.String())
		})
	}
}

func TestTimestampNanoSec(t *testing.T) {
	tcs := map[string]int64{
		"1":                                 1,
		"2":                                 2,
		"42":                                42,
		"1257894000000000000":               1257894000000000000,
		"-1":                                -1,
		"1.23":                              1,
		"9223372036854775807.2":             9223372036854775807,
		"9223372036854775807.2345987348543": 9223372036854775807,
		"1703283409994227985.0000000004":    1703283409994227985,
	}

	for tc, nano := range tcs {
		t.Run(tc, func(t *testing.T) {
			rev, err := HLCRevisionFromString(tc)
			require.NoError(t, err)

			require.Equal(t, nano, rev.TimestampNanoSec())
		})
	}
}

func TestNewHLCForTime(t *testing.T) {
	time := time.Now()
	rev := NewForTime(time)
	require.Equal(t, time.UnixNano(), rev.TimestampNanoSec())
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
			"1", "1.1", true,
		},
		{
			"1.1", "1.1", false,
		},
		{
			"1.1", "1", false,
		},
		{
			"1703283409994227985.0000000004", "1703283409994227985.0000000005", true,
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
