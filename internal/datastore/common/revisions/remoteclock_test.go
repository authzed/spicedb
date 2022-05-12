package revisions

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestRemoteClockOptimizedRevisions(t *testing.T) {
	type timeAndExpectedRevision struct {
		unixTime int64
		expected int64
	}

	testCases := []struct {
		name              string
		followerReadDelay time.Duration
		quantization      time.Duration
		times             []timeAndExpectedRevision
	}{
		{
			"direct", 0, 0,
			[]timeAndExpectedRevision{
				{1230, 1230},
				{1231, 1231},
				{1232, 1232},
				{1233, 1233},
				{1234, 1234},
				{1235, 1235},
			},
		},
		{
			"simple quantized", 0, 5 * time.Second,
			[]timeAndExpectedRevision{
				{1230, 1230},
				{1231, 1230},
				{1232, 1230},
				{1233, 1230},
				{1234, 1230},
				{1235, 1235},
			},
		},
		{
			"simple with skew", 5 * time.Second, 5 * time.Second,
			[]timeAndExpectedRevision{
				{1230, 1225},
				{1231, 1225},
				{1232, 1225},
				{1233, 1225},
				{1234, 1225},
				{1235, 1230},
			},
		},
		{
			"skew no quantization", 5 * time.Second, 0,
			[]timeAndExpectedRevision{
				{1230, 1225},
				{1231, 1226},
				{1232, 1227},
				{1233, 1228},
				{1234, 1229},
				{1235, 1230},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			rcr := NewRemoteClockRevisions(1*time.Hour, 0, tc.followerReadDelay, tc.quantization)

			remoteClock := clock.NewMock()
			rcr.clockFn = remoteClock
			rcr.SetNowFunc(func(ctx context.Context) (datastore.Revision, error) {
				log.Debug().Stringer("now", remoteClock.Now()).Msg("current remote time")
				return decimal.NewFromInt(remoteClock.Now().UnixNano()), nil
			})

			for _, timeAndExpected := range tc.times {
				remoteClock.Set(time.Unix(timeAndExpected.unixTime, 0))

				expected := decimal.NewFromInt(timeAndExpected.expected * 1_000_000_000)

				optimized, err := rcr.OptimizedRevision(context.Background())
				require.NoError(err)
				require.True(
					expected.Equal(optimized),
					"optimized revision does not match expected: %s != %s",
					expected,
					optimized,
				)
			}
		})
	}
}

func TestRemoteClockCheckRevisions(t *testing.T) {
	testCases := []struct {
		name                string
		gcWindow            time.Duration
		currentTime         int64
		testRevisionSeconds int64
		expectError         bool
	}{
		{"now is valid", 1 * time.Hour, 12345, 12345, false},
		{"near future", 1 * time.Hour, 12345, 12346, true},
		{"far future", 1 * time.Hour, 12345, 1650599916, true},
		{"recent past", 1 * time.Hour, 12345, 12344, false},
		{"expired", 1 * time.Second, 12345, 12343, true},
		{"very old", 1 * time.Hour, 12345, 8744, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			rcr := NewRemoteClockRevisions(tc.gcWindow, 0, 0, 0)

			remoteClock := clock.NewMock()
			rcr.clockFn = remoteClock
			rcr.SetNowFunc(func(ctx context.Context) (datastore.Revision, error) {
				log.Debug().Stringer("now", remoteClock.Now()).Msg("current remote time")
				return decimal.NewFromInt(remoteClock.Now().UnixNano()), nil
			})

			remoteClock.Set(time.Unix(tc.currentTime, 0))

			testRevision := decimal.NewFromInt(tc.testRevisionSeconds * 1_000_000_000)

			err := rcr.CheckRevision(context.Background(), testRevision)
			if tc.expectError {
				require.Error(err)
			} else {
				require.NoError(err)
			}
		})
	}
}
