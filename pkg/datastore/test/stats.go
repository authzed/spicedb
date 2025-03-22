package test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testfixtures"
)

const statsRetryCount = 10

func StatsTest(t *testing.T, tester DatastoreTester) {
	ctx := context.Background()
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ = testfixtures.StandardDatastoreWithData(ds, require)

	for retryCount := statsRetryCount; retryCount >= 0; retryCount-- {
		stats, err := ds.Statistics(ctx)

		// If the error contains code 3D000, the *database* is not yet available, which
		// can happen due to using follower reads in CockroachDB. We should retry in this case.
		if err != nil && strings.Contains(err.Error(), "3D000") {
			time.Sleep(5 * time.Second)
			continue
		}

		require.NoError(err)

		require.Len(stats.UniqueID, 36, "unique ID must be a valid UUID")

		if (stats.EstimatedRelationshipCount == uint64(0) || len(stats.ObjectTypeStatistics) == 0) && retryCount > 0 {
			// Sleep for a bit to get the stats table to update.
			time.Sleep(500 * time.Millisecond)
			continue
		}

		require.Len(stats.ObjectTypeStatistics, 3, "must report object stats")
		require.Greater(stats.EstimatedRelationshipCount, uint64(0), "must report some relationships")

		newStats, err := ds.Statistics(ctx)
		require.NoError(err)
		require.Equal(newStats.UniqueID, stats.UniqueID, "unique ID must be stable")
	}
}
