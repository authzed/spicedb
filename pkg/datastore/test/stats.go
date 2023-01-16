package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testfixtures"
)

const statsRetryCount = 3

func StatsTest(t *testing.T, tester DatastoreTester) {
	ctx := context.Background()
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ = testfixtures.StandardDatastoreWithData(ds, require)

	for retryCount := statsRetryCount; retryCount >= 0; retryCount-- {
		stats, err := ds.Statistics(ctx)
		require.NoError(err)

		require.Len(stats.UniqueID, 36, "unique ID must be a valid UUID")
		require.Len(stats.ObjectTypeStatistics, 3, "must report object stats")

		if stats.EstimatedRelationshipCount == uint64(0) && retryCount > 0 {
			// Sleep for a bit to get the stats table to update.
			time.Sleep(50 * time.Millisecond)
			continue
		}

		require.Greater(stats.EstimatedRelationshipCount, uint64(0), "must report some relationships")

		newStats, err := ds.Statistics(ctx)
		require.NoError(err)
		require.Equal(newStats.UniqueID, stats.UniqueID, "unique ID must be stable")
	}
}
