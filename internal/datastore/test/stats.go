package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testfixtures"
)

func StatsTest(t *testing.T, tester DatastoreTester) {
	ctx := context.Background()
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ = testfixtures.StandardDatastoreWithData(ds, require)

	stats, err := ds.Statistics(ctx)
	require.NoError(err)

	require.Len(stats.UniqueID, 36, "unique ID must be a valid UUID")
	require.Len(stats.ObjectTypeStatistics, 3, "must report object stats")
	require.Greater(stats.EstimatedRelationshipCount, uint64(0), "must report some relationships")

	newStats, err := ds.Statistics(ctx)
	require.NoError(err)
	require.Equal(newStats.UniqueID, stats.UniqueID, "unique ID must be stable")
}
