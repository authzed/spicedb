//go:build ci
// +build ci

package crdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/test"
)

func TestCRDBDatastore(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, "")
	test.All(t, test.DatastoreTesterFunc(func(revisionQuantization, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewCRDBDatastore(
				uri,
				GCWindow(gcWindow),
				RevisionQuantization(revisionQuantization),
				WatchBufferLength(watchBufferLength),
				OverlapStrategy(overlapStrategyPrefix),
			)
			require.NoError(t, err)
			return ds
		})

		return ds, nil
	}))
}

func TestCRDBDatastoreWithFollowerReads(t *testing.T) {
	followerReadDelay := time.Duration(4.8 * float64(time.Second))
	gcWindow := 100 * time.Second

	quantizationDurations := []time.Duration{
		0 * time.Second,
		100 * time.Millisecond,
	}
	for _, quantization := range quantizationDurations {
		t.Run(fmt.Sprintf("Quantization%s", quantization), func(t *testing.T) {
			require := require.New(t)

			ds := testdatastore.RunCRDBForTesting(t, "").NewDatastore(t, func(engine, uri string) datastore.Datastore {
				ds, err := NewCRDBDatastore(
					uri,
					GCWindow(gcWindow),
					RevisionQuantization(quantization),
					FollowerReadDelay(followerReadDelay),
				)
				require.NoError(err)
				return ds
			})
			defer ds.Close()

			ctx := context.Background()
			ok, err := ds.IsReady(ctx)
			require.NoError(err)
			require.True(ok)

			// Revisions should be at least the follower read delay amount in the past
			for start := time.Now(); time.Since(start) < 50*time.Millisecond; {
				testRevision, err := ds.OptimizedRevision(ctx)
				nowRevision, err := ds.HeadRevision(ctx)
				require.NoError(err)

				diff := nowRevision.IntPart() - testRevision.IntPart()
				require.True(diff > followerReadDelay.Nanoseconds())
			}
		})
	}
}
