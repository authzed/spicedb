package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestRevisionFuzzing(t *testing.T, tester DatastoreTester) {
	testCases := []struct {
		fuzzingRange             time.Duration
		expectFindLowerRevisions bool
	}{
		{0 * time.Second, false},
		{100 * time.Millisecond, true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("fuzzing%s", tc.fuzzingRange), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(tc.fuzzingRange, veryLargeGCWindow, 1)
			require.NoError(err)

			ctx := context.Background()
			veryFirstRevision, err := ds.Revision(ctx)
			require.NoError(err)
			require.True(veryFirstRevision.GreaterThan(decimal.Zero))

			postSetupRevision := setupDatastore(ds, require)
			require.True(postSetupRevision.GreaterThan(veryFirstRevision))

			// Create some revisions
			tpl := makeTestTuple("first", "owner")
			for i := 0; i < 10; i++ {
				_, err = ds.WriteTuples(ctx, nil, []*v1.RelationshipUpdate{{
					Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
					Relationship: tuple.ToRelationship(tpl),
				}})
				require.NoError(err)
			}

			// Get the new now revision
			nowRevision, err := ds.SyncRevision(ctx)
			require.NoError(err)
			require.True(nowRevision.GreaterThan(datastore.NoRevision))

			foundLowerRevision := false
			for start := time.Now(); time.Since(start) < 20*time.Millisecond; {
				testRevision, err := ds.Revision(ctx)
				require.NoError(err)
				if testRevision.LessThan(nowRevision) {
					foundLowerRevision = true
					break
				}
			}

			require.Equal(tc.expectFindLowerRevisions, foundLowerRevision)

			// Let the fuzzing window expire
			time.Sleep(tc.fuzzingRange)

			// Now we should ONLY get revisions later than the now revision
			for start := time.Now(); time.Since(start) < 10*time.Millisecond; {
				testRevision, err := ds.Revision(ctx)
				require.NoError(err)
				require.True(testRevision.GreaterThanOrEqual(nowRevision))
			}
		})
	}
}
