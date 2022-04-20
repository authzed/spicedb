package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// RevisionQuantizationTest tests whether or not the requirements for revisions hold
// for a particular datastore.
func RevisionQuantizationTest(t *testing.T, tester DatastoreTester) {
	testCases := []struct {
		quantizationRange        time.Duration
		expectFindLowerRevisions bool
	}{
		{0 * time.Second, false},
		{100 * time.Millisecond, true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("quantization%s", tc.quantizationRange), func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(tc.quantizationRange, veryLargeGCWindow, 1)
			require.NoError(err)

			ctx := context.Background()
			veryFirstRevision, err := ds.OptimizedRevision(ctx)
			require.NoError(err)
			require.True(veryFirstRevision.GreaterThan(decimal.Zero))

			postSetupRevision := setupDatastore(ds, require)
			require.True(postSetupRevision.GreaterThan(veryFirstRevision))

			// Create some revisions
			tpl := makeTestTuple("first", "owner")
			for i := 0; i < 10; i++ {
				_, err = ds.WriteTuples(ctx, nil, postSetupRevision, []*v1.RelationshipUpdate{{
					Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
					Relationship: tuple.MustToRelationship(tpl),
				}})
				require.NoError(err)
			}

			// Get the new now revision
			nowRevision, err := ds.HeadRevision(ctx)
			require.NoError(err)
			require.True(nowRevision.GreaterThan(datastore.NoRevision))

			// Let the quantization window expire
			time.Sleep(tc.quantizationRange)

			// Now we should ONLY get revisions later than the now revision
			for start := time.Now(); time.Since(start) < 10*time.Millisecond; {
				testRevision, err := ds.OptimizedRevision(ctx)
				require.NoError(err)
				require.True(testRevision.GreaterThanOrEqual(nowRevision))
			}
		})
	}
}
