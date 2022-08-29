package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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
			var writtenAt datastore.Revision
			tpl := makeTestTuple("first", "owner")
			for i := 0; i < 10; i++ {
				writtenAt, err = common.WriteTuples(ctx, ds, core.RelationTupleUpdate_TOUCH, tpl)
				require.NoError(err)
			}
			require.True(writtenAt.GreaterThan(postSetupRevision))

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
