package test

import (
	"context"
	"testing"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/stretchr/testify/require"
)

// SimpleCaveatTest makes sure a caveated tuples is stored and retrieved from the datastore correctly
func SimpleCaveatTest(t *testing.T, tester DatastoreTester) {
	req := require.New(t)

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	req.NoError(err)
	defer ds.Close()

	ctx := context.Background()

	ok, err := ds.IsReady(ctx)
	req.NoError(err)
	req.True(ok)

	setupDatastore(ds, req)
	tpl := makeTestCaveatedTuple("foo", "bar", "my caveat")
	u := tuple.Create(tpl)

	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(u)
	})

	req.NoError(err)
	tRequire := testfixtures.TupleChecker{Require: req, DS: ds}

	tRequire.TupleExists(ctx, tpl, rev)
	req.NoError(err)
}
