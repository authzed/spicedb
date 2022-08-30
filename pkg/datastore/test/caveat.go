package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/testfixtures"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/stretchr/testify/require"
)

// SimpleCaveatTest makes sure a caveated tuples is stored and retrieved from the datastore correctly
func SimpleCaveatTest(t *testing.T, tester DatastoreTester) {
	req := require.New(t)
	ctx := context.Background()

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	req.NoError(err)
	defer ds.Close()

	ok, err := ds.IsReady(ctx)
	req.NoError(err)
	req.True(ok)
	setupDatastore(ds, req)

	for index, test := range []struct {
		name   string
		caveat []byte
		fails  bool
	}{
		{
			"nil caveat is supported",
			nil,
			false,
		},
		{
			"empty caveat is supported",
			[]byte(""),
			false,
		},
		{
			"small caveat is supported",
			[]byte("my caveat"),
			false,
		},
		{
			"max caveat length is 4096 bytes",
			generateCaveat(4097),
			true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tpl := makeTestCaveatedTuple(fmt.Sprintf("foo-%d", index), fmt.Sprintf("bar-%d", index), test.caveat)
			rev, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl)
			if test.fails {
				req.Error(err)
			} else {
				req.NoError(err)
				tRequire := testfixtures.TupleChecker{Require: req, DS: ds}
				tRequire.TupleExists(ctx, tpl, rev)
			}
		})
	}
}

func generateCaveat(size int) []byte {
	caveat := make([]byte, size)
	rand.Read(caveat)
	return caveat
}
