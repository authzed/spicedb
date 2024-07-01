package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revisionparsing"
)

func TestCachedCheckRevision(t *testing.T) {
	ds := &fakeBrokenDatastore{checkCount: 0}

	wrapped := newCachedCheckRevision(ds)
	err := wrapped.CheckRevision(context.Background(), revisionparsing.MustParseRevisionForTest("10"))
	require.NoError(t, err)

	// Check again for the same revision, should not call the underlying datastore.
	err = wrapped.CheckRevision(context.Background(), revisionparsing.MustParseRevisionForTest("10"))
	require.NoError(t, err)

	// Check again for a lesser revision, should not call the underlying datastore.
	err = wrapped.CheckRevision(context.Background(), revisionparsing.MustParseRevisionForTest("10"))
	require.NoError(t, err)

	// Check again for a higher revision, which should call the datastore.
	err = wrapped.CheckRevision(context.Background(), revisionparsing.MustParseRevisionForTest("11"))
	require.Error(t, err)

	err = wrapped.CheckRevision(context.Background(), revisionparsing.MustParseRevisionForTest("11"))
	require.Error(t, err)

	err = wrapped.CheckRevision(context.Background(), revisionparsing.MustParseRevisionForTest("12"))
	require.Error(t, err)

	// Ensure the older revision still works.
	err = wrapped.CheckRevision(context.Background(), revisionparsing.MustParseRevisionForTest("10"))
	require.NoError(t, err)
}

type fakeBrokenDatastore struct {
	fakeDatastore
	checkCount int
}

func (f *fakeBrokenDatastore) CheckRevision(_ context.Context, _ datastore.Revision) error {
	if f.checkCount == 0 {
		f.checkCount++
		return nil
	}

	return fmt.Errorf("broken")
}
