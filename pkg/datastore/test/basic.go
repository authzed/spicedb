package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func UseAfterCloseTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	// Create the datastore.
	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	// Immediately close it.
	err = ds.Close()
	require.NoError(err)

	// Attempt to use and ensure an error is returned.
	_, err = ds.HeadRevision(context.Background())
	require.Error(err)
}
