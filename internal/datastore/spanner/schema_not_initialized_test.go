//go:build datastore

package spanner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
)

func TestSchemaNotInitializedErrors(t *testing.T) {
	b := testdatastore.RunSpannerForTesting(t)
	uri := b.NewDatabase(t)

	ds, err := NewSpannerDatastore(t.Context(), uri)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	requireSchemaNotInitialized := func(t *testing.T, err error) {
		t.Helper()
		require.ErrorAs(t, err, &common.SchemaNotInitializedError{})
		require.ErrorContains(t, err, "spicedb datastore migrate")
	}

	reader := ds.SnapshotReader(revisions.NewForTime(time.Now()))

	t.Run("LegacyReadCaveatByName", func(t *testing.T) {
		_, _, err := reader.LegacyReadCaveatByName(t.Context(), "somecaveat")
		requireSchemaNotInitialized(t, err)
	})

	t.Run("LegacyListAllCaveats", func(t *testing.T) {
		_, err := reader.LegacyListAllCaveats(t.Context())
		requireSchemaNotInitialized(t, err)
	})

	t.Run("LegacyReadNamespaceByName", func(t *testing.T) {
		_, _, err := reader.LegacyReadNamespaceByName(t.Context(), "document")
		requireSchemaNotInitialized(t, err)
	})

	t.Run("LegacyListAllNamespaces", func(t *testing.T) {
		_, err := reader.LegacyListAllNamespaces(t.Context())
		requireSchemaNotInitialized(t, err)
	})
}
