//go:build datastore

package crdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
)

func TestSchemaNotInitializedErrors(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, crdbTestVersion())
	uri := b.NewDatabase(t)

	ds, err := NewCRDBDatastore(t.Context(), uri, WithAcquireTimeout(5*time.Second))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	requireSchemaNotInitialized := func(t *testing.T, err error) {
		t.Helper()
		require.ErrorAs(t, err, &common.SchemaNotInitializedError{})
		require.ErrorContains(t, err, "spicedb datastore migrate")
	}

	t.Run("Features", func(t *testing.T) {
		_, err := ds.Features(t.Context())
		requireSchemaNotInitialized(t, err)
	})

	reader := ds.SnapshotReader(revisions.NewHLCForTime(time.Now()))

	t.Run("LegacyReadCaveatByName", func(t *testing.T) {
		_, _, err := reader.LegacyReadCaveatByName(t.Context(), "somecaveat")
		requireSchemaNotInitialized(t, err)
	})

	t.Run("LegacyListAllCaveats", func(t *testing.T) {
		_, err := reader.LegacyListAllCaveats(t.Context())
		requireSchemaNotInitialized(t, err)
	})

	t.Run("LegacyListAllNamespaces", func(t *testing.T) {
		_, err := reader.LegacyListAllNamespaces(t.Context())
		requireSchemaNotInitialized(t, err)
	})

	t.Run("LegacyLookupNamespacesWithNames", func(t *testing.T) {
		_, err := reader.LegacyLookupNamespacesWithNames(t.Context(), []string{"document"})
		requireSchemaNotInitialized(t, err)
	})
}
