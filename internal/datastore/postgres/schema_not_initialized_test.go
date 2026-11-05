//go:build datastore && postgres

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSchemaNotInitializedErrors(t *testing.T) {
	b := testdatastore.RunPostgresForTesting(t, postgresConfig.pgVersion, postgresConfig.pgbouncer)
	uri := b.NewDatabase(t)

	ds, err := newPostgresDatastore(t.Context(), uri, primaryInstanceID,
		GCInterval(disableBackgroundGC),
		WatchBufferLength(1),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	requireSchemaNotInitialized := func(t *testing.T, err error) {
		t.Helper()
		require.ErrorAs(t, err, &common.SchemaNotInitializedError{})
		require.ErrorContains(t, err, "spicedb datastore migrate")
	}

	t.Run("HeadRevision", func(t *testing.T) {
		_, err := ds.HeadRevision(t.Context())
		requireSchemaNotInitialized(t, err)
	})

	t.Run("ReadWriteTx", func(t *testing.T) {
		_, err := ds.ReadWriteTx(t.Context(), func(context.Context, datastore.ReadWriteTransaction) error {
			return nil
		})
		requireSchemaNotInitialized(t, err)
	})

	reader := ds.SnapshotReader(revisionForVersion(NewXid8(1)))

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

	t.Run("BulkLoad", func(t *testing.T) {
		conn, err := pgx.Connect(t.Context(), uri)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close(context.Background()) })

		tx, err := conn.Begin(t.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback(context.Background()) })

		source := &singleRelationshipSource{rel: tuple.MustParse("document:foo#viewer@user:tom")}
		_, err = pgxcommon.BulkLoad(t.Context(), tx, schema.TableTuple, copyCols, source)
		requireSchemaNotInitialized(t, err)
	})
}

type singleRelationshipSource struct {
	rel  tuple.Relationship
	done bool
}

func (s *singleRelationshipSource) Next(_ context.Context) (*tuple.Relationship, error) {
	if s.done {
		return nil, nil
	}
	s.done = true
	return &s.rel, nil
}
