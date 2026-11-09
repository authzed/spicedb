package datalayer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type migrationBoundaryDatastore struct {
	datastore.Datastore
	readBoundary func(context.Context) (datastore.SchemaMigrationBoundary, error)
}

func (d *migrationBoundaryDatastore) ReadSchemaMigrationBoundary(ctx context.Context) (datastore.SchemaMigrationBoundary, error) {
	return d.readBoundary(ctx)
}

func TestSchemaMigrationBoundaryFailureCanRecover(t *testing.T) {
	ctx := t.Context()
	ds := newTestDatastore(t)
	rev, err := ds.HeadRevision(ctx)
	require.NoError(t, err)
	readErr := errors.New("migration metadata unavailable")
	wrapped := &migrationBoundaryDatastore{Datastore: ds, readBoundary: func(context.Context) (datastore.SchemaMigrationBoundary, error) {
		return nil, readErr
	}}
	dl := NewDataLayer(wrapped, WithSchemaMode(SchemaModeReadNewWriteBoth))
	reader := dl.SnapshotReader(rev.Revision, NoSchemaHashInLegacyZedToken)

	// Even a legacy token must not hide a failure to determine schema authority.
	schema, err := reader.ReadSchema(ctx)
	require.ErrorIs(t, err, readErr)
	require.Nil(t, schema)

	wrapped.readBoundary = func(context.Context) (datastore.SchemaMigrationBoundary, error) {
		return datastore.SchemaMigrationBoundaryFunc(func(datastore.Revision) bool { return false }), nil
	}
	schema, err = reader.ReadSchema(ctx)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Once the immutable boundary is known, later reads do not depend on another metadata read.
	wrapped.readBoundary = func(context.Context) (datastore.SchemaMigrationBoundary, error) {
		return nil, readErr
	}
	schema, err = reader.ReadSchema(ctx)
	require.NoError(t, err)
	require.NotNil(t, schema)
}

func TestHistoricalSchemaReadPropagatesFailures(t *testing.T) {
	readErr := errors.New("historical definitions unavailable")
	for _, tc := range []struct {
		name   string
		reader *fakeLegacySchemaReader
	}{
		{name: "namespaces", reader: &fakeLegacySchemaReader{namespacesErr: readErr}},
		{name: "caveats", reader: &fakeLegacySchemaReader{caveatsErr: readErr}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := newHistoricalSchemaReader(t.Context(), tc.reader, testRevision)
			require.ErrorIs(t, err, readErr)
			require.Nil(t, schema)
		})
	}

	t.Run("corrupt caveat", func(t *testing.T) {
		reader := &fakeLegacySchemaReader{caveats: []datastore.RevisionedCaveat{{Definition: &core.CaveatDefinition{
			Name: "corrupt", SerializedExpression: []byte{0xff},
		}}}}
		schema, err := newHistoricalSchemaReader(t.Context(), reader, testRevision)
		require.ErrorContains(t, err, "invalid caveat expression bytes")
		require.Nil(t, schema)
	})
}
