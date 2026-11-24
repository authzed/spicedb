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

type countingLegacySchemaReader struct {
	*fakeLegacySchemaReader
	listings int
}

func (c *countingLegacySchemaReader) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	c.listings++
	return c.fakeLegacySchemaReader.LegacyListAllNamespaces(ctx)
}

func TestHistoricalSchemaIsMaterializedOncePerRevision(t *testing.T) {
	ctx := t.Context()
	state := newSchemaMigrationState(nil)
	reader := &countingLegacySchemaReader{fakeLegacySchemaReader: &fakeLegacySchemaReader{
		namespaces: []datastore.RevisionedNamespace{{Definition: &core.NamespaceDefinition{Name: "document"}}},
	}}

	first, err := state.historicalSchema(ctx, reader, testRevision)
	require.NoError(t, err)
	second, err := state.historicalSchema(ctx, reader, testRevision)
	require.NoError(t, err)
	require.Same(t, first, second)
	require.Equal(t, 1, reader.listings)

	_, found, err := second.LookupTypeDefByName(ctx, "document")
	require.NoError(t, err)
	require.True(t, found)
}

func TestHistoricalSchemaFailureIsNotCached(t *testing.T) {
	ctx := t.Context()
	state := newSchemaMigrationState(nil)
	readErr := errors.New("historical definitions unavailable")
	reader := &countingLegacySchemaReader{fakeLegacySchemaReader: &fakeLegacySchemaReader{namespacesErr: readErr}}

	_, err := state.historicalSchema(ctx, reader, testRevision)
	require.ErrorIs(t, err, readErr)

	reader.namespacesErr = nil
	schema, err := state.historicalSchema(ctx, reader, testRevision)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, 2, reader.listings)
}
