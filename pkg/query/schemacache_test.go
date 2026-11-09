package query

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type storedSchemaOnlyReader struct {
	datalayer.SchemaReader
	stored *datastore.ReadOnlyStoredSchema
}

func (r storedSchemaOnlyReader) StoredSchema() *datastore.ReadOnlyStoredSchema {
	return r.stored
}

func TestLoadSchemaUsesStoredViewWithoutListing(t *testing.T) {
	// The embedded reader is nil: listing definitions would panic. Single-store
	// callers already own the complete schema and should share its converted view.
	reader := storedSchemaOnlyReader{stored: datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		VersionOneof: &core.StoredSchema_V1{V1: &core.StoredSchema_V1StoredSchema{
			NamespaceDefinitions: map[string]*core.NamespaceDefinition{"user": {Name: "user"}},
		}},
	})}
	first, err := LoadSchema(t.Context(), reader)
	require.NoError(t, err)
	second, err := LoadSchema(context.Background(), reader)
	require.NoError(t, err)
	require.Same(t, first, second)
	_, exists := first.GetTypeDefinition("user")
	require.True(t, exists)
}

type legacySchemaReader struct {
	datalayer.SchemaReader
	namespaceError error
	caveatError    error
}

func (r legacySchemaReader) ListAllTypeDefinitions(context.Context) ([]datastore.RevisionedTypeDefinition, error) {
	return []datastore.RevisionedTypeDefinition{{Definition: &core.NamespaceDefinition{Name: "user"}}}, r.namespaceError
}

func (r legacySchemaReader) ListAllCaveatDefinitions(context.Context) ([]datastore.RevisionedCaveat, error) {
	return nil, r.caveatError
}

func TestLoadSchemaLegacyReader(t *testing.T) {
	view, err := LoadSchema(t.Context(), legacySchemaReader{})
	require.NoError(t, err)
	_, exists := view.GetTypeDefinition("user")
	require.True(t, exists)

	sentinel := errors.New("schema read failed")

	for _, reader := range []legacySchemaReader{{namespaceError: sentinel}, {caveatError: sentinel}} {
		view, err := LoadSchema(t.Context(), reader)
		require.ErrorIs(t, err, sentinel)
		require.Nil(t, view)
	}
}
