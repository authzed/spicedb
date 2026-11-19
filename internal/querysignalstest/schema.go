package querysignalstest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// Schema compiles schema text into a stored-schema fixture with the supplied hash.
func Schema(t testing.TB, hash, text string) *datastore.ReadOnlyStoredSchema {
	t.Helper()
	c, err := compiler.Compile(
		compiler.InputSchema{Source: input.Source("signal-test"), SchemaString: text},
		compiler.AllowUnprefixedObjectType(),
	)
	require.NoError(t, err)
	namespaces := map[string]*core.NamespaceDefinition{}

	for _, d := range c.ObjectDefinitions {
		namespaces[d.Name] = d
	}

	caveats := map[string]*core.CaveatDefinition{}

	for _, d := range c.CaveatDefinitions {
		caveats[d.Name] = d
	}

	return datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaHash:           hash,
				SchemaText:           text,
				NamespaceDefinitions: namespaces,
				CaveatDefinitions:    caveats,
			},
		},
	})
}
