package namespace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestAnnotateNamespace(t *testing.T) {
	require := require.New(t)

	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source: input.Source("schema"),
		SchemaString: `definition document {
	relation viewer: document
	relation editor: document

	permission aliased = viewer
	permission computed = viewer + editor
	permission other = editor - viewer
	permission also_aliased = viewer
}`,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)

	lastRevision, err := ds.HeadRevision(context.Background())
	require.NoError(err)

	ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(ds.SnapshotReader(lastRevision)))

	def, err := schema.NewDefinition(ts, compiled.ObjectDefinitions[0])
	require.NoError(err)

	ctx := context.Background()
	vdef, terr := def.Validate(ctx)
	require.NoError(terr)

	aerr := AnnotateNamespace(vdef)
	require.NoError(aerr)

	require.NotEmpty(mustGetRelation(t, def, "aliased").AliasingRelation)
	require.NotEmpty(mustGetRelation(t, def, "also_aliased").AliasingRelation)
	require.Empty(mustGetRelation(t, def, "computed").AliasingRelation)
	require.Empty(mustGetRelation(t, def, "other").AliasingRelation)

	require.NotEmpty(mustGetRelation(t, def, "also_aliased").CanonicalCacheKey)
	require.NotEmpty(mustGetRelation(t, def, "aliased").CanonicalCacheKey)
	require.NotEmpty(mustGetRelation(t, def, "computed").CanonicalCacheKey)
	require.NotEmpty(mustGetRelation(t, def, "other").CanonicalCacheKey)
}

func mustGetRelation(t testing.TB, def *schema.Definition, relationName string) *schema.Relation {
	v, ok := def.GetRelation(relationName)
	if !ok {
		t.Fatalf("Couldn't get relation %s", relationName)
	}
	return v
}
