package namespace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/typesystem"
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

	ts, err := typesystem.NewNamespaceTypeSystem(compiled.ObjectDefinitions[0], typesystem.ResolverForDatastoreReader(ds.SnapshotReader(lastRevision)))
	require.NoError(err)

	ctx := context.Background()
	vts, terr := ts.Validate(ctx)
	require.NoError(terr)

	aerr := AnnotateNamespace(vts)
	require.NoError(aerr)

	require.NotEmpty(ts.MustGetRelation("aliased").AliasingRelation)
	require.NotEmpty(ts.MustGetRelation("also_aliased").AliasingRelation)
	require.Empty(ts.MustGetRelation("computed").AliasingRelation)
	require.Empty(ts.MustGetRelation("other").AliasingRelation)

	require.NotEmpty(ts.MustGetRelation("also_aliased").CanonicalCacheKey)
	require.NotEmpty(ts.MustGetRelation("aliased").CanonicalCacheKey)
	require.NotEmpty(ts.MustGetRelation("computed").CanonicalCacheKey)
	require.NotEmpty(ts.MustGetRelation("other").CanonicalCacheKey)
}
