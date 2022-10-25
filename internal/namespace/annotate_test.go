package namespace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestAnnotateNamespace(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	empty := ""
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
	}, &empty)
	require.NoError(err)

	lastRevision, err := ds.HeadRevision(context.Background())
	require.NoError(err)

	ts, err := NewNamespaceTypeSystem(compiled.ObjectDefinitions[0], ResolverForDatastoreReader(ds.SnapshotReader(lastRevision)))
	require.NoError(err)

	ctx := context.Background()
	vts, terr := ts.Validate(ctx)
	require.NoError(terr)

	aerr := AnnotateNamespace(vts)
	require.NoError(aerr)

	require.NotEmpty(ts.relationMap["aliased"].AliasingRelation)
	require.NotEmpty(ts.relationMap["also_aliased"].AliasingRelation)
	require.Empty(ts.relationMap["computed"].AliasingRelation)
	require.Empty(ts.relationMap["other"].AliasingRelation)

	require.NotEmpty(ts.relationMap["also_aliased"].CanonicalCacheKey)
	require.NotEmpty(ts.relationMap["aliased"].CanonicalCacheKey)
	require.NotEmpty(ts.relationMap["computed"].CanonicalCacheKey)
	require.NotEmpty(ts.relationMap["other"].CanonicalCacheKey)
}
