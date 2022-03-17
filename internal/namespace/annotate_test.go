package namespace

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestAnnotateNamespace(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	ctx := datastoremw.ContextWithDatastore(context.Background(), ds)
	nsm, err := NewCachingNamespaceManager(nil)
	require.NoError(err)

	empty := ""
	defs, err := compiler.Compile([]compiler.InputSchema{
		{Source: input.Source("schema"), SchemaString: `definition document {
	relation viewer: document
	relation editor: document

	permission aliased = viewer
	permission computed = viewer + editor
	permission other = editor - viewer
}`},
	}, &empty)
	require.NoError(err)

	var lastRevision decimal.Decimal
	ts, err := BuildNamespaceTypeSystemForManager(defs[0], nsm, lastRevision)
	require.NoError(err)

	terr := ts.Validate(ctx)
	require.NoError(terr)

	aerr := AnnotateNamespace(ts)
	require.NoError(aerr)

	require.NotEmpty(ts.relationMap["aliased"].AliasingRelation)
	require.Empty(ts.relationMap["computed"].AliasingRelation)
	require.Empty(ts.relationMap["other"].AliasingRelation)

	require.NotEmpty(ts.relationMap["aliased"].CanonicalCacheKey)
	require.NotEmpty(ts.relationMap["computed"].CanonicalCacheKey)
	require.NotEmpty(ts.relationMap["other"].CanonicalCacheKey)
}
