package dispatch

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

func aliasOutline(defName, relName string, child query.Outline) query.Outline {
	return query.Outline{
		Type: query.AliasIteratorType,
		Args: &query.IteratorArgs{
			DefinitionName: defName,
			RelationName:   relName,
		},
		SubOutlines: []query.Outline{child},
	}
}

func dsOutline() query.Outline {
	rel := schema.NewTestBaseRelationWithFeatures("document", "viewer", "user", "", "", false)
	return query.Outline{
		Type: query.DatastoreIteratorType,
		Args: &query.IteratorArgs{Relation: rel},
	}
}

func TestDispatchWrapCompiles(t *testing.T) {
	t.Run("optimized outline compiles to DispatchIterator above AliasIterator", func(t *testing.T) {
		input := aliasOutline("document", "viewer", dsOutline())
		co, err := query.CanonicalizeOutline(input)
		require.NoError(t, err)
		res, err := ApplyDispatchWrap(co, queryopt.RequestParams{})
		require.NoError(t, err)

		it, err := res.Compile()
		require.NoError(t, err)

		d, ok := it.(*DispatchIterator)
		require.True(t, ok, "expected root to compile to *DispatchIterator, got %T", it)
		subs := d.Subiterators()
		require.Len(t, subs, 1)
		_, ok = subs[0].(*query.AliasIterator)
		require.True(t, ok, "expected DispatchIterator child to be *query.AliasIterator, got %T", subs[0])
	})
}
