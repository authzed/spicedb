package dispatch

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
)

func makePath(resourceID, subjectID string) query.Path {
	return query.Path{
		Resource: query.Object{ObjectType: "document", ObjectID: resourceID},
		Relation: "viewer",
		Subject: query.ObjectAndRelation{
			ObjectType: "user",
			ObjectID:   subjectID,
			Relation:   "...",
		},
	}
}

// TestDispatchIteratorPassthrough confirms the wrapper forwards Check,
// IterSubjects and IterResources untouched to its sub-iterator.
func TestDispatchIteratorPassthrough(t *testing.T) {
	paths := []query.Path{
		makePath("doc1", "alice"),
		makePath("doc2", "bob"),
	}
	sub := query.NewFixedIterator(paths...)
	d := NewDispatchIterator(sub)

	ctx := query.NewLocalContext(t.Context())

	t.Run("check hit", func(t *testing.T) {
		p, err := d.CheckImpl(ctx,
			query.Object{ObjectType: "document", ObjectID: "doc1"},
			query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
		require.NoError(t, err)
		require.NotNil(t, p)
		require.Equal(t, "doc1", p.Resource.ObjectID)
	})

	t.Run("check miss", func(t *testing.T) {
		p, err := d.CheckImpl(ctx,
			query.Object{ObjectType: "document", ObjectID: "doc1"},
			query.ObjectAndRelation{ObjectType: "user", ObjectID: "nobody", Relation: "..."})
		require.NoError(t, err)
		require.Nil(t, p)
	})

	t.Run("iter subjects", func(t *testing.T) {
		seq, err := d.IterSubjectsImpl(ctx,
			query.Object{ObjectType: "document", ObjectID: "doc1"},
			query.NoObjectFilter())
		require.NoError(t, err)
		var ids []string
		for p, err := range seq {
			require.NoError(t, err)
			ids = append(ids, p.Subject.ObjectID)
		}
		require.Equal(t, []string{"alice"}, ids)
	})

	t.Run("subiterators and replace", func(t *testing.T) {
		subs := d.Subiterators()
		require.Len(t, subs, 1)

		replacement := query.NewFixedIterator(makePath("doc3", "carol"))
		swapped, err := d.ReplaceSubiterators([]query.Iterator{replacement})
		require.NoError(t, err)

		p, err := swapped.CheckImpl(ctx,
			query.Object{ObjectType: "document", ObjectID: "doc3"},
			query.ObjectAndRelation{ObjectType: "user", ObjectID: "carol", Relation: "..."})
		require.NoError(t, err)
		require.NotNil(t, p)

		_, err = d.ReplaceSubiterators(nil)
		require.Error(t, err)
	})
}

// TestDispatchIteratorRoundtrip verifies the registered Serialize/Deserialize
// pair produces a structurally identical iterator with matching canonical keys.
func TestDispatchIteratorRoundtrip(t *testing.T) {
	sub := query.NewFixedIterator(makePath("doc1", "alice"))
	d := NewDispatchIterator(sub)

	var buf bytes.Buffer
	require.NoError(t, d.Serialize(&buf))

	got, err := query.Deserialize(&buf, &query.DeserializeContext{})
	require.NoError(t, err)
	require.Equal(t, 0, buf.Len(), "decoder must consume the entire wire payload")

	gotD, ok := got.(*DispatchIterator)
	require.True(t, ok, "expected *DispatchIterator, got %T", got)
	require.Equal(t, d.CanonicalKey(), gotD.CanonicalKey())

	ctx := query.NewLocalContext(t.Context())
	p, err := gotD.CheckImpl(ctx,
		query.Object{ObjectType: "document", ObjectID: "doc1"},
		query.ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(t, err)
	require.NotNil(t, p)
}
