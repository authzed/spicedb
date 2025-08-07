package query_test

import (
	"context"
	"testing"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	ctx := &query.Context{
		Context:   context.Background(),
		Datastore: ds,
		Revision:  revision,
	}

	vande := query.NewRelationIterator("viewer_and_editor", query.NoSubRel)
	edit := query.NewRelationIterator("editor", query.NoSubRel)
	it := query.NewIntersection()
	it.AddSubIterator(vande)
	it.AddSubIterator(edit)
	it.Check(ctx, []string{"document:specialplan"}, "user:multiroleguy")
}
