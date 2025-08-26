package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

func TestBuildTree(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	// This stands in for the step of fetching and caching the schema locally.
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	it, err := query.BuildIteratorFromSchema(dsSchema, "document", "edit")
	require.NoError(err)

	t.Logf("\n%s", it.Explain().String())

	ctx := &query.Context{
		Context:   t.Context(),
		Datastore: ds,
		Revision:  revision,
	}

	relSeq, err := it.Check(ctx, []string{"specialplan"}, "multiroleguy")
	require.NoError(err)

	rels, err := query.CollectAll(relSeq)
	require.NoError(err)
	t.Log(rels)
}

func TestBuildTreeMultipleRelations(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for edit permission which creates a union
	it, err := query.BuildIteratorFromSchema(dsSchema, "document", "edit")
	require.NoError(err)

	explain := it.Explain()
	require.Contains(explain.String(), "Union", "edit permission should create a union iterator")

	ctx := &query.Context{
		Context:   t.Context(),
		Datastore: ds,
		Revision:  revision,
	}

	relSeq, err := it.Check(ctx, []string{"specialplan"}, "multiroleguy")
	require.NoError(err)

	rels, err := query.CollectAll(relSeq)
	require.NoError(err)
	require.NotEmpty(rels, "should find relations for edit permission")
}

func TestBuildTreeInvalidDefinition(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test with invalid definition name
	_, err = query.BuildIteratorFromSchema(dsSchema, "nonexistent", "edit")
	require.Error(err)
	require.Contains(err.Error(), "couldn't find a schema definition named `nonexistent`")

	// Test with invalid relation/permission name
	_, err = query.BuildIteratorFromSchema(dsSchema, "document", "nonexistent")
	require.Error(err)
	require.Contains(err.Error(), "couldn't find a relation or permission named `nonexistent`")
}

func TestBuildTreeSubRelations(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for a relation with subrelations
	it, err := query.BuildIteratorFromSchema(dsSchema, "document", "parent")
	require.NoError(err)

	// Should have created a relation iterator
	explain := it.Explain()
	require.NotEmpty(explain.String())

	ctx := &query.Context{
		Context:   t.Context(),
		Datastore: ds,
		Revision:  revision,
	}

	// Just test that the iterator can be executed without error
	relSeq, err := it.Check(ctx, []string{"companyplan"}, "legal")
	require.NoError(err)

	_, err = query.CollectAll(relSeq)
	require.NoError(err)
}
