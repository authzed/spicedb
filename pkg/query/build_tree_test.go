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

func TestBuildTreeRecursion(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create a simple schema with potential recursion using group membership
	userDef := testfixtures.UserNS.CloneVT()

	groupDef := &corev1.NamespaceDefinition{
		Name: "group",
		Relation: []*corev1.Relation{
			{
				Name: "member",
				UsersetRewrite: &corev1.UsersetRewrite{
					RewriteOperation: &corev1.UsersetRewrite_Union{
						Union: &corev1.SetOperation{
							Child: []*corev1.SetOperation_Child{
								{
									ChildType: &corev1.SetOperation_Child_ComputedUserset{
										ComputedUserset: &corev1.ComputedUserset{
											Relation: "member",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	objectDefs := []*corev1.NamespaceDefinition{userDef, groupDef}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// This should detect recursion and return an error
	_, err = query.BuildIteratorFromSchema(dsSchema, "group", "member")
	require.Error(err)
	require.Contains(err.Error(), "recursive schema iterators are as yet unsupported")
}

func TestBuildTreeArrowOperation(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Test that we can detect when arrow operations would be created
	// by examining a complex permission that would include them
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// The schema conversion should successfully build the schema
	// even if we can't directly test arrow execution due to recursion
	require.NotNil(dsSchema)

	// Verify that the schema contains definitions with arrow-like operations
	docDef, ok := dsSchema.Definitions["document"]
	require.True(ok, "should find document definition")

	viewPerm, ok := docDef.Permissions["view"]
	require.True(ok, "should find view permission that contains arrow operations")
	require.NotNil(viewPerm.Operation, "view permission should have operation defined")
}

func TestBuildTreeIntersectionOperation(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for view_and_edit permission which uses intersection operations
	it, err := query.BuildIteratorFromSchema(dsSchema, "document", "view_and_edit")
	require.NoError(err)

	// Should create an intersection iterator
	explain := it.Explain()
	require.NotEmpty(explain.String())
	require.Contains(explain.String(), "Intersection", "should create intersection iterator")

	ctx := &query.Context{
		Context:   t.Context(),
		Datastore: ds,
		Revision:  revision,
	}

	// Test execution
	relSeq, err := it.Check(ctx, []string{"specialplan"}, "multiroleguy")
	require.NoError(err)

	rels, err := query.CollectAll(relSeq)
	require.NoError(err)
	t.Logf("Intersection operation results: %+v", rels)
}

func TestBuildTreeExclusionOperation(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create a simple schema to test exclusion handling
	userDef := testfixtures.UserNS.CloneVT()

	// Create a document with an exclusion operation
	docDef := &corev1.NamespaceDefinition{
		Name: "document",
		Relation: []*corev1.Relation{
			{
				Name: "excluded_perm",
				UsersetRewrite: &corev1.UsersetRewrite{
					RewriteOperation: &corev1.UsersetRewrite_Exclusion{
						Exclusion: &corev1.SetOperation{
							Child: []*corev1.SetOperation_Child{
								{
									ChildType: &corev1.SetOperation_Child_XThis{},
								},
								{
									ChildType: &corev1.SetOperation_Child_XThis{},
								},
							},
						},
					},
				},
			},
		},
	}

	objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for exclusion permission - should return ErrUnimplemented
	_, err = query.BuildIteratorFromSchema(dsSchema, "document", "excluded_perm")
	require.ErrorIs(err, query.ErrUnimplemented)
}

func TestBuildTreeArrowMissingLeftRelation(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create a schema with an arrow that references a non-existent left relation
	userDef := testfixtures.UserNS.CloneVT()

	docDef := &corev1.NamespaceDefinition{
		Name: "document",
		Relation: []*corev1.Relation{
			{
				Name: "bad_arrow",
				UsersetRewrite: &corev1.UsersetRewrite{
					RewriteOperation: &corev1.UsersetRewrite_Union{
						Union: &corev1.SetOperation{
							Child: []*corev1.SetOperation_Child{
								{
									ChildType: &corev1.SetOperation_Child_TupleToUserset{
										TupleToUserset: &corev1.TupleToUserset{
											Tupleset: &corev1.TupleToUserset_Tupleset{
												Relation: "nonexistent",
											},
											ComputedUserset: &corev1.ComputedUserset{
												Relation: "view",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for arrow with missing left relation
	_, err = query.BuildIteratorFromSchema(dsSchema, "document", "bad_arrow")
	require.Error(err)
	require.Contains(err.Error(), "couldn't find left-hand relation for arrow")
}

func TestBuildTreeSingleRelationOptimization(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for a simple relation - should not create unnecessary unions
	it, err := query.BuildIteratorFromSchema(dsSchema, "document", "owner")
	require.NoError(err)

	// Should create a simple relation iterator without extra union wrappers
	explain := it.Explain()
	require.NotEmpty(explain.String())
	require.Contains(explain.String(), "Relation", "should create relation iterator")

	ctx := &query.Context{
		Context:   t.Context(),
		Datastore: ds,
		Revision:  revision,
	}

	// Test execution
	relSeq, err := it.Check(ctx, []string{"companyplan"}, "legal")
	require.NoError(err)

	rels, err := query.CollectAll(relSeq)
	require.NoError(err)
	t.Logf("Single relation results: %+v", rels)
}
