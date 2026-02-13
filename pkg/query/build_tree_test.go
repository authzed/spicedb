package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/namespace"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

func TestBuildTree(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	// This stands in for the step of fetching and caching the schema locally.
	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	it, err := BuildIteratorFromSchema(dsSchema, "document", "edit")
	require.NoError(err)

	ctx := NewLocalContext(t.Context(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(revision)))

	relSeq, err := ctx.Check(it, NewObjects("document", "specialplan"), NewObject("user", "multiroleguy").WithEllipses())
	require.NoError(err)

	_, err = CollectAll(relSeq)
	require.NoError(err)
}

func TestBuildTreeMultipleRelations(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for edit permission which creates a union
	it, err := BuildIteratorFromSchema(dsSchema, "document", "edit")
	require.NoError(err)

	explain := it.Explain()
	require.Contains(explain.String(), "Union", "edit permission should create a union iterator")

	ctx := NewLocalContext(t.Context(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(revision)))

	relSeq, err := ctx.Check(it, NewObjects("document", "specialplan"), NewObject("user", "multiroleguy").WithEllipses())
	require.NoError(err)

	rels, err := CollectAll(relSeq)
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
	_, err = BuildIteratorFromSchema(dsSchema, "nonexistent", "edit")
	require.Error(err)
	require.Contains(err.Error(), "couldn't find a schema definition named `nonexistent`")

	// Test with invalid relation/permission name
	_, err = BuildIteratorFromSchema(dsSchema, "document", "nonexistent")
	require.Error(err)
	require.Contains(err.Error(), "couldn't find a relation or permission named `nonexistent`")
}

func TestBuildTreeSubRelations(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for a relation with subrelations
	it, err := BuildIteratorFromSchema(dsSchema, "document", "parent")
	require.NoError(err)

	// Should have created a relation iterator
	explain := it.Explain()
	require.NotEmpty(explain.String())

	ctx := NewLocalContext(t.Context(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(revision)))

	// Just test that the iterator can be executed without error
	relSeq, err := ctx.Check(it, NewObjects("document", "companyplan"), NewObject("user", "legal").WithEllipses())
	require.NoError(err)

	_, err = CollectAll(relSeq)
	require.NoError(err)
}

func TestBuildTreeRecursion(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create a proper recursive group hierarchy schema:
	// definition group {
	//    relation parent: group
	//    permission member = parent->member
	// }
	// This creates recursion: computing member arrows through parent groups,
	// which recursively compute their own member permission
	groupDef := namespace.Namespace("group",
		namespace.MustRelation("parent", nil,
			namespace.AllowedRelation("group", "..."),
		),
		namespace.MustRelation("member",
			namespace.Union(
				namespace.TupleToUserset("parent", "member"),
			),
		),
	)

	objectDefs := []*corev1.NamespaceDefinition{groupDef}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// This should detect recursion and create a RecursiveIterator
	// The arrow operation parent->member creates recursion: group->parent->member->parent->member...
	it, err := BuildIteratorFromSchema(dsSchema, "group", "member")
	require.NoError(err)
	require.NotNil(it)

	// Verify it's wrapped in a RecursiveIterator
	_, isRecursive := it.(*RecursiveIterator)
	require.True(isRecursive, "Expected RecursiveIterator for recursive arrow operation")

	// Verify the explain output
	explain := it.Explain()
	require.Equal("Recursive", explain.Name)
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
	docDef, ok := dsSchema.GetTypeDefinition("document")
	require.True(ok, "should find document definition")

	viewPerm, ok := docDef.GetPermission("view")
	require.True(ok, "should find view permission that contains arrow operations")
	require.NotNil(viewPerm.Operation, "view permission should have operation defined")
}

func TestBuildTreeIntersectionOperation(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for view_and_edit permission which uses intersection operations
	it, err := BuildIteratorFromSchema(dsSchema, "document", "view_and_edit")
	require.NoError(err)

	// Should create an intersection iterator
	explain := it.Explain()
	require.NotEmpty(explain.String())
	require.Contains(explain.String(), "Intersection", "should create intersection iterator")

	ctx := NewLocalContext(t.Context(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(revision)))

	// Test execution
	relSeq, err := ctx.Check(it, NewObjects("document", "specialplan"), NewObject("user", "multiroleguy").WithEllipses())
	require.NoError(err)

	_, err = CollectAll(relSeq)
	require.NoError(err)
}

func TestBuildTreeExclusionOperation(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create a simple schema to test exclusion handling
	userDef := testfixtures.UserNS.CloneVT()

	// Create a document with an exclusion operation
	docDef := namespace.Namespace("document",
		namespace.MustRelation("excluded_perm",
			namespace.Exclusion(
				namespace.Nil(),
				namespace.Nil(),
			),
		),
	)

	objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for exclusion permission - should succeed
	it, err := BuildIteratorFromSchema(dsSchema, "document", "excluded_perm")
	require.NoError(err)
	require.NotNil(it)
	// Should be wrapped in an Alias
	require.IsType(&AliasIterator{}, it, "Expected Alias wrapper")
	alias := it.(*AliasIterator)
	require.IsType(&ExclusionIterator{}, alias.subIt)

	// Verify the explain shows alias structure with exclusion underneath
	explain := it.Explain()
	require.Contains(explain.Info, "Alias(excluded_perm)")
	require.Len(explain.SubExplain, 1, "Should have one sub-iterator (the exclusion)")
	require.Equal("Exclusion", explain.SubExplain[0].Info)
	require.Len(explain.SubExplain[0].SubExplain, 2, "Should have main and excluded sub-iterators")

	// Verify that at least one of the sub-iterators is a FixedIterator (representing _nil)
	explainStr := explain.String()
	require.Contains(explainStr, "Fixed")
}

func TestBuildTreeExclusionEdgeCases(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	ctx := NewLocalContext(t.Context(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(revision)))

	userDef := testfixtures.UserNS.CloneVT()

	t.Run("Exclusion with Relation Reference", func(t *testing.T) {
		t.Parallel()
		// Create schema with exclusion using relation references
		docDef := namespace.Namespace("document",
			namespace.MustRelation("owner", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("viewer", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("can_view",
				namespace.Exclusion(
					namespace.ComputedUserset("viewer"),
					namespace.ComputedUserset("owner"),
				),
			),
		)

		objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
		dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
		require.NoError(err)

		it, err := BuildIteratorFromSchema(dsSchema, "document", "can_view")
		require.NoError(err)
		require.NotNil(it)
		// Should be wrapped in an Alias
		require.IsType(&AliasIterator{}, it, "Expected Alias wrapper")
		alias := it.(*AliasIterator)
		require.IsType(&ExclusionIterator{}, alias.subIt)

		// Test execution doesn't crash
		relSeq, err := ctx.Check(it, []Object{NewObject("document", "test_doc")}, NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		_, err = CollectAll(relSeq)
		require.NoError(err)
	})

	t.Run("Exclusion with Union Operations", func(t *testing.T) {
		t.Parallel()
		// Create schema with exclusion containing union operations
		docDef := namespace.Namespace("document",
			namespace.MustRelation("owner", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("editor", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("viewer", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("restricted_viewers",
				namespace.Exclusion(
					namespace.Rewrite(
						namespace.Union(
							namespace.ComputedUserset("viewer"),
							namespace.ComputedUserset("editor"),
						),
					),
					namespace.ComputedUserset("owner"),
				),
			),
		)

		objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
		dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
		require.NoError(err)

		it, err := BuildIteratorFromSchema(dsSchema, "document", "restricted_viewers")
		require.NoError(err)
		require.NotNil(it)
		// Should be wrapped in an Alias
		require.IsType(&AliasIterator{}, it, "Expected Alias wrapper")
		alias := it.(*AliasIterator)
		require.IsType(&ExclusionIterator{}, alias.subIt)

		// Verify the structure includes union in main set
		explain := it.Explain()
		require.Contains(explain.Info, "Alias(restricted_viewers)")
		require.Len(explain.SubExplain, 1, "Should have one sub-iterator (the exclusion)")
		require.Equal("Exclusion", explain.SubExplain[0].Info)
		require.Len(explain.SubExplain[0].SubExplain, 2)
		explainStr := explain.String()
		require.Contains(explainStr, "Union")
	})

	t.Run("Exclusion with Intersection Operations", func(t *testing.T) {
		t.Parallel()
		// Create schema with exclusion containing intersection operations
		docDef := namespace.Namespace("document",
			namespace.MustRelation("owner", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("editor", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("restricted_view",
				namespace.Exclusion(
					namespace.Rewrite(
						namespace.Intersection(
							namespace.ComputedUserset("editor"),
							namespace.ComputedUserset("owner"),
						),
					),
					namespace.Nil(),
				),
			),
		)

		objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
		dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
		require.NoError(err)

		it, err := BuildIteratorFromSchema(dsSchema, "document", "restricted_view")
		require.NoError(err)
		require.NotNil(it)
		// Should be wrapped in an Alias
		require.IsType(&AliasIterator{}, it, "Expected Alias wrapper")
		alias := it.(*AliasIterator)
		require.IsType(&ExclusionIterator{}, alias.subIt)

		// Verify the structure includes intersection in main set
		explain := it.Explain()
		require.Contains(explain.Info, "Alias(restricted_view)")
		require.Len(explain.SubExplain, 1, "Should have one sub-iterator (the exclusion)")
		require.Equal("Exclusion", explain.SubExplain[0].Info)
		require.Len(explain.SubExplain[0].SubExplain, 2)
		explainStr := explain.String()
		require.Contains(explainStr, "Intersection")
	})

	t.Run("Nested Exclusion Operations", func(t *testing.T) {
		t.Parallel()
		// Create schema with nested exclusions
		docDef := namespace.Namespace("document",
			namespace.MustRelation("all_users", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("banned_users", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("restricted_users", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("allowed_users",
				namespace.Exclusion(
					namespace.Rewrite(
						namespace.Exclusion(
							namespace.ComputedUserset("all_users"),
							namespace.ComputedUserset("banned_users"),
						),
					),
					namespace.ComputedUserset("restricted_users"),
				),
			),
		)

		objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
		dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
		require.NoError(err)

		it, err := BuildIteratorFromSchema(dsSchema, "document", "allowed_users")
		require.NoError(err)
		require.NotNil(it)
		// Should be wrapped in an Alias
		require.IsType(&AliasIterator{}, it, "Expected Alias wrapper")
		alias := it.(*AliasIterator)
		require.IsType(&ExclusionIterator{}, alias.subIt)

		// Verify nested structure
		explain := it.Explain()
		require.Contains(explain.Info, "Alias(allowed_users)")
		require.Len(explain.SubExplain, 1, "Should have one sub-iterator (the exclusion)")
		require.Equal("Exclusion", explain.SubExplain[0].Info)
		require.Len(explain.SubExplain[0].SubExplain, 2)

		// The first sub-explain should be another exclusion
		mainSetExplain := explain.SubExplain[0]
		require.Equal("Exclusion", mainSetExplain.Info)
	})

	t.Run("Exclusion with Error in Left Operation", func(t *testing.T) {
		t.Parallel()
		// Create schema with exclusion where left operation references non-existent relation
		docDef := namespace.Namespace("document",
			namespace.MustRelation("viewer", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("bad_exclusion",
				namespace.Exclusion(
					namespace.ComputedUserset("nonexistent_relation"),
					namespace.ComputedUserset("viewer"),
				),
			),
		)

		objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
		dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
		require.NoError(err)

		// Building iterator should fail due to missing relation
		_, err = BuildIteratorFromSchema(dsSchema, "document", "bad_exclusion")
		require.Error(err)
		require.Contains(err.Error(), "couldn't find a relation or permission named `nonexistent_relation`")
	})

	t.Run("Exclusion with Error in Right Operation", func(t *testing.T) {
		t.Parallel()
		// Create schema with exclusion where right operation references non-existent relation
		docDef := namespace.Namespace("document",
			namespace.MustRelation("viewer", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("bad_exclusion",
				namespace.Exclusion(
					namespace.ComputedUserset("viewer"),
					namespace.ComputedUserset("nonexistent_relation"),
				),
			),
		)

		objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
		dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
		require.NoError(err)

		// Building iterator should fail due to missing relation
		_, err = BuildIteratorFromSchema(dsSchema, "document", "bad_exclusion")
		require.Error(err)
		require.Contains(err.Error(), "couldn't find a relation or permission named `nonexistent_relation`")
	})
}

func TestBuildTreeArrowMissingLeftRelation(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create a schema with an arrow that references a non-existent left relation
	userDef := testfixtures.UserNS.CloneVT()

	docDef := namespace.Namespace("document",
		namespace.MustRelation("bad_arrow",
			namespace.Union(
				namespace.TupleToUserset("nonexistent", "view"),
			),
		),
	)

	objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for arrow with missing left relation
	_, err = BuildIteratorFromSchema(dsSchema, "document", "bad_arrow")
	require.Error(err)
	require.Contains(err.Error(), "couldn't find left-hand relation for arrow")
}

func TestBuildTreeSingleRelationOptimization(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	objectDefs := []*corev1.NamespaceDefinition{testfixtures.UserNS.CloneVT(), testfixtures.FolderNS.CloneVT(), testfixtures.DocumentNS.CloneVT()}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Test building iterator for a simple relation - should not create unnecessary unions
	it, err := BuildIteratorFromSchema(dsSchema, "document", "owner")
	require.NoError(err)

	// Should create a simple relation iterator without extra union wrappers
	explain := it.Explain()
	require.NotEmpty(explain.String())
	require.Contains(explain.String(), "Datastore", "should create datastore iterator")

	ctx := NewLocalContext(t.Context(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(revision)))

	// Test execution
	relSeq, err := ctx.Check(it, NewObjects("document", "companyplan"), NewObject("user", "legal").WithEllipses())
	require.NoError(err)

	_, err = CollectAll(relSeq)
	require.NoError(err)
}

func TestBuildTreeSubrelationHandling(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	ctx := NewLocalContext(t.Context(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(revision)))

	userDef := testfixtures.UserNS.CloneVT()

	t.Run("Base Relation with Ellipsis Subrelation", func(t *testing.T) {
		t.Parallel()
		// Test that base relations with ellipsis (group:...) work correctly with arrows
		groupDef := namespace.Namespace("group",
			namespace.MustRelation("member", nil, namespace.AllowedRelation("user", "...")),
		)

		docDef := namespace.Namespace("document",
			namespace.MustRelation("parent", nil, namespace.AllowedRelation("group", "...")), // Ellipsis on group
			namespace.MustRelation("viewer",
				namespace.Union(
					namespace.TupleToUserset("parent", "member"), // Arrow to group's member relation
				),
			),
		)

		objectDefs := []*corev1.NamespaceDefinition{userDef, groupDef, docDef}
		dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
		require.NoError(err)

		// Should create an alias wrapping union with arrow
		it, err := BuildIteratorFromSchema(dsSchema, "document", "viewer")
		require.NoError(err)
		require.NotNil(it)

		// Verify structure has arrow operation
		explain := it.Explain()
		explainStr := explain.String()
		require.Contains(explainStr, "Arrow", "Expected arrow operation for tuple-to-userset")

		// Test execution doesn't crash
		relSeq, err := ctx.Check(it, []Object{NewObject("document", "test_doc")}, NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		_, err = CollectAll(relSeq)
		require.NoError(err)
	})

	t.Run("Base Relation with Specific Subrelation", func(t *testing.T) {
		t.Parallel()
		// Create schema with specific subrelation that should create union with arrow
		groupDef := namespace.Namespace("group",
			namespace.MustRelation("member", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("admin", nil, namespace.AllowedRelation("user", "...")),
		)

		docDef := namespace.Namespace("document",
			namespace.MustRelation("parent", nil, namespace.AllowedRelation("group", "...")),
			namespace.MustRelation("viewer",
				namespace.Union(
					namespace.TupleToUserset("parent", "admin"), // This should create arrow from parent to admin
				),
			),
		)

		objectDefs := []*corev1.NamespaceDefinition{userDef, groupDef, docDef}
		dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
		require.NoError(err)

		it, err := BuildIteratorFromSchema(dsSchema, "document", "viewer")
		require.NoError(err)
		require.NotNil(it)

		// Note: After canonicalization, single-element unions are collapsed,
		// so we don't check for specific structure, just that execution works

		// Test execution doesn't crash
		relSeq, err := ctx.Check(it, []Object{NewObject("document", "test_doc")}, NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		_, err = CollectAll(relSeq)
		require.NoError(err)
	})

	t.Run("Base Relation Without Subrelations Disabled", func(t *testing.T) {
		t.Parallel()
		// Test base relation iterator with withSubRelations = false
		// This hits the buildBaseDatastoreIterator path where subrelations are disabled
		docDef := namespace.Namespace("document",
			namespace.MustRelation("parent", nil, namespace.AllowedRelation("document", "...")),
			namespace.MustRelation("viewer",
				namespace.Union(
					namespace.TupleToUserset("parent", "viewer"), // Arrow operation disables subrelations
				),
			),
		)

		objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
		dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
		require.NoError(err)

		// Should create RecursiveIterator for arrow recursion
		it, err := BuildIteratorFromSchema(dsSchema, "document", "viewer")
		require.NoError(err)
		require.NotNil(it)

		// Should be wrapped in RecursiveIterator
		_, isRecursive := it.(*RecursiveIterator)
		require.True(isRecursive, "Expected RecursiveIterator for arrow recursion")
	})

	t.Run("Base Relation with Missing Subrelation Definition", func(t *testing.T) {
		t.Parallel()
		// Create schema where base relation references a subrelation that doesn't exist in target
		groupDef := namespace.Namespace("group",
			namespace.MustRelation("member", nil, namespace.AllowedRelation("user", "...")),
			// Missing "nonexistent" relation
		)

		docDef := namespace.Namespace("document",
			namespace.MustRelation("parent", nil, namespace.AllowedRelation("group", "...")),
			namespace.MustRelation("viewer",
				namespace.Union(
					namespace.TupleToUserset("parent", "nonexistent"), // References non-existent relation
				),
			),
		)

		objectDefs := []*corev1.NamespaceDefinition{userDef, groupDef, docDef}
		dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
		require.NoError(err)

		// Should fail when trying to build iterator due to missing subrelation
		_, err = BuildIteratorFromSchema(dsSchema, "document", "viewer")
		require.Error(err)
		require.Contains(err.Error(), "couldn't find a relation or permission named `nonexistent`")
	})

	t.Run("Multiple Base Relations with Different Subrelation Handling", func(t *testing.T) {
		t.Parallel()
		// Test relation with multiple base relations, some with subrelations, some without
		groupDef := namespace.Namespace("group",
			namespace.MustRelation("member", nil, namespace.AllowedRelation("user", "...")),
			namespace.MustRelation("admin", nil, namespace.AllowedRelation("user", "...")),
		)

		docDef := namespace.Namespace("document",
			namespace.MustRelation("owner", nil, namespace.AllowedRelation("user", "...")), // Simple relation without subrelations
			namespace.MustRelation("parent", nil, namespace.AllowedRelation("group", "...")),
			namespace.MustRelation("viewer",
				namespace.Union(
					namespace.ComputedUserset("owner"),          // Direct relation
					namespace.TupleToUserset("parent", "admin"), // Arrow with subrelation
				),
			),
		)

		objectDefs := []*corev1.NamespaceDefinition{userDef, groupDef, docDef}
		dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
		require.NoError(err)

		it, err := BuildIteratorFromSchema(dsSchema, "document", "viewer")
		require.NoError(err)
		require.NotNil(it)

		// Should create union with mixed relation types
		explain := it.Explain()
		explainStr := explain.String()
		require.Contains(explainStr, "Union") // Should contain union for different relation types

		// Test execution doesn't crash
		relSeq, err := ctx.Check(it, []Object{NewObject("document", "test_doc")}, NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		_, err = CollectAll(relSeq)
		require.NoError(err)
	})
}

func TestBuildTreeWildcardIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create a simple schema with a wildcard relation using core types directly
	docDef := &corev1.NamespaceDefinition{
		Name: "document",
		Relation: []*corev1.Relation{
			{
				Name: "viewer",
				TypeInformation: &corev1.TypeInformation{
					AllowedDirectRelations: []*corev1.AllowedRelation{
						{
							Namespace: "user",
							RelationOrWildcard: &corev1.AllowedRelation_PublicWildcard_{
								PublicWildcard: &corev1.AllowedRelation_PublicWildcard{},
							},
						},
					},
				},
			},
		},
	}

	userDef := &corev1.NamespaceDefinition{
		Name: "user",
	}

	objectDefs := []*corev1.NamespaceDefinition{userDef, docDef}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	// Verify the schema has the wildcard BaseRelation
	documentDef, ok := dsSchema.GetTypeDefinition("document")
	require.True(ok)
	viewerRelation, ok := documentDef.GetRelation("viewer")
	require.True(ok)
	require.Len(viewerRelation.BaseRelations(), 1)
	baseRel := viewerRelation.BaseRelations()[0]
	require.True(baseRel.Wildcard(), "BaseRelation should have Wildcard: true")
	require.Equal("user", baseRel.Type())

	t.Run("Schema with wildcard creates WildcardIterator", func(t *testing.T) {
		t.Parallel()
		it, err := BuildIteratorFromSchema(dsSchema, "document", "viewer")
		require.NoError(err)
		require.NotNil(it)

		// Verify it's an Alias wrapping a DatastoreIterator with wildcard support
		require.IsType(&AliasIterator{}, it)
		alias := it.(*AliasIterator)
		require.IsType(&DatastoreIterator{}, alias.subIt)

		// Check the explain output contains wildcard information
		explain := it.Explain()
		explainStr := explain.String()
		require.Contains(explainStr, "Datastore")
		require.Contains(explainStr, "user:*")
	})

	t.Run("Mixed wildcard and regular relations", func(t *testing.T) {
		t.Parallel()
		// Create a schema with both wildcard and regular relations
		mixedDocDef := namespace.Namespace(
			"document",
			namespace.MustRelation("viewer", nil,
				namespace.AllowedRelation("user", ""),    // Regular relation
				namespace.AllowedPublicNamespace("user"), // Wildcard relation
			),
		)

		mixedObjectDefs := []*corev1.NamespaceDefinition{userDef, mixedDocDef}
		mixedSchema, err := schema.BuildSchemaFromDefinitions(mixedObjectDefs, nil)
		require.NoError(err)

		it, err := BuildIteratorFromSchema(mixedSchema, "document", "viewer")
		require.NoError(err)
		require.NotNil(it)

		// Should create an alias with a union containing both regular and wildcard iterators
		require.IsType(&AliasIterator{}, it)
		alias := it.(*AliasIterator)
		require.IsType(&UnionIterator{}, alias.subIt)

		// Check explain contains both relation types (regular and wildcard)
		explain := it.Explain()
		explainStr := explain.String()
		require.Contains(explainStr, "Union")
		require.Contains(explainStr, "user:...", "should contain regular relation")
		require.Contains(explainStr, "user:*", "should contain wildcard relation")
	})
}

func TestBuildTreeMutualRecursionSentinelFiltering(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create a schema with mutual recursion between document and otherdocument
	// This is the exact scenario from walkbackandforth.yaml
	userDef := testfixtures.UserNS.CloneVT()

	otherdocumentDef := namespace.Namespace("otherdocument",
		namespace.MustRelation("viewer", nil,
			namespace.AllowedRelation("user", "..."),
			namespace.AllowedRelation("document", "viewer"),
		),
		namespace.MustRelation("view",
			namespace.Union(
				namespace.ComputedUserset("viewer"),
			),
		),
	)

	documentDef := namespace.Namespace("document",
		namespace.MustRelation("viewer", nil,
			namespace.AllowedRelation("user", "..."),
			namespace.AllowedRelation("otherdocument", "viewer"),
		),
		namespace.MustRelation("view",
			namespace.Union(
				namespace.ComputedUserset("viewer"),
			),
		),
	)

	objectDefs := []*corev1.NamespaceDefinition{userDef, documentDef, otherdocumentDef}
	dsSchema, err := schema.BuildSchemaFromDefinitions(objectDefs, nil)
	require.NoError(err)

	t.Run("document viewer builds successfully with mutual recursion", func(t *testing.T) {
		t.Parallel()
		// Build iterator for document#viewer - should detect recursion and wrap properly
		it, err := BuildIteratorFromSchema(dsSchema, "document", "viewer")
		require.NoError(err)
		require.NotNil(it)

		// The tree should contain RecursiveIterator(s) due to mutual recursion
		explain := it.Explain()
		explainStr := explain.String()
		require.Contains(explainStr, "Recursive", "should contain Recursive for mutual recursion")
	})

	t.Run("otherdocument viewer builds successfully with mutual recursion", func(t *testing.T) {
		t.Parallel()
		// Build iterator for otherdocument#viewer - should also handle mutual recursion
		it, err := BuildIteratorFromSchema(dsSchema, "otherdocument", "viewer")
		require.NoError(err)
		require.NotNil(it)

		// The tree should contain RecursiveIterator(s)
		explain := it.Explain()
		explainStr := explain.String()
		require.Contains(explainStr, "Recursive", "should contain Recursive for mutual recursion")
	})

	t.Run("sentinels are filtered by definition/relation", func(t *testing.T) {
		t.Parallel()
		// This test verifies that when building document#viewer, which encounters
		// otherdocument#viewer, which then encounters document#viewer again (recursion),
		// the sentinels are properly filtered so each RecursiveIterator only handles
		// its own sentinels.

		// Build the tree
		it, err := BuildIteratorFromSchema(dsSchema, "document", "viewer")
		require.NoError(err)
		require.NotNil(it)

		// Walk the tree to find RecursiveIterators and verify their sentinels
		var recursiveIterators []*RecursiveIterator
		var walk func(Iterator)
		walk = func(it Iterator) {
			if rec, ok := it.(*RecursiveIterator); ok {
				recursiveIterators = append(recursiveIterators, rec)
			}
			for _, sub := range it.Subiterators() {
				walk(sub)
			}
		}
		walk(it)

		for _, recursiveIterator := range recursiveIterators {
			require.NotEmpty(recursiveIterator.definitionName)
			require.NotEmpty(recursiveIterator.relationName)

			var recursiveSentinels []*RecursiveSentinelIterator
			_, _ = Walk(recursiveIterator.templateTree, func(it Iterator) (Iterator, error) {
				if sentinel, ok := it.(*RecursiveSentinelIterator); ok {
					recursiveSentinels = append(recursiveSentinels, sentinel)
				}
				return it, nil
			})

			// Verify each sentinel matches THIS RecursiveIterator
			for _, recursiveSentinel := range recursiveSentinels {
				require.Equal(recursiveIterator.definitionName, recursiveSentinel.DefinitionName())
				require.Equal(recursiveIterator.relationName, recursiveSentinel.RelationName())
			}
		}
	})
}
