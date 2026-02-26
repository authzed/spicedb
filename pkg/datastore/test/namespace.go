package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	testNamespace = ns.Namespace("foo/bar",
		ns.MustRelation("editor", nil, ns.AllowedRelation(testUserNS.Name, "...")),
	)

	updatedNamespace = ns.Namespace(testNamespace.Name,
		ns.MustRelation("reader", nil, ns.AllowedRelation(testUserNS.Name, "...")),
		ns.MustRelation("editor", nil, ns.AllowedRelation(testUserNS.Name, "...")),
	)
)

// NamespaceNotFoundTest tests to ensure that an unknown namespace returns the expected
// error.
func NamespaceNotFoundTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	startRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	_, _, err = ds.SnapshotReader(startRevision).LegacyReadNamespaceByName(ctx, "unknown")
	require.ErrorAs(err, &datastore.NamespaceNotFoundError{})
}

// NamespaceWriteTest tests whether or not the requirements for writing
// namespaces hold for a particular datastore.
func NamespaceWriteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	startRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	nsDefs, err := ds.SnapshotReader(startRevision).LegacyListAllNamespaces(ctx)
	require.NoError(err)
	require.Empty(nsDefs)

	writtenRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, testUserNS)
	})
	require.NoError(err)
	require.True(writtenRev.GreaterThan(startRevision))

	nsDefs, err = ds.SnapshotReader(writtenRev).LegacyListAllNamespaces(ctx)
	require.NoError(err)
	require.Len(nsDefs, 1)
	require.Equal(testUserNS.Name, nsDefs[0].Definition.Name)

	secondWritten, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, testNamespace)
	})
	require.NoError(err)
	require.True(secondWritten.GreaterThan(writtenRev))

	nsDefs, err = ds.SnapshotReader(secondWritten).LegacyListAllNamespaces(ctx)
	require.NoError(err)
	require.Len(nsDefs, 2)

	_, _, err = ds.SnapshotReader(writtenRev).LegacyReadNamespaceByName(ctx, testNamespace.Name)
	require.Error(err)

	nsDefs, err = ds.SnapshotReader(writtenRev).LegacyListAllNamespaces(ctx)
	require.NoError(err)
	require.Len(nsDefs, 1)

	found, createdRev, err := ds.SnapshotReader(secondWritten).LegacyReadNamespaceByName(ctx, testNamespace.Name)
	require.NoError(err)
	require.False(createdRev.GreaterThan(secondWritten))
	require.True(createdRev.GreaterThan(startRevision))
	foundDiff := cmp.Diff(testNamespace, found, protocmp.Transform())
	require.Empty(foundDiff)

	updatedRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, updatedNamespace)
	})
	require.NoError(err)

	checkUpdated, createdRev, err := ds.SnapshotReader(updatedRevision).LegacyReadNamespaceByName(ctx, testNamespace.Name)
	require.NoError(err)
	require.False(createdRev.GreaterThan(updatedRevision))
	require.True(createdRev.GreaterThan(startRevision))
	foundUpdated := cmp.Diff(updatedNamespace, checkUpdated, protocmp.Transform())
	require.Empty(foundUpdated)

	checkOld, createdRev, err := ds.SnapshotReader(writtenRev).LegacyReadNamespaceByName(ctx, testUserNamespace)
	require.NoError(err)
	require.False(createdRev.GreaterThan(writtenRev))
	require.True(createdRev.GreaterThan(startRevision))
	require.Empty(cmp.Diff(testUserNS, checkOld, protocmp.Transform()))

	checkOldList, err := ds.SnapshotReader(writtenRev).LegacyListAllNamespaces(ctx)
	require.NoError(err)
	require.Len(checkOldList, 1)
	require.Equal(testUserNS.Name, checkOldList[0].Definition.Name)
	require.Empty(cmp.Diff(testUserNS, checkOldList[0].Definition, protocmp.Transform()))

	checkLookup, err := ds.SnapshotReader(secondWritten).LegacyLookupNamespacesWithNames(ctx, []string{testNamespace.Name})
	require.NoError(err)
	require.Len(checkLookup, 1)
	require.Equal(testNamespace.Name, checkLookup[0].Definition.Name)
	require.Empty(cmp.Diff(testNamespace, checkLookup[0].Definition, protocmp.Transform()))

	checkLookupMultiple, err := ds.SnapshotReader(secondWritten).LegacyLookupNamespacesWithNames(ctx, []string{testNamespace.Name, testUserNS.Name})
	require.NoError(err)
	require.Len(checkLookupMultiple, 2)

	emptyLookup, err := ds.SnapshotReader(secondWritten).LegacyLookupNamespacesWithNames(ctx, []string{"anothername"})
	require.NoError(err)
	require.Empty(emptyLookup)
}

// NamespaceDeleteTest tests whether or not the requirements for deleting
// namespaces hold for a particular datastore.
func NamespaceDeleteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := t.Context()

	tRequire := testfixtures.RelationshipChecker{Require: require, DS: ds}
	docTpl, err := tuple.Parse(testfixtures.StandardRelationships[0])
	require.NoError(err)
	require.NotNil(docTpl)
	tRequire.RelationshipExists(ctx, docTpl, revision)

	folderTpl, err := tuple.Parse(testfixtures.StandardRelationships[2])
	require.NoError(err)
	require.NotNil(folderTpl)
	tRequire.RelationshipExists(ctx, folderTpl, revision)

	deletedRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyDeleteNamespaces(ctx, []string{testfixtures.DocumentNS.Name}, datastore.DeleteNamespacesAndRelationships)
	})
	require.NoError(err)
	require.True(deletedRev.GreaterThan(revision))

	_, _, err = ds.SnapshotReader(deletedRev).LegacyReadNamespaceByName(ctx, testfixtures.DocumentNS.Name)
	require.ErrorAs(err, &datastore.NamespaceNotFoundError{})

	found, nsCreatedRev, err := ds.SnapshotReader(deletedRev).LegacyReadNamespaceByName(ctx, testfixtures.FolderNS.Name)
	require.NoError(err)
	require.NotNil(found)
	require.True(nsCreatedRev.LessThan(deletedRev))

	allNamespaces, err := ds.SnapshotReader(deletedRev).LegacyListAllNamespaces(ctx)
	require.NoError(err)
	for _, ns := range allNamespaces {
		require.NotEqual(testfixtures.DocumentNS.Name, ns.Definition.Name, "deleted namespace '%s' should not be in namespace list", ns.Definition.Name)
	}

	deletedRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	iter, err := ds.SnapshotReader(deletedRevision).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
	}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(err)
	tRequire.VerifyIteratorResults(iter)

	tRequire.RelationshipExists(ctx, folderTpl, deletedRevision)
}

func NamespaceDeleteNoRelationshipsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	ctx := t.Context()

	tRequire := testfixtures.RelationshipChecker{Require: require, DS: ds}
	docTpl, err := tuple.Parse(testfixtures.StandardRelationships[0])
	require.NoError(err)
	require.NotNil(docTpl)
	tRequire.NoRelationshipExists(ctx, docTpl, revision)

	deletedRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyDeleteNamespaces(ctx, []string{testfixtures.DocumentNS.Name}, datastore.DeleteNamespacesOnly)
	})
	require.NoError(err)
	require.True(deletedRev.GreaterThan(revision))

	_, _, err = ds.SnapshotReader(deletedRev).LegacyReadNamespaceByName(ctx, testfixtures.DocumentNS.Name)
	require.ErrorAs(err, &datastore.NamespaceNotFoundError{})

	found, nsCreatedRev, err := ds.SnapshotReader(deletedRev).LegacyReadNamespaceByName(ctx, testfixtures.FolderNS.Name)
	require.NoError(err)
	require.NotNil(found)
	require.True(nsCreatedRev.LessThan(deletedRev))

	allNamespaces, err := ds.SnapshotReader(deletedRev).LegacyListAllNamespaces(ctx)
	require.NoError(err)
	for _, ns := range allNamespaces {
		require.NotEqual(testfixtures.DocumentNS.Name, ns.Definition.Name, "deleted namespace '%s' should not be in namespace list", ns.Definition.Name)
	}
}

func NamespaceMultiDeleteTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	ctx := t.Context()

	namespaces, err := ds.SnapshotReader(revision).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)

	nsNames := make([]string, 0, len(namespaces))
	for _, ns := range namespaces {
		nsNames = append(nsNames, ns.Definition.Name)
	}

	deletedRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyDeleteNamespaces(ctx, nsNames, datastore.DeleteNamespacesAndRelationships)
	})
	require.NoError(t, err)

	namespacesAfterDel, err := ds.SnapshotReader(deletedRev).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Empty(t, namespacesAfterDel)
}

// EmptyNamespaceDeleteTest tests deleting an empty namespace in the datastore.
func EmptyNamespaceDeleteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := t.Context()

	deletedRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyDeleteNamespaces(ctx, []string{testfixtures.UserNS.Name}, datastore.DeleteNamespacesAndRelationships)
	})
	require.NoError(err)
	require.True(deletedRev.GreaterThan(revision))

	_, _, err = ds.SnapshotReader(deletedRev).LegacyReadNamespaceByName(ctx, testfixtures.UserNS.Name)
	require.ErrorAs(err, &datastore.NamespaceNotFoundError{})
}

// NamespaceDeleteInvalidNamespaceTest tests deleting an invalid namespace in the datastore.
func NamespaceDeleteInvalidNamespaceTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	schemaString := `definition user {}

definition document {
	relation viewer: user
}`

	// Compile namespace to write to the datastore.
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)
	require.Len(compiled.OrderedDefinitions, 2)

	// Write the namespace definition to the datastore.
	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.LegacyWriteCaveats(ctx, compiled.CaveatDefinitions)
		if err != nil {
			return err
		}

		return rwt.LegacyWriteNamespaces(ctx, compiled.ObjectDefinitions...)
	})
	require.NoError(err)

	// Attempt to delete the invalid namespace.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyDeleteNamespaces(ctx, []string{"invalid"}, datastore.DeleteNamespacesAndRelationships)
	})
	require.Error(err)
	require.ErrorContains(err, "not found")
}

// StableNamespaceReadWriteTest tests writing a namespace to the datastore and reading it back,
// ensuring that it does not change in any way and that the deserialized data matches that stored.
func StableNamespaceReadWriteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	schemaString := `caveat foo(someParam int) {
	someParam == 42
}

definition document {
	relation viewer: user | user:*
	relation editor: user | group#member with foo
	relation parent: organization
	permission edit = editor
	permission view = viewer + edit + parent->view
	permission other = viewer - edit
	permission intersect = viewer & edit
	permission with_nil = (viewer - edit) & parent->view & nil
}`

	// Compile namespace to write to the datastore.
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)
	require.Len(compiled.OrderedDefinitions, 2)

	// Write the namespace definition to the datastore.
	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()
	updatedRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.LegacyWriteCaveats(ctx, compiled.CaveatDefinitions)
		if err != nil {
			return err
		}

		return rwt.LegacyWriteNamespaces(ctx, compiled.ObjectDefinitions...)
	})
	require.NoError(err)

	// Read the namespace definition back from the datastore and compare.
	nsConfig := compiled.ObjectDefinitions[0]
	readNsDef, _, err := ds.SnapshotReader(updatedRevision).LegacyReadNamespaceByName(ctx, nsConfig.Name)
	require.NoError(err)
	testutil.RequireProtoEqual(t, nsConfig, readNsDef, "found changed namespace definition")

	// Read the caveat back from the datastore and compare.
	caveatDef := compiled.CaveatDefinitions[0]
	readCaveatDef, _, err := ds.SnapshotReader(updatedRevision).LegacyReadCaveatByName(ctx, caveatDef.Name)
	require.NoError(err)
	testutil.RequireProtoEqual(t, caveatDef, readCaveatDef, "found changed caveat definition")

	// Ensure the read namespace's string form matches the input as an extra check.
	generated, _, err := generator.GenerateSchema([]compiler.SchemaDefinition{readCaveatDef, readNsDef})
	require.NoError(err)
	require.Equal(schemaString, generated)
}
