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
		ns.MustRelation("editor", nil, ns.AllowedRelation(testUserNS.GetName(), "...")),
	)

	updatedNamespace = ns.Namespace(testNamespace.GetName(),
		ns.MustRelation("reader", nil, ns.AllowedRelation(testUserNS.GetName(), "...")),
		ns.MustRelation("editor", nil, ns.AllowedRelation(testUserNS.GetName(), "...")),
	)
)

// NamespaceNotFoundTest tests to ensure that an unknown namespace returns the expected
// error.
func NamespaceNotFoundTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	startRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	_, _, err = ds.SnapshotReader(startRevision).ReadNamespaceByName(ctx, "unknown")
	require.ErrorAs(err, &datastore.NamespaceNotFoundError{})
}

// NamespaceWriteTest tests whether or not the requirements for writing
// namespaces hold for a particular datastore.
func NamespaceWriteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	startRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	nsDefs, err := ds.SnapshotReader(startRevision).ListAllNamespaces(ctx)
	require.NoError(err)
	require.Empty(nsDefs)

	writtenRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, testUserNS)
	})
	require.NoError(err)
	require.True(writtenRev.GreaterThan(startRevision))

	nsDefs, err = ds.SnapshotReader(writtenRev).ListAllNamespaces(ctx)
	require.NoError(err)
	require.Len(nsDefs, 1)
	require.Equal(testUserNS.GetName(), nsDefs[0].Definition.GetName())

	secondWritten, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, testNamespace)
	})
	require.NoError(err)
	require.True(secondWritten.GreaterThan(writtenRev))

	nsDefs, err = ds.SnapshotReader(secondWritten).ListAllNamespaces(ctx)
	require.NoError(err)
	require.Len(nsDefs, 2)

	_, _, err = ds.SnapshotReader(writtenRev).ReadNamespaceByName(ctx, testNamespace.GetName())
	require.Error(err)

	nsDefs, err = ds.SnapshotReader(writtenRev).ListAllNamespaces(ctx)
	require.NoError(err)
	require.Len(nsDefs, 1)

	found, createdRev, err := ds.SnapshotReader(secondWritten).ReadNamespaceByName(ctx, testNamespace.GetName())
	require.NoError(err)
	require.False(createdRev.GreaterThan(secondWritten))
	require.True(createdRev.GreaterThan(startRevision))
	foundDiff := cmp.Diff(testNamespace, found, protocmp.Transform())
	require.Empty(foundDiff)

	updatedRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, updatedNamespace)
	})
	require.NoError(err)

	checkUpdated, createdRev, err := ds.SnapshotReader(updatedRevision).ReadNamespaceByName(ctx, testNamespace.GetName())
	require.NoError(err)
	require.False(createdRev.GreaterThan(updatedRevision))
	require.True(createdRev.GreaterThan(startRevision))
	foundUpdated := cmp.Diff(updatedNamespace, checkUpdated, protocmp.Transform())
	require.Empty(foundUpdated)

	checkOld, createdRev, err := ds.SnapshotReader(writtenRev).ReadNamespaceByName(ctx, testUserNamespace)
	require.NoError(err)
	require.False(createdRev.GreaterThan(writtenRev))
	require.True(createdRev.GreaterThan(startRevision))
	require.Empty(cmp.Diff(testUserNS, checkOld, protocmp.Transform()))

	checkOldList, err := ds.SnapshotReader(writtenRev).ListAllNamespaces(ctx)
	require.NoError(err)
	require.Len(checkOldList, 1)
	require.Equal(testUserNS.GetName(), checkOldList[0].Definition.GetName())
	require.Empty(cmp.Diff(testUserNS, checkOldList[0].Definition, protocmp.Transform()))

	checkLookup, err := ds.SnapshotReader(secondWritten).LookupNamespacesWithNames(ctx, []string{testNamespace.GetName()})
	require.NoError(err)
	require.Len(checkLookup, 1)
	require.Equal(testNamespace.GetName(), checkLookup[0].Definition.GetName())
	require.Empty(cmp.Diff(testNamespace, checkLookup[0].Definition, protocmp.Transform()))

	checkLookupMultiple, err := ds.SnapshotReader(secondWritten).LookupNamespacesWithNames(ctx, []string{testNamespace.GetName(), testUserNS.GetName()})
	require.NoError(err)
	require.Len(checkLookupMultiple, 2)

	emptyLookup, err := ds.SnapshotReader(secondWritten).LookupNamespacesWithNames(ctx, []string{"anothername"})
	require.NoError(err)
	require.Empty(emptyLookup)
}

// NamespaceDeleteTest tests whether or not the requirements for deleting
// namespaces hold for a particular datastore.
func NamespaceDeleteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
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
		return rwt.DeleteNamespaces(ctx, []string{testfixtures.DocumentNS.GetName()}, datastore.DeleteNamespacesAndRelationships)
	})
	require.NoError(err)
	require.True(deletedRev.GreaterThan(revision))

	_, _, err = ds.SnapshotReader(deletedRev).ReadNamespaceByName(ctx, testfixtures.DocumentNS.GetName())
	require.ErrorAs(err, &datastore.NamespaceNotFoundError{})

	found, nsCreatedRev, err := ds.SnapshotReader(deletedRev).ReadNamespaceByName(ctx, testfixtures.FolderNS.GetName())
	require.NoError(err)
	require.NotNil(found)
	require.True(nsCreatedRev.LessThan(deletedRev))

	allNamespaces, err := ds.SnapshotReader(deletedRev).ListAllNamespaces(ctx)
	require.NoError(err)
	for _, ns := range allNamespaces {
		require.NotEqual(testfixtures.DocumentNS.GetName(), ns.Definition.GetName(), "deleted namespace '%s' should not be in namespace list", ns.Definition.GetName())
	}

	deletedRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	iter, err := ds.SnapshotReader(deletedRevision).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.GetName(),
	}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(err)
	tRequire.VerifyIteratorResults(iter)

	tRequire.RelationshipExists(ctx, folderTpl, deletedRevision)
}

func NamespaceDeleteNoRelationshipsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	ctx := t.Context()

	tRequire := testfixtures.RelationshipChecker{Require: require, DS: ds}
	docTpl, err := tuple.Parse(testfixtures.StandardRelationships[0])
	require.NoError(err)
	require.NotNil(docTpl)
	tRequire.NoRelationshipExists(ctx, docTpl, revision)

	deletedRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.DeleteNamespaces(ctx, []string{testfixtures.DocumentNS.GetName()}, datastore.DeleteNamespacesOnly)
	})
	require.NoError(err)
	require.True(deletedRev.GreaterThan(revision))

	_, _, err = ds.SnapshotReader(deletedRev).ReadNamespaceByName(ctx, testfixtures.DocumentNS.GetName())
	require.ErrorAs(err, &datastore.NamespaceNotFoundError{})

	found, nsCreatedRev, err := ds.SnapshotReader(deletedRev).ReadNamespaceByName(ctx, testfixtures.FolderNS.GetName())
	require.NoError(err)
	require.NotNil(found)
	require.True(nsCreatedRev.LessThan(deletedRev))

	allNamespaces, err := ds.SnapshotReader(deletedRev).ListAllNamespaces(ctx)
	require.NoError(err)
	for _, ns := range allNamespaces {
		require.NotEqual(testfixtures.DocumentNS.GetName(), ns.Definition.GetName(), "deleted namespace '%s' should not be in namespace list", ns.Definition.GetName())
	}
}

func NamespaceMultiDeleteTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	ctx := t.Context()

	namespaces, err := ds.SnapshotReader(revision).ListAllNamespaces(ctx)
	require.NoError(t, err)

	nsNames := make([]string, 0, len(namespaces))
	for _, ns := range namespaces {
		nsNames = append(nsNames, ns.Definition.GetName())
	}

	deletedRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.DeleteNamespaces(ctx, nsNames, datastore.DeleteNamespacesAndRelationships)
	})
	require.NoError(t, err)

	namespacesAfterDel, err := ds.SnapshotReader(deletedRev).ListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Empty(t, namespacesAfterDel)
}

// EmptyNamespaceDeleteTest tests deleting an empty namespace in the datastore.
func EmptyNamespaceDeleteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := t.Context()

	deletedRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.DeleteNamespaces(ctx, []string{testfixtures.UserNS.GetName()}, datastore.DeleteNamespacesAndRelationships)
	})
	require.NoError(err)
	require.True(deletedRev.GreaterThan(revision))

	_, _, err = ds.SnapshotReader(deletedRev).ReadNamespaceByName(ctx, testfixtures.UserNS.GetName())
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
	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteCaveats(ctx, compiled.CaveatDefinitions)
		if err != nil {
			return err
		}

		return rwt.WriteNamespaces(ctx, compiled.ObjectDefinitions...)
	})
	require.NoError(err)

	// Attempt to delete the invalid namespace.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.DeleteNamespaces(ctx, []string{"invalid"}, datastore.DeleteNamespacesAndRelationships)
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
	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()
	updatedRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteCaveats(ctx, compiled.CaveatDefinitions)
		if err != nil {
			return err
		}

		return rwt.WriteNamespaces(ctx, compiled.ObjectDefinitions...)
	})
	require.NoError(err)

	// Read the namespace definition back from the datastore and compare.
	nsConfig := compiled.ObjectDefinitions[0]
	readNsDef, _, err := ds.SnapshotReader(updatedRevision).ReadNamespaceByName(ctx, nsConfig.GetName())
	require.NoError(err)
	testutil.RequireProtoEqual(t, nsConfig, readNsDef, "found changed namespace definition")

	// Read the caveat back from the datastore and compare.
	caveatDef := compiled.CaveatDefinitions[0]
	readCaveatDef, _, err := ds.SnapshotReader(updatedRevision).ReadCaveatByName(ctx, caveatDef.GetName())
	require.NoError(err)
	testutil.RequireProtoEqual(t, caveatDef, readCaveatDef, "found changed caveat definition")

	// Ensure the read namespace's string form matches the input as an extra check.
	generated, _, err := generator.GenerateSchema([]compiler.SchemaDefinition{readCaveatDef, readNsDef})
	require.NoError(err)
	require.Equal(schemaString, generated)
}
