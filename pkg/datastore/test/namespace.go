package test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	testNamespace = ns.Namespace("foo/bar",
		ns.Relation("editor", nil, ns.AllowedRelation(testUserNS.Name, "...")),
	)

	updatedNamespace = ns.Namespace(testNamespace.Name,
		ns.Relation("reader", nil, ns.AllowedRelation(testUserNS.Name, "...")),
		ns.Relation("editor", nil, ns.AllowedRelation(testUserNS.Name, "...")),
	)
)

// NamespaceWriteTest tests whether or not the requirements for writing
// namespaces hold for a particular datastore.
func NamespaceWriteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := context.Background()

	startRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)
	require.True(startRevision.GreaterThan(datastore.NoRevision))

	nsDefs, err := ds.SnapshotReader(startRevision).ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(0, len(nsDefs))

	writtenRev, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, testUserNS)
	})
	require.NoError(err)
	require.True(writtenRev.GreaterThan(startRevision))

	nsDefs, err = ds.SnapshotReader(writtenRev).ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(1, len(nsDefs))
	require.Equal(testUserNS.Name, nsDefs[0].Name)

	secondWritten, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, testNamespace)
	})
	require.NoError(err)
	require.True(secondWritten.GreaterThan(writtenRev))

	nsDefs, err = ds.SnapshotReader(secondWritten).ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(2, len(nsDefs))

	_, _, err = ds.SnapshotReader(writtenRev).ReadNamespace(ctx, testNamespace.Name)
	require.Error(err)

	nsDefs, err = ds.SnapshotReader(writtenRev).ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(1, len(nsDefs))

	found, createdRev, err := ds.SnapshotReader(secondWritten).ReadNamespace(ctx, testNamespace.Name)
	require.NoError(err)
	require.False(createdRev.GreaterThan(secondWritten))
	require.True(createdRev.GreaterThan(startRevision))
	foundDiff := cmp.Diff(testNamespace, found, protocmp.Transform())
	require.Empty(foundDiff)

	updatedRevision, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteNamespaces(ctx, updatedNamespace)
	})
	require.NoError(err)

	checkUpdated, createdRev, err := ds.SnapshotReader(updatedRevision).ReadNamespace(ctx, testNamespace.Name)
	require.NoError(err)
	require.False(createdRev.GreaterThan(updatedRevision))
	require.True(createdRev.GreaterThan(startRevision))
	foundUpdated := cmp.Diff(updatedNamespace, checkUpdated, protocmp.Transform())
	require.Empty(foundUpdated)

	checkOld, createdRev, err := ds.SnapshotReader(writtenRev).ReadNamespace(ctx, testUserNamespace)
	require.NoError(err)
	require.False(createdRev.GreaterThan(writtenRev))
	require.True(createdRev.GreaterThan(startRevision))
	require.Empty(cmp.Diff(testUserNS, checkOld, protocmp.Transform()))

	checkOldList, err := ds.SnapshotReader(writtenRev).ListNamespaces(ctx)
	require.NoError(err)
	require.Equal(1, len(checkOldList))
	require.Equal(testUserNS.Name, checkOldList[0].Name)
	require.Empty(cmp.Diff(testUserNS, checkOldList[0], protocmp.Transform()))

	checkLookup, err := ds.SnapshotReader(secondWritten).LookupNamespaces(ctx, []string{testNamespace.Name})
	require.NoError(err)
	require.Equal(1, len(checkLookup))
	require.Equal(testNamespace.Name, checkLookup[0].Name)
	require.Empty(cmp.Diff(testNamespace, checkLookup[0], protocmp.Transform()))

	checkLookupMultiple, err := ds.SnapshotReader(secondWritten).LookupNamespaces(ctx, []string{testNamespace.Name, testUserNS.Name})
	require.NoError(err)
	require.Equal(2, len(checkLookupMultiple))

	emptyLookup, err := ds.SnapshotReader(secondWritten).LookupNamespaces(ctx, []string{"anothername"})
	require.NoError(err)
	require.Equal(0, len(emptyLookup))
}

// NamespaceDeleteTest tests whether or not the requirements for deleting
// namespaces hold for a particular datastore.
func NamespaceDeleteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	tRequire := testfixtures.TupleChecker{Require: require, DS: ds}
	docTpl := tuple.Parse(testfixtures.StandardTuples[0])
	require.NotNil(docTpl)
	tRequire.TupleExists(ctx, docTpl, revision)

	folderTpl := tuple.Parse(testfixtures.StandardTuples[2])
	require.NotNil(folderTpl)
	tRequire.TupleExists(ctx, folderTpl, revision)

	deletedRev, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		return rwt.DeleteNamespaces(ctx, testfixtures.DocumentNS.Name)
	})
	require.NoError(err)
	require.True(deletedRev.GreaterThan(revision))

	_, _, err = ds.SnapshotReader(deletedRev).ReadNamespace(ctx, testfixtures.DocumentNS.Name)
	require.True(errors.As(err, &datastore.ErrNamespaceNotFound{}))

	found, nsCreatedRev, err := ds.SnapshotReader(deletedRev).ReadNamespace(ctx, testfixtures.FolderNS.Name)
	require.NotNil(found)
	require.True(nsCreatedRev.LessThan(deletedRev))
	require.NoError(err)

	allNamespaces, err := ds.SnapshotReader(deletedRev).ListNamespaces(ctx)
	require.NoError(err)
	for _, ns := range allNamespaces {
		require.NotEqual(testfixtures.DocumentNS.Name, ns.Name, "deleted namespace '%s' should not be in namespace list", ns.Name)
	}

	deletedRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	iter, err := ds.SnapshotReader(deletedRevision).QueryRelationships(ctx, datastore.RelationshipsFilter{
		ResourceType: testfixtures.DocumentNS.Name,
	})
	require.NoError(err)
	tRequire.VerifyIteratorResults(iter)

	tRequire.TupleExists(ctx, folderTpl, deletedRevision)
}

func NamespaceMultiDeleteTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))
	ctx := context.Background()

	namespaces, err := ds.SnapshotReader(revision).ListNamespaces(ctx)
	require.NoError(t, err)

	nsNames := make([]string, 0, len(namespaces))
	for _, ns := range namespaces {
		nsNames = append(nsNames, ns.Name)
	}

	deletedRev, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		return rwt.DeleteNamespaces(ctx, nsNames...)
	})
	require.NoError(t, err)

	namespacesAfterDel, err := ds.SnapshotReader(deletedRev).ListNamespaces(ctx)
	require.NoError(t, err)
	require.Len(t, namespacesAfterDel, 0)
}

// EmptyNamespaceDeleteTest tests deleting an empty namespace in the datastore.
func EmptyNamespaceDeleteTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)
	ctx := context.Background()

	deletedRev, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		return rwt.DeleteNamespaces(ctx, testfixtures.UserNS.Name)
	})
	require.NoError(err)
	require.True(deletedRev.GreaterThan(revision))

	_, _, err = ds.SnapshotReader(deletedRev).ReadNamespace(ctx, testfixtures.UserNS.Name)
	require.True(errors.As(err, &datastore.ErrNamespaceNotFound{}))
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
	permission with_nil = ((viewer - edit) & parent->view) & nil
}`

	// Compile namespace to write to the datastore.
	empty := ""
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schemaString,
	}, &empty)
	require.NoError(err)
	require.Equal(2, len(compiled.OrderedDefinitions))

	// Write the namespace definition to the datastore.
	ds, err := tester.New(0, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := context.Background()
	updatedRevision, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteCaveats(ctx, compiled.CaveatDefinitions)
		if err != nil {
			return err
		}

		return rwt.WriteNamespaces(ctx, compiled.ObjectDefinitions...)
	})
	require.NoError(err)

	// Read the namespace definition back from the datastore and compare.
	nsConfig := compiled.ObjectDefinitions[0]
	readNsDef, _, err := ds.SnapshotReader(updatedRevision).ReadNamespace(ctx, nsConfig.Name)
	require.NoError(err)
	testutil.RequireProtoEqual(t, nsConfig, readNsDef, "found changed namespace definition")

	// Read the caveat back from the datastore and compare.
	caveatDef := compiled.CaveatDefinitions[0]
	readCaveatDef, _, err := ds.SnapshotReader(updatedRevision).ReadCaveatByName(ctx, caveatDef.Name)
	require.NoError(err)
	testutil.RequireProtoEqual(t, caveatDef, readCaveatDef, "found changed caveat definition")

	// Ensure the read namespace's string form matches the input as an extra check.
	generated, _ := generator.GenerateSchema([]compiler.SchemaDefinition{readCaveatDef, readNsDef})
	require.Equal(schemaString, generated)
}
