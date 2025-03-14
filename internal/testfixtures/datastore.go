package testfixtures

import (
	"context"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

var UserNS = ns.Namespace("user")

var CaveatDef = ns.MustCaveatDefinition(
	caveats.MustEnvForVariables(map[string]caveattypes.VariableType{
		"secret":         caveattypes.StringType,
		"expectedSecret": caveattypes.StringType,
	}),
	"test",
	"secret == expectedSecret",
)

var DocumentNS = ns.Namespace(
	"document",
	ns.MustRelation("owner",
		nil,
		ns.AllowedRelation("user", "..."),
		ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("test")),
	),
	ns.MustRelation("editor",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.MustRelation("viewer",
		nil,
		ns.AllowedRelation("user", "..."),
		ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("test")),
	),
	ns.MustRelation("viewer_and_editor",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.MustRelation("caveated_viewer",
		nil,
		ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("test")),
	),
	ns.MustRelation("expiring_viewer",
		nil,
		ns.AllowedRelationWithExpiration("user", "..."),
	),
	ns.MustRelation("parent", nil, ns.AllowedRelation("folder", "...")),
	ns.MustRelation("edit",
		ns.Union(
			ns.ComputedUserset("owner"),
			ns.ComputedUserset("editor"),
		),
	),
	ns.MustRelation("view",
		ns.Union(
			ns.ComputedUserset("viewer"),
			ns.ComputedUserset("edit"),
			ns.TupleToUserset("parent", "view"),
		),
	),
	ns.MustRelation("view_and_edit",
		ns.Intersection(
			ns.ComputedUserset("viewer_and_editor"),
			ns.ComputedUserset("edit"),
		),
	),
)

var FolderNS = ns.Namespace(
	"folder",
	ns.MustRelation("owner",
		nil,
		ns.AllowedRelation("user", "..."),
		ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("test")),
	),
	ns.MustRelation("editor",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.MustRelation("viewer",
		nil,
		ns.AllowedRelation("user", "..."),
		ns.AllowedRelation("folder", "viewer"),
		ns.AllowedRelationWithCaveat("folder", "viewer", ns.AllowedCaveat("test")),
	),
	ns.MustRelation("parent", nil, ns.AllowedRelation("folder", "...")),
	ns.MustRelation("edit",
		ns.Union(
			ns.ComputedUserset("editor"),
			ns.ComputedUserset("owner"),
		),
	),
	ns.MustRelation("view",
		ns.Union(
			ns.ComputedUserset("viewer"),
			ns.ComputedUserset("edit"),
			ns.TupleToUserset("parent", "view"),
		),
	),
)

// StandardRelationships defines standard relationships for tests.
// NOTE: some tests index directly into this slice, so if you're adding a new relationship, add it
// at the *end*.
var StandardRelationships = []string{
	"document:companyplan#parent@folder:company#...",
	"document:masterplan#parent@folder:strategy#...",
	"folder:strategy#parent@folder:company#...",
	"folder:company#owner@user:owner#...",
	"folder:company#viewer@user:legal#...",
	"folder:strategy#owner@user:vp_product#...",
	"document:masterplan#owner@user:product_manager#...",
	"document:masterplan#viewer@user:eng_lead#...",
	"document:masterplan#parent@folder:plans#...",
	"folder:plans#viewer@user:chief_financial_officer#...",
	"folder:auditors#viewer@user:auditor#...",
	"folder:company#viewer@folder:auditors#viewer",
	"document:healthplan#parent@folder:plans#...",
	"folder:isolated#viewer@user:villain#...",
	"document:specialplan#viewer_and_editor@user:multiroleguy#...",
	"document:specialplan#editor@user:multiroleguy#...",
	"document:specialplan#viewer_and_editor@user:missingrolegal#...",
	"document:base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#owner@user:base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#...",
	"document:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong#owner@user:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong#...",
	"document:ownerplan#viewer@user:owner#...",
}

var StandardCaveatedRelationships = []string{
	"document:caveatedplan#caveated_viewer@user:caveatedguy#...[test:{\"expectedSecret\":\"1234\"}]",
}

// EmptyDatastore returns an empty datastore for testing.
func EmptyDatastore(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	rev, err := ds.HeadRevision(context.Background())
	require.NoError(err)
	return ds, rev
}

// StandardDatastoreWithSchema returns a datastore populated with the standard test definitions.
func StandardDatastoreWithSchema(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	validating := NewValidatingDatastore(ds)
	objectDefs := []*core.NamespaceDefinition{UserNS.CloneVT(), FolderNS.CloneVT(), DocumentNS.CloneVT()}
	return validating, writeDefinitions(validating, require, objectDefs, []*core.CaveatDefinition{CaveatDef})
}

// StandardDatastoreWithData returns a datastore populated with both the standard test definitions
// and relationships.
func StandardDatastoreWithData(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	ds, _ = StandardDatastoreWithSchema(ds, require)
	ctx := context.Background()

	rels := make([]tuple.Relationship, 0, len(StandardRelationships))
	for _, tupleStr := range StandardRelationships {
		rel, err := tuple.Parse(tupleStr)
		require.NoError(err)
		require.NotNil(rel)
		rels = append(rels, rel)
	}
	revision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rels...)
	require.NoError(err)

	return ds, revision
}

// StandardDatastoreWithCaveatedData returns a datastore populated with both the standard test definitions
// and some caveated relationships.
func StandardDatastoreWithCaveatedData(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	ds, _ = StandardDatastoreWithSchema(ds, require)
	ctx := context.Background()

	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteCaveats(ctx, createTestCaveat(require))
	})
	require.NoError(err)

	rels := make([]tuple.Relationship, 0, len(StandardRelationships)+len(StandardCaveatedRelationships))
	for _, tupleStr := range StandardRelationships {
		rel, err := tuple.Parse(tupleStr)
		require.NoError(err)
		require.NotNil(rel)
		rels = append(rels, rel)
	}
	for _, tupleStr := range StandardCaveatedRelationships {
		rel, err := tuple.Parse(tupleStr)
		require.NoError(err)
		require.NotNil(rel)
		rels = append(rels, rel)
	}

	revision, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rels...)
	require.NoError(err)

	return ds, revision
}

func createTestCaveat(require *require.Assertions) []*core.CaveatDefinition {
	env, err := caveats.EnvForVariables(map[string]caveattypes.VariableType{
		"secret":         caveattypes.StringType,
		"expectedSecret": caveattypes.StringType,
	})
	require.NoError(err)

	c, err := caveats.CompileCaveatWithName(env, "secret == expectedSecret", "test")
	require.NoError(err)

	cBytes, err := c.Serialize()
	require.NoError(err)

	return []*core.CaveatDefinition{{
		Name:                 "test",
		SerializedExpression: cBytes,
		ParameterTypes:       env.EncodedParametersTypes(),
	}}
}

// DatastoreFromSchemaAndTestRelationships returns a validating datastore wrapping that specified,
// loaded with the given scehma and relationships.
func DatastoreFromSchemaAndTestRelationships(ds datastore.Datastore, schema string, relationships []tuple.Relationship, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	ctx := context.Background()
	validating := NewValidatingDatastore(ds)

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schema,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)

	_ = writeDefinitions(validating, require, compiled.ObjectDefinitions, compiled.CaveatDefinitions)

	newRevision, err := validating.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		mutations := make([]tuple.RelationshipUpdate, 0, len(relationships))
		for _, rel := range relationships {
			mutations = append(mutations, tuple.Create(rel))
		}
		err = rwt.WriteRelationships(ctx, mutations)
		require.NoError(err)

		return nil
	})
	require.NoError(err)

	return validating, newRevision
}

func writeDefinitions(ds datastore.Datastore, require *require.Assertions, objectDefs []*core.NamespaceDefinition, caveatDefs []*core.CaveatDefinition) datastore.Revision {
	ctx := context.Background()
	newRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		if len(caveatDefs) > 0 {
			err := rwt.WriteCaveats(ctx, caveatDefs)
			require.NoError(err)
		}

		ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(rwt).WithPredefinedElements(schema.PredefinedElements{
			Definitions: objectDefs,
			Caveats:     caveatDefs,
		}))
		for _, nsDef := range objectDefs {
			vdef, err := ts.GetValidatedDefinition(ctx, nsDef.GetName())
			require.NoError(err)

			aerr := namespace.AnnotateNamespace(vdef)
			require.NoError(aerr)

			err = rwt.WriteNamespaces(ctx, nsDef)
			require.NoError(err)
		}

		return nil
	})
	require.NoError(err)
	return newRevision
}

// RelationshipChecker is a helper type which provides an easy way for collecting relationships from
// an iterator and verify those found.
type RelationshipChecker struct {
	Require *require.Assertions
	DS      datastore.Datastore
}

func (tc RelationshipChecker) ExactRelationshipIterator(ctx context.Context, rel tuple.Relationship, rev datastore.Revision) datastore.RelationshipIterator {
	filter := tuple.ToV1Filter(rel)
	dsFilter, err := datastore.RelationshipsFilterFromPublicFilter(filter)
	tc.Require.NoError(err)

	iter, err := tc.DS.SnapshotReader(rev).QueryRelationships(ctx, dsFilter)
	tc.Require.NoError(err)
	return iter
}

func (tc RelationshipChecker) VerifyIteratorCount(iter datastore.RelationshipIterator, count int) {
	foundCount := 0
	for _, err := range iter {
		tc.Require.NoError(err)
		foundCount++
	}
	tc.Require.Equal(count, foundCount)
}

func (tc RelationshipChecker) VerifyIteratorResults(iter datastore.RelationshipIterator, rels ...tuple.Relationship) {
	toFind := mapz.NewSet[string]()
	for _, rel := range rels {
		toFind.Add(tuple.MustString(rel))
	}

	for found, err := range iter {
		tc.Require.NoError(err)

		foundStr := tuple.MustString(found)
		tc.Require.True(toFind.Has(foundStr), "found unexpected relationship %s in iterator", foundStr)
		toFind.Delete(foundStr)
	}

	tc.Require.True(toFind.IsEmpty(), "did not find some expected relationships: %#v", toFind.AsSlice())
}

func (tc RelationshipChecker) VerifyOrderedIteratorResults(iter datastore.RelationshipIterator, rels ...tuple.Relationship) options.Cursor {
	expected := make([]tuple.Relationship, 0, len(rels))
	for rel, err := range iter {
		tc.Require.NoError(err)
		expected = append(expected, rel)
	}

	var cursor options.Cursor
	for index, rel := range rels {
		expectedStr := tuple.MustString(rel)

		if index > len(expected)-1 {
			tc.Require.Fail("expected %s, but found no additional results", expectedStr)
		}

		foundStr := tuple.MustString(expected[index])
		tc.Require.Equal(expectedStr, foundStr)

		cursor = options.ToCursor(rel)
	}
	return cursor
}

func (tc RelationshipChecker) RelationshipExists(ctx context.Context, rel tuple.Relationship, rev datastore.Revision) {
	iter := tc.ExactRelationshipIterator(ctx, rel, rev)
	tc.VerifyIteratorResults(iter, rel)
}

func (tc RelationshipChecker) NoRelationshipExists(ctx context.Context, rel tuple.Relationship, rev datastore.Revision) {
	iter := tc.ExactRelationshipIterator(ctx, rel, rev)
	tc.VerifyIteratorResults(iter)
}
