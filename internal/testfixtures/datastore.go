package testfixtures

import (
	"context"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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
	),
	ns.MustRelation("editor",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.MustRelation("viewer",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.MustRelation("viewer_and_editor",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.MustRelation("caveated_viewer",
		nil,
		ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("test")),
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
	),
	ns.MustRelation("editor",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.MustRelation("viewer",
		nil,
		ns.AllowedRelation("user", "..."),
		ns.AllowedRelation("folder", "viewer"),
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

// StandardTuples defines standard tuples for tests.
// NOTE: some tests index directly into this slice, so if you're adding a new tuple, add it
// at the *end*.
var StandardTuples = []string{
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

	tuples := make([]*core.RelationTuple, 0, len(StandardTuples))
	for _, tupleStr := range StandardTuples {
		tpl := tuple.Parse(tupleStr)
		require.NotNil(tpl)
		tuples = append(tuples, tpl)
	}
	revision, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tuples...)
	require.NoError(err)

	return ds, revision
}

// StandardDatastoreWithCaveatedData returns a datastore populated with both the standard test definitions
// and some caveated relationships.
func StandardDatastoreWithCaveatedData(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	ds, _ = StandardDatastoreWithSchema(ds, require)
	ctx := context.Background()

	_, err := ds.ReadWriteTx(ctx, func(tx datastore.ReadWriteTransaction) error {
		return tx.WriteCaveats(ctx, createTestCaveat(require))
	})
	require.NoError(err)

	caveatedTpls := make([]*core.RelationTuple, 0, len(StandardTuples))
	for _, tupleStr := range StandardTuples {
		tpl := tuple.Parse(tupleStr)
		require.NotNil(tpl)
		tpl.Caveat = &core.ContextualizedCaveat{
			CaveatName: "test",
			Context:    mustProtoStruct(map[string]any{"expectedSecret": "1234"}),
		}
		caveatedTpls = append(caveatedTpls, tpl)
	}
	revision, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, caveatedTpls...)
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
func DatastoreFromSchemaAndTestRelationships(ds datastore.Datastore, schema string, relationships []*core.RelationTuple, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	ctx := context.Background()
	validating := NewValidatingDatastore(ds)

	emptyDefaultPrefix := ""
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schema,
	}, &emptyDefaultPrefix)
	require.NoError(err)

	_ = writeDefinitions(validating, require, compiled.ObjectDefinitions, compiled.CaveatDefinitions)

	newRevision, err := validating.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		mutations := make([]*core.RelationTupleUpdate, 0, len(relationships))
		for _, rel := range relationships {
			mutations = append(mutations, tuple.Create(rel.CloneVT()))
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
	newRevision, err := ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		if len(caveatDefs) > 0 {
			err := rwt.WriteCaveats(ctx, caveatDefs)
			require.NoError(err)
		}

		for _, nsDef := range objectDefs {
			ts, err := namespace.NewNamespaceTypeSystem(nsDef,
				namespace.ResolverForDatastoreReader(rwt).WithPredefinedElements(namespace.PredefinedElements{
					Namespaces: objectDefs,
					Caveats:    caveatDefs,
				}))
			require.NoError(err)

			vts, err := ts.Validate(ctx)
			require.NoError(err)

			aerr := namespace.AnnotateNamespace(vts)
			require.NoError(aerr)

			err = rwt.WriteNamespaces(ctx, nsDef)
			require.NoError(err)
		}

		return nil
	})
	require.NoError(err)
	return newRevision
}

// TupleChecker is a helper type which provides an easy way for collecting relationships/tuples from
// an iterator and verify those found.
type TupleChecker struct {
	Require *require.Assertions
	DS      datastore.Datastore
}

func (tc TupleChecker) ExactRelationshipIterator(ctx context.Context, tpl *core.RelationTuple, rev datastore.Revision) datastore.RelationshipIterator {
	filter := tuple.MustToFilter(tpl)
	iter, err := tc.DS.SnapshotReader(rev).QueryRelationships(ctx, datastore.RelationshipsFilterFromPublicFilter(filter))
	tc.Require.NoError(err)
	return iter
}

func (tc TupleChecker) VerifyIteratorCount(iter datastore.RelationshipIterator, count int) {
	foundCount := 0
	for found := iter.Next(); found != nil; found = iter.Next() {
		foundCount++
	}
	tc.Require.NoError(iter.Err())
	tc.Require.Equal(count, foundCount)
}

func (tc TupleChecker) VerifyIteratorResults(iter datastore.RelationshipIterator, tpls ...*core.RelationTuple) {
	defer iter.Close()

	toFind := make(map[string]struct{}, 1024)

	for _, tpl := range tpls {
		toFind[tuple.MustString(tpl)] = struct{}{}
	}

	for found := iter.Next(); found != nil; found = iter.Next() {
		tc.Require.NoError(iter.Err())
		foundStr := tuple.MustString(found)
		_, ok := toFind[foundStr]
		tc.Require.True(ok, "found unexpected tuple %s in iterator", foundStr)
		delete(toFind, foundStr)
	}
	tc.Require.NoError(iter.Err())

	tc.Require.Zero(len(toFind), "did not find some expected tuples: %#v", toFind)
}

func (tc TupleChecker) VerifyOrderedIteratorResults(iter datastore.RelationshipIterator, tpls ...*core.RelationTuple) {
	for _, tpl := range tpls {
		expectedStr := tuple.MustString(tpl)

		found := iter.Next()
		tc.Require.NotNil(found, "expected %s, but found no additional results", expectedStr)

		foundStr := tuple.MustString(found)
		tc.Require.Equal(expectedStr, foundStr)
	}

	pastLast := iter.Next()
	tc.Require.Nil(pastLast)
	tc.Require.Nil(iter.Err())
}

func (tc TupleChecker) TupleExists(ctx context.Context, tpl *core.RelationTuple, rev datastore.Revision) {
	iter := tc.ExactRelationshipIterator(ctx, tpl, rev)
	tc.VerifyIteratorResults(iter, tpl)
}

func (tc TupleChecker) NoTupleExists(ctx context.Context, tpl *core.RelationTuple, rev datastore.Revision) {
	iter := tc.ExactRelationshipIterator(ctx, tpl, rev)
	tc.VerifyIteratorResults(iter)
}

func mustProtoStruct(in map[string]any) *structpb.Struct {
	out, err := structpb.NewStruct(in)
	if err != nil {
		panic(err)
	}
	return out
}
