package testfixtures

import (
	"context"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var UserNS = ns.Namespace("user")

var DocumentNS = ns.Namespace(
	"document",
	ns.Relation("owner",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.Relation("editor",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.Relation("viewer",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.Relation("viewer_and_editor",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.Relation("parent", nil, ns.AllowedRelation("folder", "...")),
	ns.Relation("edit",
		ns.Union(
			ns.ComputedUserset("owner"),
			ns.ComputedUserset("editor"),
		),
	),
	ns.Relation("view",
		ns.Union(
			ns.ComputedUserset("viewer"),
			ns.ComputedUserset("edit"),
			ns.TupleToUserset("parent", "view"),
		),
	),
	ns.Relation("view_and_edit",
		ns.Intersection(
			ns.ComputedUserset("viewer_and_editor"),
			ns.ComputedUserset("edit"),
		),
	),
)

var FolderNS = ns.Namespace(
	"folder",
	ns.Relation("owner",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.Relation("editor",
		nil,
		ns.AllowedRelation("user", "..."),
	),
	ns.Relation("viewer",
		nil,
		ns.AllowedRelation("user", "..."),
		ns.AllowedRelation("folder", "viewer"),
	),
	ns.Relation("parent", nil, ns.AllowedRelation("folder", "...")),
	ns.Relation("edit",
		ns.Union(
			ns.ComputedUserset("editor"),
			ns.ComputedUserset("owner"),
		),
	),
	ns.Relation("view",
		ns.Union(
			ns.ComputedUserset("viewer"),
			ns.ComputedUserset("edit"),
			ns.TupleToUserset("parent", "view"),
		),
	),
)

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
}

func EmptyDatastore(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	rev, err := ds.HeadRevision(context.Background())
	require.NoError(err)
	return ds, rev
}

func StandardDatastoreWithSchema(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	ctx := context.Background()
	validating := NewValidatingDatastore(ds)

	allDefs := []*core.NamespaceDefinition{UserNS, FolderNS, DocumentNS}

	newRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		for _, nsDef := range allDefs {
			ts, err := namespace.BuildNamespaceTypeSystemWithFallback(nsDef, rwt, allDefs)
			require.NoError(err)

			vts, err := ts.Validate(ctx)
			require.NoError(err)

			aerr := namespace.AnnotateNamespace(vts)
			require.NoError(aerr)

			err = rwt.WriteNamespaces(nsDef)
			require.NoError(err)
		}

		return nil
	})
	require.NoError(err)

	return validating, newRevision
}

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
	defer iter.Close()

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
		toFind[tuple.String(tpl)] = struct{}{}
	}

	for found := iter.Next(); found != nil; found = iter.Next() {
		tc.Require.NoError(iter.Err())
		foundStr := tuple.String(found)
		_, ok := toFind[foundStr]
		tc.Require.True(ok, "found unexpected tuple %s in iterator", foundStr)
		delete(toFind, foundStr)
	}
	tc.Require.NoError(iter.Err())

	tc.Require.Zero(len(toFind), "did not find some expected tuples: %#v", toFind)
}

func (tc TupleChecker) TupleExists(ctx context.Context, tpl *core.RelationTuple, rev datastore.Revision) {
	iter := tc.ExactRelationshipIterator(ctx, tpl, rev)
	tc.VerifyIteratorResults(iter, tpl)
}

func (tc TupleChecker) NoTupleExists(ctx context.Context, tpl *core.RelationTuple, rev datastore.Revision) {
	iter := tc.ExactRelationshipIterator(ctx, tpl, rev)
	tc.VerifyIteratorResults(iter)
}
