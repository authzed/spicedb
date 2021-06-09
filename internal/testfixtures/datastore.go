package testfixtures

import (
	"context"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

var UserNS = ns.Namespace("user")

var DocumentNS = ns.Namespace(
	"document",
	ns.Relation("owner",
		nil,
		ns.RelationReference("user", "..."),
	),
	ns.Relation("editor",
		ns.Union(
			ns.This(),
			ns.ComputedUserset("owner"),
		),
		ns.RelationReference("user", "..."),
	),
	ns.Relation("parent", nil, ns.RelationReference("folder", "...")),
	ns.Relation("lock", nil),
	ns.Relation("viewer",
		ns.Union(
			ns.This(),
			ns.ComputedUserset("editor"),
			ns.TupleToUserset("parent", "viewer"),
		),
		ns.RelationReference("user", "..."),
	),
	ns.Relation("viewer_and_editor",
		ns.Intersection(
			ns.This(),
			ns.ComputedUserset("editor"),
		),
		ns.RelationReference("user", "..."),
	),
	ns.Relation("viewer_and_editor_derived",
		ns.Union(
			ns.This(),
			ns.ComputedUserset("viewer_and_editor"),
		),
		ns.RelationReference("user", "..."),
	),
)

var FolderNS = ns.Namespace(
	"folder",
	ns.Relation("owner",
		nil,
		ns.RelationReference("user", "..."),
	),
	ns.Relation("parent", nil, ns.RelationReference("folder", "...")),
	ns.Relation("editor",
		ns.Union(
			ns.This(),
			ns.ComputedUserset("owner"),
		),
		ns.RelationReference("user", "..."),
	),
	ns.Relation("viewer",
		ns.Union(
			ns.This(),
			ns.ComputedUserset("editor"),
			ns.TupleToUserset("parent", "viewer"),
		),
		ns.RelationReference("user", "..."),
		ns.RelationReference("folder", "viewer"),
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

func StandardDatastoreWithSchema(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	ctx := context.Background()

	var lastRevision datastore.Revision
	for _, namespace := range []*pb.NamespaceDefinition{UserNS, FolderNS, DocumentNS} {
		var err error
		lastRevision, err = ds.WriteNamespace(ctx, namespace)
		require.NoError(err)
	}

	return ds, lastRevision
}

func StandardDatastoreWithData(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
	ds, _ = StandardDatastoreWithSchema(ds, require)
	ctx := context.Background()

	var revision datastore.Revision
	for _, tupleStr := range StandardTuples {
		tpl := tuple.Scan(tupleStr)
		require.NotNil(tpl)

		var err error
		revision, err = ds.WriteTuples(ctx, nil, []*pb.RelationTupleUpdate{tuple.Create(tpl)})
		require.NoError(err)
	}

	return ds, revision
}

type TupleChecker struct {
	Require *require.Assertions
	DS      datastore.Datastore
}

func (tc TupleChecker) ExactTupleIterator(ctx context.Context, tpl *pb.RelationTuple, rev datastore.Revision) datastore.TupleIterator {
	iter, err := tc.DS.QueryTuples(tpl.ObjectAndRelation.Namespace, rev).
		WithObjectID(tpl.ObjectAndRelation.ObjectId).
		WithRelation(tpl.ObjectAndRelation.Relation).
		WithUserset(tpl.User.GetUserset()).
		Execute(ctx)

	tc.Require.NoError(err)
	return iter
}

func (tc TupleChecker) VerifyIteratorCount(iter datastore.TupleIterator, count int) {
	defer iter.Close()

	foundCount := 0
	for found := iter.Next(); found != nil; found = iter.Next() {
		foundCount += 1
	}
	tc.Require.NoError(iter.Err())
	tc.Require.Equal(count, foundCount)
}

func (tc TupleChecker) VerifyIteratorResults(iter datastore.TupleIterator, tpls ...*pb.RelationTuple) {
	defer iter.Close()

	toFind := make(map[string]struct{}, 1024)

	for _, tpl := range tpls {
		toFind[tuple.String(tpl)] = struct{}{}
	}

	for found := iter.Next(); found != nil; found = iter.Next() {
		tc.Require.NoError(iter.Err())
		foundStr := tuple.String(found)
		_, ok := toFind[foundStr]
		tc.Require.True(ok, "Did not find expected tuple %s in %v", foundStr, toFind)
		delete(toFind, foundStr)
	}
	tc.Require.NoError(iter.Err())

	tc.Require.Zero(len(toFind), "Should not be any extra to find")
}

func (tc TupleChecker) TupleExists(ctx context.Context, tpl *pb.RelationTuple, rev datastore.Revision) {
	iter := tc.ExactTupleIterator(ctx, tpl, rev)
	tc.VerifyIteratorResults(iter, tpl)
}

func (tc TupleChecker) NoTupleExists(ctx context.Context, tpl *pb.RelationTuple, rev datastore.Revision) {
	iter := tc.ExactTupleIterator(ctx, tpl, rev)
	tc.VerifyIteratorResults(iter)
}
