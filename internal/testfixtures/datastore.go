package testfixtures

import (
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

var UserNS = ns.Namespace("user")

var DocumentNS = ns.Namespace(
	"document",
	ns.Relation("owner", nil),
	ns.Relation("editor", ns.Union(
		ns.This(),
		ns.ComputedUserset("owner"),
	)),
	ns.Relation("parent", nil),
	ns.Relation("lock", nil),
	ns.Relation("viewer", ns.Union(
		ns.This(),
		ns.ComputedUserset("editor"),
		ns.TupleToUserset("parent", "viewer"),
	)),
)

var FolderNS = ns.Namespace(
	"folder",
	ns.Relation("owner", nil),
	ns.Relation("parent", nil),
	ns.Relation("editor", ns.Union(
		ns.This(),
		ns.ComputedUserset("owner"),
	)),
	ns.Relation("viewer", ns.Union(
		ns.This(),
		ns.ComputedUserset("editor"),
		ns.TupleToUserset("parent", "viewer"),
	)),
)

var StandardTuples = []string{
	"document:masterplan#parent@folder:strategy#...",
	"folder:strategy#parent@folder:company#...",
	"folder:company#owner@user:owner#...",
	"folder:company#viewer@user:legal#...",
	"folder:strategy#owner@user:vp_product#...",
	"document:masterplan#owner@user:pm#...",
	"document:masterplan#viewer@user:eng_lead#...",
	"document:masterplan#parent@folder:plans#...",
	"folder:plans#viewer@user:cfo#...",
	"folder:auditors#viewer@user:auditor#...",
	"folder:company#viewer@folder:auditors#viewer",
	"document:healthplan#parent@folder:plans#...",
	"folder:isolated#viewer@user:villain#...",
}

func StandardDatastoreWithSchema(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, uint64) {
	var lastRevision uint64
	for _, namespace := range []*pb.NamespaceDefinition{UserNS, DocumentNS, FolderNS} {
		var err error
		lastRevision, err = ds.WriteNamespace(namespace)
		require.NoError(err)
	}

	return ds, lastRevision
}

func StandardDatastoreWithData(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, uint64) {
	ds, _ = StandardDatastoreWithSchema(ds, require)

	var mutations []*pb.RelationTupleUpdate
	for _, tupleStr := range StandardTuples {
		tuple := tuple.Scan(tupleStr)
		require.NotNil(tuple)

		mutations = append(mutations, C(tuple))
	}

	revision, err := ds.WriteTuples(NoPreconditions, mutations)
	require.NoError(err)

	return ds, revision
}

var NoPreconditions = []*pb.RelationTuple{}

type TupleChecker struct {
	Require *require.Assertions
	DS      datastore.Datastore
}

func (tc TupleChecker) ExactTupleIterator(tpl *pb.RelationTuple, rev uint64) datastore.TupleIterator {
	iter, err := tc.DS.QueryTuples(tpl.ObjectAndRelation.Namespace, rev).
		WithObjectID(tpl.ObjectAndRelation.ObjectId).
		WithRelation(tpl.ObjectAndRelation.Relation).
		WithUserset(tpl.User.GetUserset()).
		Execute()

	tc.Require.NoError(err)
	return iter
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
		tc.Require.True(ok)
		delete(toFind, foundStr)
	}
	tc.Require.NoError(iter.Err())

	tc.Require.Zero(len(toFind), "Should not be any extra to find")
}

func (tc TupleChecker) TupleExists(tpl *pb.RelationTuple, rev uint64) {
	iter := tc.ExactTupleIterator(tpl, rev)
	tc.VerifyIteratorResults(iter, tpl)
}

func (tc TupleChecker) NoTupleExists(tpl *pb.RelationTuple, rev uint64) {
	iter := tc.ExactTupleIterator(tpl, rev)
	tc.VerifyIteratorResults(iter)
}
