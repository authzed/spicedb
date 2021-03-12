package testfixtures

import (
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/stretchr/testify/require"
)

var UserNS = Namespace("user")

var DocumentNS = Namespace(
	"document",
	Relation("owner", nil),
	Relation("editor", Union(
		This(),
		ComputedUserset("owner"),
	)),
	Relation("parent", nil),
	Relation("lock", nil),
	Relation("viewer", Union(
		This(),
		ComputedUserset("editor"),
		TupleToUserset("parent", "viewer"),
	)),
)

var FolderNS = Namespace(
	"folder",
	Relation("owner", nil),
	Relation("parent", nil),
	Relation("editor", Union(
		This(),
		ComputedUserset("owner"),
	)),
	Relation("viewer", Union(
		This(),
		ComputedUserset("editor"),
		TupleToUserset("parent", "viewer"),
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

func StandardDatastore(require *require.Assertions) datastore.Datastore {
	ds, err := memdb.NewMemdbDatastore(0)
	require.NoError(err)

	return ds
}

func StandardDatastoreWithSchema(require *require.Assertions) (datastore.Datastore, uint64) {
	ds := StandardDatastore(require)

	var lastRevision uint64
	for _, namespace := range []*pb.NamespaceDefinition{UserNS, DocumentNS, FolderNS} {
		var err error
		lastRevision, err = ds.WriteNamespace(namespace)
		require.NoError(err)
	}

	return ds, lastRevision
}

func StandardDatastoreWithData(require *require.Assertions) (datastore.Datastore, uint64) {
	ds, _ := StandardDatastoreWithSchema(require)

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

func Namespace(name string, relations ...*pb.Relation) *pb.NamespaceDefinition {
	return &pb.NamespaceDefinition{
		Name:     name,
		Relation: relations,
	}
}

func Relation(name string, rewrite *pb.UsersetRewrite) *pb.Relation {
	return &pb.Relation{
		Name:           name,
		UsersetRewrite: rewrite,
	}
}

func Union(children ...*pb.SetOperation_Child) *pb.UsersetRewrite {
	return &pb.UsersetRewrite{
		RewriteOperation: &pb.UsersetRewrite_Union{
			Union: &pb.SetOperation{
				Child: children,
			},
		},
	}
}

func This() *pb.SetOperation_Child {
	return &pb.SetOperation_Child{
		ChildType: &pb.SetOperation_Child_XThis{},
	}
}

func ComputedUserset(relation string) *pb.SetOperation_Child {
	return &pb.SetOperation_Child{
		ChildType: &pb.SetOperation_Child_ComputedUserset{
			ComputedUserset: &pb.ComputedUserset{
				Relation: "owner",
			},
		},
	}
}

func TupleToUserset(tuplesetRelation, usersetRelation string) *pb.SetOperation_Child {
	return &pb.SetOperation_Child{
		ChildType: &pb.SetOperation_Child_TupleToUserset{
			TupleToUserset: &pb.TupleToUserset{
				Tupleset: &pb.TupleToUserset_Tupleset{
					Relation: tuplesetRelation,
				},
				ComputedUserset: &pb.ComputedUserset{
					Relation: usersetRelation,
					Object:   pb.ComputedUserset_TUPLE_USERSET_OBJECT,
				},
			},
		},
	}
}

func C(tpl *pb.RelationTuple) *pb.RelationTupleUpdate {
	return &pb.RelationTupleUpdate{
		Operation: pb.RelationTupleUpdate_CREATE,
		Tuple:     tpl,
	}
}

func T(tpl *pb.RelationTuple) *pb.RelationTupleUpdate {
	return &pb.RelationTupleUpdate{
		Operation: pb.RelationTupleUpdate_TOUCH,
		Tuple:     tpl,
	}
}

func D(tpl *pb.RelationTuple) *pb.RelationTupleUpdate {
	return &pb.RelationTupleUpdate{
		Operation: pb.RelationTupleUpdate_DELETE,
		Tuple:     tpl,
	}
}

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
