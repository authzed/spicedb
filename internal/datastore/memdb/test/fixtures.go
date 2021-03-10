package memdbtest

import (
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/stretchr/testify/require"
)

var userNS = namespace("user")

var documentNS = namespace(
	"document",
	relation("owner", nil),
	relation("editor", union(
		this(),
		computedUserset("owner"),
	)),
	relation("parent", nil),
	relation("lock", nil),
	relation("viewer", union(
		this(),
		computedUserset("editor"),
		tupleToUserset("parent", "viewer"),
	)),
)

var lockNS = namespace("lock", relation("parent", nil))

var folderNS = namespace(
	"folder",
	relation("owner", nil),
	relation("parent", nil),
	relation("editor", union(
		this(),
		computedUserset("owner"),
	)),
	relation("viewer", union(
		this(),
		computedUserset("editor"),
		tupleToUserset("parent", "viewer"),
	)),
)

var standardTuples = []string{
	"document:masterplan#parent@folder:strategy#...",
	"document:masterplan#lock@lock:masterplan#...",
	"folder:strategy#parent@folder:company#...",
	"folder:company#owner@user:1#...",
	"folder:company#viewer@user:2#...",
	"folder:strategy#owner@user:3#...",
	"document:masterplan#owner@user:4#...",
	"document:masterplan#viewer@user:5#...",
	"document:masterplan#parent@folder:plans#...",
	"folder:plans#viewer@user:6#...",
	"document:healthplan#parent@folder:plans#...",
	"document:healthplan#lock@lock:healthplan#...",
	"folder:isolated#viewer@user:7#...",
}

func standardDatastore(require *require.Assertions) datastore.Datastore {
	ds, err := memdb.NewMemdbDatastore(0)
	require.NoError(err)

	return ds
}

func standardDatastoreWithSchema(require *require.Assertions) (datastore.Datastore, uint64) {
	ds := standardDatastore(require)

	var lastRevision uint64
	for _, namespace := range []*pb.NamespaceDefinition{userNS, documentNS, lockNS, folderNS} {
		var err error
		lastRevision, err = ds.WriteNamespace(namespace)
		require.NoError(err)
	}

	return ds, lastRevision
}

func standardDatastoreWithData(require *require.Assertions) (datastore.Datastore, uint64) {
	ds, _ := standardDatastoreWithSchema(require)

	var mutations []*pb.RelationTupleUpdate
	for _, tupleStr := range standardTuples {
		tuple := tuple.Scan(tupleStr)
		require.NotNil(tuple)

		mutations = append(mutations, c(tuple))
	}

	revision, err := ds.WriteTuples(noPreconditions, mutations)
	require.NoError(err)

	return ds, revision
}

var noPreconditions = []*pb.RelationTuple{}

func namespace(name string, relations ...*pb.Relation) *pb.NamespaceDefinition {
	return &pb.NamespaceDefinition{
		Name:     name,
		Relation: relations,
	}
}

func relation(name string, rewrite *pb.UsersetRewrite) *pb.Relation {
	return &pb.Relation{
		Name: name,
	}
}

func union(children ...*pb.SetOperation_Child) *pb.UsersetRewrite {
	return &pb.UsersetRewrite{
		RewriteOperation: &pb.UsersetRewrite_Union{
			Union: &pb.SetOperation{
				Child: children,
			},
		},
	}
}

func this() *pb.SetOperation_Child {
	return &pb.SetOperation_Child{
		ChildType: &pb.SetOperation_Child_XThis{},
	}
}

func computedUserset(relation string) *pb.SetOperation_Child {
	return &pb.SetOperation_Child{
		ChildType: &pb.SetOperation_Child_ComputedUserset{
			ComputedUserset: &pb.ComputedUserset{
				Relation: "owner",
			},
		},
	}
}

func tupleToUserset(tuplesetRelation, usersetRelation string) *pb.SetOperation_Child {
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

func c(tpl *pb.RelationTuple) *pb.RelationTupleUpdate {
	return &pb.RelationTupleUpdate{
		Operation: pb.RelationTupleUpdate_CREATE,
		Tuple:     tpl,
	}
}

func t(tpl *pb.RelationTuple) *pb.RelationTupleUpdate {
	return &pb.RelationTupleUpdate{
		Operation: pb.RelationTupleUpdate_TOUCH,
		Tuple:     tpl,
	}
}

func d(tpl *pb.RelationTuple) *pb.RelationTupleUpdate {
	return &pb.RelationTupleUpdate{
		Operation: pb.RelationTupleUpdate_DELETE,
		Tuple:     tpl,
	}
}

type tupleChecker struct {
	require *require.Assertions
	ds      datastore.Datastore
}

func (tc tupleChecker) exactTupleIterator(tpl *pb.RelationTuple, rev uint64) datastore.TupleIterator {
	iter, err := tc.ds.QueryTuples(tpl.ObjectAndRelation.Namespace, rev).
		WithObjectID(tpl.ObjectAndRelation.ObjectId).
		WithRelation(tpl.ObjectAndRelation.Relation).
		WithUserset(tpl.User.GetUserset()).
		Execute()

	tc.require.NoError(err)
	return iter
}

func (tc tupleChecker) verifyIteratorResults(iter datastore.TupleIterator, tpls ...*pb.RelationTuple) {
	defer iter.Close()

	toFind := make(map[string]struct{}, 1024)

	for _, tpl := range tpls {
		toFind[tuple.String(tpl)] = struct{}{}
	}

	for found := iter.Next(); found != nil; found = iter.Next() {
		tc.require.NoError(iter.Err())
		foundStr := tuple.String(found)
		_, ok := toFind[foundStr]
		tc.require.True(ok)
		delete(toFind, foundStr)
	}
	tc.require.NoError(iter.Err())

	tc.require.Zero(len(toFind), "Should not be any extra to find")
}

func (tc tupleChecker) tupleExists(tpl *pb.RelationTuple, rev uint64) {
	iter := tc.exactTupleIterator(tpl, rev)
	tc.verifyIteratorResults(iter, tpl)
}

func (tc tupleChecker) noTupleExists(tpl *pb.RelationTuple, rev uint64) {
	iter := tc.exactTupleIterator(tpl, rev)
	tc.verifyIteratorResults(iter)
}
