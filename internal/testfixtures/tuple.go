package testfixtures

import (
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

func ONR(ns, oid, rel string) *pb.ObjectAndRelation {
	return &pb.ObjectAndRelation{
		Namespace: ns,
		ObjectId:  oid,
		Relation:  rel,
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
