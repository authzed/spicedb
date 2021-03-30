package memdb

import (
	"fmt"
	"runtime"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

type memdbTupleQuery struct {
	db        *memdb.MemDB
	namespace string
	revision  uint64

	objectIDFilter *string
	relationFilter *string
	usersetFilter  *pb.ObjectAndRelation
}

func (mtq *memdbTupleQuery) WithObjectID(objectID string) datastore.TupleQuery {
	copy := *mtq
	(&copy).objectIDFilter = &objectID
	return &copy
}

func (mtq *memdbTupleQuery) WithRelation(relation string) datastore.TupleQuery {
	copy := *mtq
	(&copy).relationFilter = &relation
	return &copy
}

func (mtq *memdbTupleQuery) WithUserset(userset *pb.ObjectAndRelation) datastore.TupleQuery {
	copy := *mtq
	(&copy).usersetFilter = userset
	return &copy
}

func (mtq *memdbTupleQuery) Execute() (datastore.TupleIterator, error) {
	txn := mtq.db.Txn(false)

	var err error
	if mtq.relationFilter != nil {
		err = verifyNamespaceAndRelation(txn, mtq.namespace, *mtq.relationFilter, false)
	} else {
		err = verifyNamespaceAndRelation(txn, mtq.namespace, datastore.Ellipsis, true)
	}
	if err != nil {
		txn.Abort()
		return nil, err
	}

	var bestIterator memdb.ResultIterator
	if mtq.objectIDFilter != nil {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespaceAndObjectID,
			mtq.namespace,
			*mtq.objectIDFilter,
		)
	} else if mtq.usersetFilter != nil {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespaceAndUserset,
			mtq.namespace,
			mtq.usersetFilter.Namespace,
			mtq.usersetFilter.ObjectId,
			mtq.usersetFilter.Relation,
		)
	} else if mtq.relationFilter != nil {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespaceAndRelation,
			mtq.namespace,
			*mtq.relationFilter,
		)
	} else {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespace,
			mtq.namespace,
		)
	}

	if err != nil {
		txn.Abort()
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	filteredIterator := memdb.NewFilterIterator(bestIterator, func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*tupleEntry)

		if mtq.objectIDFilter != nil && *mtq.objectIDFilter != tuple.objectID {
			return true
		}
		if mtq.relationFilter != nil && *mtq.relationFilter != tuple.relation {
			return true
		}
		if mtq.usersetFilter != nil && (mtq.usersetFilter.Namespace != tuple.usersetNamespace ||
			mtq.usersetFilter.ObjectId != tuple.usersetObjectID ||
			mtq.usersetFilter.Relation != tuple.usersetRelation) {
			return true
		}
		if mtq.revision < tuple.createdTxn || mtq.revision >= tuple.deletedTxn {
			return true
		}
		return false
	})

	iter := &memdbTupleIterator{
		mtq: mtq,
		txn: txn,
		it:  filteredIterator,
	}

	runtime.SetFinalizer(iter, func(iter *memdbTupleIterator) {
		if iter.txn != nil {
			panic("Tuple iterator garbage collected before Close() was called")
		}
	})

	return iter, nil
}

type memdbTupleIterator struct {
	mtq *memdbTupleQuery
	txn *memdb.Txn
	it  memdb.ResultIterator
}

func (mti *memdbTupleIterator) Next() *pb.RelationTuple {
	foundRaw := mti.it.Next()
	if foundRaw == nil {
		return nil
	}

	found := foundRaw.(*tupleEntry)

	return &pb.RelationTuple{
		ObjectAndRelation: &pb.ObjectAndRelation{
			Namespace: found.namespace,
			ObjectId:  found.objectID,
			Relation:  found.relation,
		},
		User: &pb.User{
			UserOneof: &pb.User_Userset{
				Userset: &pb.ObjectAndRelation{
					Namespace: found.usersetNamespace,
					ObjectId:  found.usersetObjectID,
					Relation:  found.usersetRelation,
				},
			},
		},
	}
}

func (mti *memdbTupleIterator) Err() error {
	return nil
}

func (mti *memdbTupleIterator) Close() {
	mti.txn.Abort()
	mti.txn = nil
}
