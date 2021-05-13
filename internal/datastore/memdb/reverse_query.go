package memdb

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

type memdbReverseTupleQuery struct {
	db       *memdb.MemDB
	revision uint64

	namespaceName string
	relationName  string
	usersetFilter *pb.ObjectAndRelation

	simulatedLatency time.Duration
}

func (mtq memdbReverseTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	txn := mtq.db.Txn(false)

	time.Sleep(mtq.simulatedLatency)
	bestIterator, err := txn.Get(
		tableTuple,
		indexRelationAndUserset,
		mtq.usersetFilter.Namespace,
		mtq.usersetFilter.ObjectId,
		mtq.usersetFilter.Relation,
		mtq.namespaceName,
		mtq.relationName,
	)

	if err != nil {
		txn.Abort()
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	filteredIterator := memdb.NewFilterIterator(bestIterator, func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*tupleEntry)
		if mtq.revision < tuple.createdTxn || mtq.revision >= tuple.deletedTxn {
			return true
		}
		return false
	})

	iter := &memdbTupleIterator{
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
