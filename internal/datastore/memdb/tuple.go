package memdb

import (
	"fmt"
	"time"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errUnableToInstantiateTuplestore = "unable to instantiate datastore: %w"
	errUnableToWriteTuples           = "unable to write tuples: %w"
	errUnableToQueryTuples           = "unable to query tuples: %w"
	errRevision                      = "unable to find revision: %w"
)

const deletedTransactionID = ^uint64(0)

func (mds *memdbDatastore) WriteTuples(preconditions []*pb.RelationTuple, mutations []*pb.RelationTupleUpdate) (uint64, error) {
	txn := mds.db.Txn(true)
	defer txn.Abort()

	// Check the preconditions
	for _, expectedTuple := range preconditions {
		found, err := findTuple(txn, expectedTuple)
		if err != nil {
			return 0, fmt.Errorf(errUnableToWriteTuples, err)
		}

		if found == nil {
			return 0, datastore.ErrPreconditionFailed
		}
	}

	// Create the changelog entry
	newChangelogID, err := nextTupleChangelogID(txn)
	if err != nil {
		return 0, fmt.Errorf(errUnableToWriteTuples, err)
	}

	newChangelogEntry := &tupleChangelog{
		id:        newChangelogID,
		timestamp: time.Now(),
		changes:   mutations,
	}

	if err := txn.Insert(tableChangelog, newChangelogEntry); err != nil {
		return 0, fmt.Errorf(errUnableToWriteTuples, err)
	}

	// Apply the mutations
	for _, mutation := range mutations {
		newVersion := &tupleEntry{
			namespace:        mutation.Tuple.ObjectAndRelation.Namespace,
			objectID:         mutation.Tuple.ObjectAndRelation.ObjectId,
			relation:         mutation.Tuple.ObjectAndRelation.Relation,
			usersetNamespace: mutation.Tuple.User.GetUserset().Namespace,
			usersetObjectID:  mutation.Tuple.User.GetUserset().ObjectId,
			usersetRelation:  mutation.Tuple.User.GetUserset().Relation,
			createdTxn:       newChangelogID,
			deletedTxn:       deletedTransactionID,
		}

		existing, err := findTuple(txn, mutation.Tuple)
		if err != nil {
			return 0, fmt.Errorf(errUnableToWriteTuples, err)
		}

		var deletedExisting tupleEntry
		if existing != nil {
			deletedExisting = *existing
			deletedExisting.deletedTxn = newChangelogID
		}

		switch mutation.Operation {
		case pb.RelationTupleUpdate_CREATE:
			if err := txn.Insert(tableTuple, newVersion); err != nil {
				return 0, fmt.Errorf(errUnableToWriteTuples, err)
			}
		case pb.RelationTupleUpdate_DELETE:
			if existing == nil {
				return 0, datastore.ErrPreconditionFailed
			}
			if err := txn.Insert(tableTuple, &deletedExisting); err != nil {
				return 0, fmt.Errorf(errUnableToWriteTuples, err)
			}
		case pb.RelationTupleUpdate_TOUCH:
			if existing != nil {
				if err := txn.Insert(tableTuple, &deletedExisting); err != nil {
					return 0, fmt.Errorf(errUnableToWriteTuples, err)
				}
			}
			if err := txn.Insert(tableTuple, newVersion); err != nil {
				return 0, fmt.Errorf(errUnableToWriteTuples, err)
			}
		default:
			return 0, fmt.Errorf(
				errUnableToWriteTuples,
				fmt.Errorf("unknown tuple mutation operation type: %s", mutation.Operation),
			)
		}
	}

	txn.Commit()

	return newChangelogID, nil
}

func (mds *memdbDatastore) QueryTuples(namespace string, revision uint64) datastore.TupleQuery {
	return &memdbTupleQuery{
		db:        mds.db,
		namespace: namespace,
		revision:  revision,
	}
}

func (mds *memdbDatastore) Revision() (uint64, error) {
	// Compute the current revision
	txn := mds.db.Txn(false)
	defer txn.Abort()

	lastRaw, err := txn.Last(tableChangelog, indexID)
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}
	if lastRaw != nil {
		return lastRaw.(*tupleChangelog).id, nil
	}
	return 0, nil
}

func findTuple(txn *memdb.Txn, toFind *pb.RelationTuple) (*tupleEntry, error) {
	foundRaw, err := txn.First(
		tableTuple,
		indexLive,
		toFind.ObjectAndRelation.Namespace,
		toFind.ObjectAndRelation.ObjectId,
		toFind.ObjectAndRelation.Relation,
		toFind.User.GetUserset().Namespace,
		toFind.User.GetUserset().ObjectId,
		toFind.User.GetUserset().Relation,
		deletedTransactionID,
	)
	if err != nil {
		return nil, err
	}

	if foundRaw == nil {
		return nil, nil
	}

	return foundRaw.(*tupleEntry), nil
}

func nextTupleChangelogID(txn *memdb.Txn) (uint64, error) {
	lastChangeRaw, err := txn.Last(tableChangelog, indexID)
	if err != nil {
		return 0, err
	}

	if lastChangeRaw == nil {
		return 1, nil
	}

	return lastChangeRaw.(*tupleChangelog).id + 1, nil
}
