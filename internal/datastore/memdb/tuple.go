package memdb

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errUnableToWriteTuples = "unable to write tuples: %w"
	errUnableToQueryTuples = "unable to query tuples: %w"
	errRevision            = "unable to find revision: %w"
	errCheckRevision       = "unable to check revision: %w"
)

const deletedTransactionID = ^uint64(0)

func (mds *memdbDatastore) WriteTuples(ctx context.Context, preconditions []*pb.RelationTuple, mutations []*pb.RelationTupleUpdate) (uint64, error) {
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
	time.Sleep(mds.simulatedLatency)
	newChangelogID, err := nextTupleChangelogID(txn)
	if err != nil {
		return 0, fmt.Errorf(errUnableToWriteTuples, err)
	}

	newChangelogEntry := &tupleChangelog{
		id:        newChangelogID,
		timestamp: uint64(time.Now().UnixNano()),
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
			if existing != nil {
				if err := txn.Insert(tableTuple, &deletedExisting); err != nil {
					return 0, fmt.Errorf(errUnableToWriteTuples, err)
				}
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
		db:               mds.db,
		namespace:        namespace,
		revision:         revision,
		simulatedLatency: mds.simulatedLatency,
	}
}

func (mds *memdbDatastore) ReverseQueryTuples(revision uint64) datastore.ReverseTupleQuery {
	return &memdbReverseTupleQuery{
		db:               mds.db,
		revision:         revision,
		simulatedLatency: mds.simulatedLatency,
	}
}

func (mds *memdbDatastore) SyncRevision(ctx context.Context) (uint64, error) {
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

func (mds *memdbDatastore) Revision(ctx context.Context) (uint64, error) {
	txn := mds.db.Txn(false)
	defer txn.Abort()

	lowerBound := uint64(time.Now().Add(-1 * mds.revisionFuzzingTimedelta).UnixNano())

	time.Sleep(mds.simulatedLatency)
	iter, err := txn.LowerBound(tableChangelog, indexTimestamp, lowerBound)
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}

	var candidates []uint64
	for oneChange := iter.Next(); oneChange != nil; oneChange = iter.Next() {
		candidates = append(candidates, oneChange.(*tupleChangelog).id)
	}

	if len(candidates) > 0 {
		return candidates[rand.Intn(len(candidates))], nil
	} else {
		return mds.SyncRevision(ctx)
	}
}

func (mds *memdbDatastore) CheckRevision(ctx context.Context, revision uint64) error {
	txn := mds.db.Txn(false)
	defer txn.Abort()

	// We need to know the highest possible revision
	time.Sleep(mds.simulatedLatency)
	lastRaw, err := txn.Last(tableChangelog, indexID)
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}
	if lastRaw == nil {
		return datastore.ErrInvalidRevision
	}

	highest := lastRaw.(*tupleChangelog).id

	if revision > highest {
		return datastore.ErrInvalidRevision
	}

	lowerBound := uint64(time.Now().Add(mds.gcWindowInverted).UnixNano())
	time.Sleep(mds.simulatedLatency)
	iter, err := txn.LowerBound(tableChangelog, indexTimestamp, lowerBound)
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	firstValid := iter.Next()
	if firstValid == nil && revision != highest {
		return datastore.ErrInvalidRevision
	}

	if firstValid != nil && revision < firstValid.(*tupleChangelog).id {
		return datastore.ErrInvalidRevision
	}

	return nil
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
