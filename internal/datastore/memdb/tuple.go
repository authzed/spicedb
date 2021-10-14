package memdb

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"
	"github.com/jzelinskie/stringz"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	errUnableToWriteTuples  = "unable to write tuples: %w"
	errUnableToDeleteTuples = "unable to delete tuples: %w"
	errUnableToQueryTuples  = "unable to query tuples: %w"
	errRevision             = "unable to find revision: %w"
	errCheckRevision        = "unable to check revision: %w"
)

const deletedTransactionID = ^uint64(0)

func (mds *memdbDatastore) checkPrecondition(txn *memdb.Txn, preconditions []*v1.Precondition) error {
	for _, precond := range preconditions {
		switch precond.Operation {
		case v1.Precondition_OPERATION_MUST_NOT_MATCH, v1.Precondition_OPERATION_MUST_MATCH:
			bestIter, err := iteratorForFilter(txn, precond.Filter)
			if err != nil {
				return err
			}

			filteredIter := memdb.NewFilterIterator(bestIter, relationshipFilterFilterFunc(precond.Filter))

			exists := filteredIter.Next() != nil
			if (precond.Operation == v1.Precondition_OPERATION_MUST_MATCH && !exists) ||
				(precond.Operation == v1.Precondition_OPERATION_MUST_NOT_MATCH && exists) {
				return datastore.NewPreconditionFailedErr(precond)
			}
		default:
			return fmt.Errorf("unspecified precondition operation")
		}
	}

	return nil
}

func (mds *memdbDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, mutations []*v1.RelationshipUpdate) (datastore.Revision, error) {
	db := mds.db
	if db == nil {
		return datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	txn := db.Txn(true)
	defer txn.Abort()

	if err := mds.checkPrecondition(txn, preconditions); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	newChangelogID, err := mds.write(ctx, txn, mutations)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	txn.Commit()

	return revisionFromVersion(newChangelogID), nil
}

func (mds *memdbDatastore) write(ctx context.Context, txn *memdb.Txn, mutations []*v1.RelationshipUpdate) (uint64, error) {
	// Create the changelog entry
	time.Sleep(mds.simulatedLatency)
	newChangelogID, err := nextTupleChangelogID(txn)
	if err != nil {
		return 0, err
	}

	changes := make([]*v0.RelationTupleUpdate, 0, len(mutations))
	for _, mut := range mutations {
		changes = append(changes, tuple.UpdateFromRelationshipUpdate(mut))
	}

	newChangelogEntry := &tupleChangelog{
		id:        newChangelogID,
		timestamp: uint64(time.Now().UnixNano()),
		changes:   changes,
	}

	if err := txn.Insert(tableChangelog, newChangelogEntry); err != nil {
		return 0, err
	}

	// Apply the mutations
	for _, mutation := range mutations {
		existing, err := findRelationship(txn, mutation.Relationship)
		if err != nil {
			return 0, err
		}

		var deletedExisting tupleEntry
		if existing != nil {
			deletedExisting = *existing
			deletedExisting.deletedTxn = newChangelogID
		}

		newVersion := tupleEntryFromRelationship(mutation.Relationship, newChangelogID, deletedTransactionID)
		switch mutation.Operation {
		case v1.RelationshipUpdate_OPERATION_CREATE:
			if err := txn.Insert(tableTuple, newVersion); err != nil {
				return 0, err
			}
		case v1.RelationshipUpdate_OPERATION_DELETE:
			if existing != nil {
				if err := txn.Insert(tableTuple, &deletedExisting); err != nil {
					return 0, err
				}
			}
		case v1.RelationshipUpdate_OPERATION_TOUCH:
			if existing != nil {
				if err := txn.Insert(tableTuple, &deletedExisting); err != nil {
					return 0, err
				}
			}
			if err := txn.Insert(tableTuple, newVersion); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("unknown tuple mutation operation type: %s", mutation.Operation)
		}
	}

	return newChangelogID, nil
}

func (mds *memdbDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	db := mds.db
	if db == nil {
		return datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	txn := db.Txn(true)
	defer txn.Abort()

	if err := mds.checkPrecondition(txn, preconditions); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}

	// Create an iterator to find the relevant tuples
	bestIter, err := iteratorForFilter(txn, filter)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}
	filteredIter := memdb.NewFilterIterator(bestIter, relationshipFilterFilterFunc(filter))

	// Collect the tuples into a slice of mutations for the changelog
	var mutations []*v1.RelationshipUpdate
	for row := filteredIter.Next(); row != nil; row = filteredIter.Next() {
		mutations = append(mutations, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
			Relationship: row.(*tupleEntry).Relationship(),
		})
	}

	newChangelogID, err := mds.write(ctx, txn, mutations)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}

	txn.Commit()

	return revisionFromVersion(newChangelogID), nil
}

func (mds *memdbDatastore) QueryTuples(filter datastore.TupleQueryResourceFilter, revision datastore.Revision) datastore.TupleQuery {
	return &memdbTupleQuery{
		db:               mds.db,
		revision:         revision,
		simulatedLatency: mds.simulatedLatency,
		resourceFilter: &v1.RelationshipFilter{
			ResourceType:       filter.ResourceType,
			OptionalResourceId: filter.OptionalResourceID,
			OptionalRelation:   filter.OptionalResourceRelation,
		},
	}
}

func (mds *memdbDatastore) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	return &memdbReverseTupleQuery{
		db:               mds.db,
		revision:         revision,
		simulatedLatency: mds.simulatedLatency,

		subNamespaceName: subject.Namespace,
		subObjectID:      subject.ObjectId,
		subRelationName:  subject.Relation,
	}
}

func (mds *memdbDatastore) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return &memdbReverseTupleQuery{
		db:               mds.db,
		revision:         revision,
		simulatedLatency: mds.simulatedLatency,

		subNamespaceName: subjectNamespace,
		subRelationName:  subjectRelation,
	}
}

func (mds *memdbDatastore) ReverseQueryTuplesFromSubjectNamespace(subjectNamespace string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return &memdbReverseTupleQuery{
		db:               mds.db,
		revision:         revision,
		simulatedLatency: mds.simulatedLatency,

		subNamespaceName: subjectNamespace,
	}
}

func (mds *memdbDatastore) SyncRevision(ctx context.Context) (datastore.Revision, error) {
	db := mds.db
	if db == nil {
		return datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	// Compute the current revision
	txn := db.Txn(false)
	defer txn.Abort()

	lastRaw, err := txn.Last(tableChangelog, indexID)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}
	if lastRaw != nil {
		return revisionFromVersion(lastRaw.(*tupleChangelog).id), nil
	}
	return datastore.NoRevision, nil
}

func (mds *memdbDatastore) Revision(ctx context.Context) (datastore.Revision, error) {
	db := mds.db
	if db == nil {
		return datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	txn := db.Txn(false)
	defer txn.Abort()

	lowerBound := uint64(time.Now().Add(-1 * mds.revisionFuzzingTimedelta).UnixNano())

	time.Sleep(mds.simulatedLatency)
	iter, err := txn.LowerBound(tableChangelog, indexTimestamp, lowerBound)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}

	var candidates []datastore.Revision
	for oneChange := iter.Next(); oneChange != nil; oneChange = iter.Next() {
		candidates = append(candidates, revisionFromVersion(oneChange.(*tupleChangelog).id))
	}

	if len(candidates) > 0 {
		return candidates[rand.Intn(len(candidates))], nil
	}
	return mds.SyncRevision(ctx)
}

func (mds *memdbDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	db := mds.db
	if db == nil {
		return fmt.Errorf("memdb closed")
	}

	txn := db.Txn(false)
	defer txn.Abort()

	// We need to know the highest possible revision
	time.Sleep(mds.simulatedLatency)
	lastRaw, err := txn.Last(tableChangelog, indexID)
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}
	if lastRaw == nil {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}

	highest := revisionFromVersion(lastRaw.(*tupleChangelog).id)

	if revision.GreaterThan(highest) {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionInFuture)
	}

	lowerBound := uint64(time.Now().Add(mds.gcWindowInverted).UnixNano())
	time.Sleep(mds.simulatedLatency)
	iter, err := txn.LowerBound(tableChangelog, indexTimestamp, lowerBound)
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	firstValid := iter.Next()
	if firstValid == nil && !revision.Equal(highest) {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	if firstValid != nil && revision.LessThan(revisionFromVersion(firstValid.(*tupleChangelog).id)) {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	return nil
}

func relationshipFilterFilterFunc(filter *v1.RelationshipFilter) func(interface{}) bool {
	return func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*tupleEntry)

		// If it's already dead, filter it.
		if tuple.deletedTxn != deletedTransactionID {
			return true
		}

		// If it doesn't match one of the resource filters, filter it.
		switch {
		case filter.ResourceType != tuple.namespace:
			return true
		case filter.OptionalResourceId != "" && filter.OptionalResourceId != tuple.objectID:
			return true
		case filter.OptionalRelation != "" && filter.OptionalRelation != tuple.relation:
			return true
		}

		// If it doesn't match one of the subject filters, filter it.
		if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
			switch {
			case subjectFilter.SubjectType != tuple.usersetNamespace:
				return true
			case subjectFilter.OptionalSubjectId != "" && subjectFilter.OptionalSubjectId != tuple.usersetObjectID:
				return true
			case subjectFilter.OptionalRelation != nil &&
				stringz.DefaultEmpty(subjectFilter.OptionalRelation.Relation, datastore.Ellipsis) != tuple.usersetRelation:
				return true
			}
		}

		return false
	}
}

func findRelationship(txn *memdb.Txn, toFind *v1.Relationship) (*tupleEntry, error) {
	foundRaw, err := txn.First(
		tableTuple,
		indexLive,
		toFind.Resource.ObjectType,
		toFind.Resource.ObjectId,
		toFind.Relation,
		toFind.Subject.Object.ObjectType,
		toFind.Subject.Object.ObjectId,
		stringz.DefaultEmpty(toFind.Subject.OptionalRelation, datastore.Ellipsis),
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
