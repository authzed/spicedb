package memdb

import (
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"
	"github.com/jzelinskie/stringz"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type memdbReadWriteTx struct {
	memdbReader
	newRevision datastore.Revision
}

func (rwt *memdbReadWriteTx) WriteRelationships(mutations []*core.RelationTupleUpdate) error {
	rwt.lockOrPanic()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	return rwt.write(tx, mutations...)
}

// Caller must already hold the concurrent access lock!
func (rwt *memdbReadWriteTx) write(tx *memdb.Txn, mutations ...*core.RelationTupleUpdate) error {
	// Apply the mutations
	for _, mutation := range mutations {
		rel := &relationship{
			mutation.Tuple.ResourceAndRelation.Namespace,
			mutation.Tuple.ResourceAndRelation.ObjectId,
			mutation.Tuple.ResourceAndRelation.Relation,
			mutation.Tuple.Subject.Namespace,
			mutation.Tuple.Subject.ObjectId,
			mutation.Tuple.Subject.Relation,
		}

		found, err := tx.First(
			tableRelationship,
			indexID,
			rel.namespace,
			rel.resourceID,
			rel.relation,
			rel.subjectNamespace,
			rel.subjectObjectID,
			rel.subjectRelation,
		)
		if err != nil {
			return fmt.Errorf("error loading existing relationship: %w", err)
		}

		var existing *relationship
		if found != nil {
			existing = found.(*relationship)
		}

		switch mutation.Operation {
		case core.RelationTupleUpdate_CREATE:
			if existing != nil {
				return common.NewCreateRelationshipExistsError(existing.RelationTuple())
			}
			fallthrough
		case core.RelationTupleUpdate_TOUCH:
			if err := tx.Insert(tableRelationship, rel); err != nil {
				return fmt.Errorf("error inserting relationship: %w", err)
			}
		case core.RelationTupleUpdate_DELETE:
			if existing != nil {
				if err := tx.Delete(tableRelationship, existing); err != nil {
					return fmt.Errorf("error deleting relationship: %w", err)
				}
			}
		default:
			return fmt.Errorf("unknown tuple mutation operation type: %s", mutation.Operation)
		}
	}

	return nil
}

func (rwt *memdbReadWriteTx) DeleteRelationships(filter *v1.RelationshipFilter) error {
	rwt.lockOrPanic()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	return rwt.deleteWithLock(tx, filter)
}

// caller must already hold the concurrent access lock
func (rwt *memdbReadWriteTx) deleteWithLock(tx *memdb.Txn, filter *v1.RelationshipFilter) error {
	// Create an iterator to find the relevant tuples
	bestIter, err := iteratorForFilter(tx, datastore.RelationshipsFilterFromPublicFilter(filter))
	if err != nil {
		return err
	}
	filteredIter := memdb.NewFilterIterator(bestIter, relationshipFilterFilterFunc(filter))

	// Collect the tuples into a slice of mutations for the changelog
	var mutations []*core.RelationTupleUpdate
	for row := filteredIter.Next(); row != nil; row = filteredIter.Next() {
		mutations = append(mutations, tuple.Delete(row.(*relationship).RelationTuple()))
	}

	return rwt.write(tx, mutations...)
}

func (rwt *memdbReadWriteTx) WriteNamespaces(newConfigs ...*core.NamespaceDefinition) error {
	rwt.lockOrPanic()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	for _, newConfig := range newConfigs {
		serialized, err := proto.Marshal(newConfig)
		if err != nil {
			return err
		}

		newConfigEntry := &namespace{newConfig.Name, serialized, rwt.newRevision}

		err = tx.Insert(tableNamespace, newConfigEntry)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rwt *memdbReadWriteTx) DeleteNamespace(nsName string) error {
	rwt.lockOrPanic()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	foundRaw, err := tx.First(tableNamespace, indexID, nsName)
	if err != nil {
		return err
	}

	if foundRaw == nil {
		return fmt.Errorf("unable to find namespace to delete")
	}

	if err := tx.Delete(tableNamespace, foundRaw); err != nil {
		return err
	}

	// Delete the relationships from the namespace
	if err := rwt.deleteWithLock(tx, &v1.RelationshipFilter{
		ResourceType: nsName,
	}); err != nil {
		return fmt.Errorf("unable to delete relationships from deleted namespace: %w", err)
	}

	return nil
}

func relationshipFilterFilterFunc(filter *v1.RelationshipFilter) func(interface{}) bool {
	return func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*relationship)

		// If it doesn't match one of the resource filters, filter it.
		switch {
		case filter.ResourceType != tuple.namespace:
			return true
		case filter.OptionalResourceId != "" && filter.OptionalResourceId != tuple.resourceID:
			return true
		case filter.OptionalRelation != "" && filter.OptionalRelation != tuple.relation:
			return true
		}

		// If it doesn't match one of the subject filters, filter it.
		if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
			switch {
			case subjectFilter.SubjectType != tuple.subjectNamespace:
				return true
			case subjectFilter.OptionalSubjectId != "" && subjectFilter.OptionalSubjectId != tuple.subjectObjectID:
				return true
			case subjectFilter.OptionalRelation != nil &&
				stringz.DefaultEmpty(subjectFilter.OptionalRelation.Relation, datastore.Ellipsis) != tuple.subjectRelation:
				return true
			}
		}

		return false
	}
}

var _ datastore.ReadWriteTransaction = &memdbReadWriteTx{}
