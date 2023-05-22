package memdb

import (
	"context"
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

func (rwt *memdbReadWriteTx) WriteRelationships(_ context.Context, mutations []*core.RelationTupleUpdate) error {
	rwt.mustLock()
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
			rwt.toCaveatReference(mutation),
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
				rt, err := existing.RelationTuple()
				if err != nil {
					return err
				}
				return common.NewCreateRelationshipExistsError(rt)
			}
			if err := tx.Insert(tableRelationship, rel); err != nil {
				return fmt.Errorf("error inserting relationship: %w", err)
			}

		case core.RelationTupleUpdate_TOUCH:
			if existing != nil {
				rt, err := existing.RelationTuple()
				if err != nil {
					return err
				}
				if tuple.MustString(rt) == tuple.MustString(mutation.Tuple) {
					continue
				}
			}

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

func (rwt *memdbReadWriteTx) toCaveatReference(mutation *core.RelationTupleUpdate) *contextualizedCaveat {
	var cr *contextualizedCaveat
	if mutation.Tuple.Caveat != nil {
		cr = &contextualizedCaveat{
			caveatName: mutation.Tuple.Caveat.CaveatName,
			context:    mutation.Tuple.Caveat.Context.AsMap(),
		}
	}
	return cr
}

func (rwt *memdbReadWriteTx) DeleteRelationships(_ context.Context, filter *v1.RelationshipFilter) error {
	rwt.mustLock()
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
		rt, err := row.(*relationship).RelationTuple()
		if err != nil {
			return err
		}
		mutations = append(mutations, tuple.Delete(rt))
	}

	return rwt.write(tx, mutations...)
}

func (rwt *memdbReadWriteTx) WriteNamespaces(_ context.Context, newConfigs ...*core.NamespaceDefinition) error {
	rwt.mustLock()
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

func (rwt *memdbReadWriteTx) DeleteNamespaces(_ context.Context, nsNames ...string) error {
	rwt.mustLock()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	for _, nsName := range nsNames {
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
	}

	return nil
}

func (rwt *memdbReadWriteTx) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	updates := []*core.RelationTupleUpdate{{
		Operation: core.RelationTupleUpdate_TOUCH,
	}}

	var numCopied uint64
	var next *core.RelationTuple
	var err error
	for next, err = iter.Next(ctx); next != nil && err == nil; next, err = iter.Next(ctx) {
		updates[0].Tuple = next
		if err := rwt.WriteRelationships(ctx, updates); err != nil {
			return 0, err
		}
		numCopied++
	}

	return numCopied, err
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
