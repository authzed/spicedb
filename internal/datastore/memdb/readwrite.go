package memdb

import (
	"context"
	"fmt"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"
	"github.com/jzelinskie/stringz"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type memdbReadWriteTx struct {
	memdbReader
	newRevision datastore.Revision
}

func (rwt *memdbReadWriteTx) WriteRelationships(_ context.Context, mutations []tuple.RelationshipUpdate) error {
	rwt.mustLock()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	return rwt.write(tx, mutations...)
}

func (rwt *memdbReadWriteTx) toIntegrity(mutation tuple.RelationshipUpdate) *relationshipIntegrity {
	var ri *relationshipIntegrity
	if mutation.Relationship.OptionalIntegrity != nil {
		ri = &relationshipIntegrity{
			keyID:     mutation.Relationship.OptionalIntegrity.KeyId,
			hash:      mutation.Relationship.OptionalIntegrity.Hash,
			timestamp: mutation.Relationship.OptionalIntegrity.HashedAt.AsTime(),
		}
	}
	return ri
}

// Caller must already hold the concurrent access lock!
func (rwt *memdbReadWriteTx) write(tx *memdb.Txn, mutations ...tuple.RelationshipUpdate) error {
	// Apply the mutations
	for _, mutation := range mutations {
		rel := &relationship{
			mutation.Relationship.Resource.ObjectType,
			mutation.Relationship.Resource.ObjectID,
			mutation.Relationship.Resource.Relation,
			mutation.Relationship.Subject.ObjectType,
			mutation.Relationship.Subject.ObjectID,
			mutation.Relationship.Subject.Relation,
			rwt.toCaveatReference(mutation),
			rwt.toIntegrity(mutation),
			mutation.Relationship.OptionalExpiration,
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
		case tuple.UpdateOperationCreate:
			if existing != nil {
				rt, err := existing.Relationship()
				if err != nil {
					return err
				}
				return common.NewCreateRelationshipExistsError(&rt)
			}
			if err := tx.Insert(tableRelationship, rel); err != nil {
				return fmt.Errorf("error inserting relationship: %w", err)
			}

		case tuple.UpdateOperationTouch:
			if existing != nil {
				rt, err := existing.Relationship()
				if err != nil {
					return err
				}
				if tuple.MustString(rt) == tuple.MustString(mutation.Relationship) {
					continue
				}
			}

			if err := tx.Insert(tableRelationship, rel); err != nil {
				return fmt.Errorf("error inserting relationship: %w", err)
			}

		case tuple.UpdateOperationDelete:
			if existing != nil {
				if err := tx.Delete(tableRelationship, existing); err != nil {
					return fmt.Errorf("error deleting relationship: %w", err)
				}
			}
		default:
			return spiceerrors.MustBugf("unknown tuple mutation operation type: %v", mutation.Operation)
		}
	}

	return nil
}

func (rwt *memdbReadWriteTx) toCaveatReference(mutation tuple.RelationshipUpdate) *contextualizedCaveat {
	var cr *contextualizedCaveat
	if mutation.Relationship.OptionalCaveat != nil {
		cr = &contextualizedCaveat{
			caveatName: mutation.Relationship.OptionalCaveat.CaveatName,
			context:    mutation.Relationship.OptionalCaveat.Context.AsMap(),
		}
	}
	return cr
}

func (rwt *memdbReadWriteTx) DeleteRelationships(_ context.Context, filter *v1.RelationshipFilter, opts ...options.DeleteOptionsOption) (uint64, bool, error) {
	rwt.mustLock()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return 0, false, err
	}

	delOpts := options.NewDeleteOptionsWithOptionsAndDefaults(opts...)
	var delLimit uint64
	if delOpts.DeleteLimit != nil && *delOpts.DeleteLimit > 0 {
		delLimit = *delOpts.DeleteLimit
	}

	return rwt.deleteWithLock(tx, filter, delLimit)
}

// caller must already hold the concurrent access lock
func (rwt *memdbReadWriteTx) deleteWithLock(tx *memdb.Txn, filter *v1.RelationshipFilter, limit uint64) (uint64, bool, error) {
	// Create an iterator to find the relevant tuples
	dsFilter, err := datastore.RelationshipsFilterFromPublicFilter(filter)
	if err != nil {
		return 0, false, err
	}

	bestIter, err := iteratorForFilter(tx, dsFilter)
	if err != nil {
		return 0, false, err
	}
	filteredIter := memdb.NewFilterIterator(bestIter, relationshipFilterFilterFunc(filter))

	// Collect the tuples into a slice of mutations for the changelog
	var mutations []tuple.RelationshipUpdate
	var counter uint64

	metLimit := false
	for row := filteredIter.Next(); row != nil; row = filteredIter.Next() {
		rt, err := row.(*relationship).Relationship()
		if err != nil {
			return 0, false, err
		}
		mutations = append(mutations, tuple.Delete(rt))
		counter++

		if limit > 0 && counter == limit {
			metLimit = true
			break
		}
	}

	return counter, metLimit, rwt.write(tx, mutations...)
}

func (rwt *memdbReadWriteTx) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	rwt.mustLock()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	foundRaw, err := tx.First(tableCounters, indexID, name)
	if err != nil {
		return err
	}

	if foundRaw != nil {
		return datastore.NewCounterAlreadyRegisteredErr(name, filter)
	}

	filterBytes, err := filter.MarshalVT()
	if err != nil {
		return err
	}

	// Insert the counter
	counter := &counter{
		name,
		filterBytes,
		0,
		datastore.NoRevision,
	}

	return tx.Insert(tableCounters, counter)
}

func (rwt *memdbReadWriteTx) UnregisterCounter(ctx context.Context, name string) error {
	rwt.mustLock()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	// Check if the counter exists
	foundRaw, err := tx.First(tableCounters, indexID, name)
	if err != nil {
		return err
	}

	if foundRaw == nil {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	return tx.Delete(tableCounters, foundRaw)
}

func (rwt *memdbReadWriteTx) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	rwt.mustLock()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	// Check if the counter exists
	foundRaw, err := tx.First(tableCounters, indexID, name)
	if err != nil {
		return err
	}

	if foundRaw == nil {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	counter := foundRaw.(*counter)
	counter.count = value
	counter.updated = computedAtRevision

	return tx.Insert(tableCounters, counter)
}

func (rwt *memdbReadWriteTx) WriteNamespaces(_ context.Context, newConfigs ...*core.NamespaceDefinition) error {
	rwt.mustLock()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	for _, newConfig := range newConfigs {
		serialized, err := newConfig.MarshalVT()
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
			return fmt.Errorf("namespace not found")
		}

		if err := tx.Delete(tableNamespace, foundRaw); err != nil {
			return err
		}

		// Delete the relationships from the namespace
		if _, _, err := rwt.deleteWithLock(tx, &v1.RelationshipFilter{
			ResourceType: nsName,
		}, 0); err != nil {
			return fmt.Errorf("unable to delete relationships from deleted namespace: %w", err)
		}
	}

	return nil
}

func (rwt *memdbReadWriteTx) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	var numCopied uint64
	var next *tuple.Relationship
	var err error

	updates := []tuple.RelationshipUpdate{{
		Operation: tuple.UpdateOperationCreate,
	}}

	for next, err = iter.Next(ctx); next != nil && err == nil; next, err = iter.Next(ctx) {
		updates[0].Relationship = *next
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
		case filter.ResourceType != "" && filter.ResourceType != tuple.namespace:
			return true
		case filter.OptionalResourceId != "" && filter.OptionalResourceId != tuple.resourceID:
			return true
		case filter.OptionalResourceIdPrefix != "" && !strings.HasPrefix(tuple.resourceID, filter.OptionalResourceIdPrefix):
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
