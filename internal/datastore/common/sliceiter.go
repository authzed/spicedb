package common

import (
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewSliceRelationshipIterator creates a datastore.RelationshipIterator instance from a materialized slice of tuples.
func NewSliceRelationshipIterator(rels []tuple.Relationship) datastore.RelationshipIterator {
	return func(yield func(tuple.Relationship, error) bool) {
		for _, rel := range rels {
			if !yield(rel, nil) {
				break
			}
		}
	}
}
