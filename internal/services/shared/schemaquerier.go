package shared

import "github.com/authzed/spicedb/pkg/datastore"

// RelationshipQuerier is the part of a reader that the schema change checks use to look for
// relationships that a change would leave without schema. A datalayer.ReadWriteTransaction is
// one; the querier from datastore.CreatedRelationshipsReader, which covers only the
// relationships created after a revision, is another.
type RelationshipQuerier = datastore.RelationshipQuerier
