package spanner

import (
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

var IndexRelationshipBySubject = common.IndexDefinition{
	Name:               "ix_relation_tuple_by_subject",
	ColumnsSQL:         `relation_tuple (userset_object_id, userset_namespace, userset_relation, namespace, relation)`,
	PreferredSortOrder: options.BySubject,
}

var NoIndexingHint common.IndexingHint = nil

// IndexingHintForQueryShape returns an indexing hint for the given query shape, if any.
func IndexingHintForQueryShape(schema common.SchemaInformation, qs queryshape.Shape) common.IndexingHint {
	if schema.IntegrityEnabled {
		// Don't force anything since we don't have the other indexes.
		return NoIndexingHint
	}

	switch qs {
	case queryshape.MatchingResourcesForSubject:
		return forcedIndex{IndexRelationshipBySubject}

	default:
		return NoIndexingHint
	}
}

// forcedIndex is an index hint that forces the use of a specific index.
type forcedIndex struct {
	index common.IndexDefinition
}

func (f forcedIndex) FromSQLSuffix() (string, error) {
	return "", nil
}

func (f forcedIndex) FromTable(existingTableName string) (string, error) {
	return existingTableName + "@{FORCE_INDEX=" + f.index.Name + "}", nil
}

func (f forcedIndex) SQLPrefix() (string, error) {
	return "", nil
}

func (f forcedIndex) SortOrder() options.SortOrder {
	return f.index.PreferredSortOrder
}

var _ common.IndexingHint = forcedIndex{}
