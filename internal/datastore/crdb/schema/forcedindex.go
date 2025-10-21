package schema

import (
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// forcedIndex is an index hint that forces the use of a specific index.
type forcedIndex struct {
	index common.IndexDefinition
}

func (f forcedIndex) FromSQLSuffix() (string, error) {
	return "", nil
}

func (f forcedIndex) FromTable(existingTableName string) (string, error) {
	// Indexes are forced in CRDB by appending the index name after an @ sign after the table
	// name in the FROM clause.
	// Example: FROM relation_tuple@ix_relation_tuple_by_subject
	if existingTableName == "" {
		return "", spiceerrors.MustBugf("existing table name is empty")
	}
	return existingTableName + "@" + f.index.Name, nil
}

func (f forcedIndex) SQLPrefix() (string, error) {
	return "", nil
}

func (f forcedIndex) SortOrder() options.SortOrder {
	return f.index.PreferredSortOrder
}

var _ common.IndexingHint = forcedIndex{}
