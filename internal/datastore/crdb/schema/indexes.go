package schema

import (
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// IndexPrimaryKey is a synthetic index that represents the primary key of the relation_tuple table.
var IndexPrimaryKey = common.IndexDefinition{
	Name:       "pk_relation_tuple",
	ColumnsSQL: `PRIMARY KEY (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)`,
	Shapes: []queryshape.Shape{
		queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
		queryshape.FindResourceOfType,
		queryshape.AllSubjectsForResources,
	},
}

// IndexRelationshipBySubject is an index for looking up relationships by subject.
var IndexRelationshipBySubject = common.IndexDefinition{
	Name:       "ix_relation_tuple_by_subject",
	ColumnsSQL: `relation_tuple (userset_object_id, userset_namespace, userset_relation, namespace, relation)`,
	Shapes: []queryshape.Shape{
		queryshape.MatchingResourcesForSubject,
		queryshape.FindSubjectOfType,
	},
}

// IndexRelationshipBySubjectRelation is an index for looking up relationships by subject type and relation.
// Used by schema delta checking.
var IndexRelationshipBySubjectRelation = common.IndexDefinition{
	Name:       "ix_relation_tuple_by_subject_relation",
	ColumnsSQL: `relation_tuple (userset_namespace, userset_relation, namespace, relation)`,
}

// IndexRelationshipWithIntegrity is an index for looking up relationships with integrity.
var IndexRelationshipWithIntegrity = common.IndexDefinition{
	Name:       "ix_relation_tuple_with_integrity",
	ColumnsSQL: `relation_tuple_with_integrity (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation) STORING (integrity_key_id, integrity_hash, timestamp, caveat_name, caveat_context)`,
	Shapes: []queryshape.Shape{
		queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
	},
}

var crdbIndexes = []common.IndexDefinition{
	IndexPrimaryKey,
	IndexRelationshipBySubject,
	IndexRelationshipBySubjectRelation,
	IndexRelationshipWithIntegrity,
}

// IndexingHintForQueryShape returns an indexing hint for the given query shape, if any.
func IndexingHintForQueryShape(schema common.SchemaInformation, qs queryshape.Shape) common.IndexingHint {
	if schema.IntegrityEnabled {
		// Don't force anything since we don't have the other indexes.
		return nil
	}

	switch qs {
	case queryshape.CheckPermissionSelectDirectSubjects:
		return forcedIndex{IndexPrimaryKey}

	case queryshape.CheckPermissionSelectIndirectSubjects:
		return forcedIndex{IndexPrimaryKey}

	case queryshape.AllSubjectsForResources:
		return forcedIndex{IndexPrimaryKey}

	case queryshape.MatchingResourcesForSubject:
		return forcedIndex{IndexRelationshipBySubject}

	case queryshape.FindResourceOfType:
		return forcedIndex{IndexPrimaryKey}

	case queryshape.FindSubjectOfType:
		return forcedIndex{IndexRelationshipBySubject}

	default:
		return nil
	}
}

// IndexForFilter returns the index to use for a given relationships filter.
func IndexForFilter(schema common.SchemaInformation, filter datastore.RelationshipsFilter) common.IndexDefinition {
	if schema.IntegrityEnabled {
		// Don't force anything since we don't have the other indexes.
		return IndexPrimaryKey
	}

	// If there is no resource type specified, but there is a subject filter, use the
	// inverse index.
	if filter.OptionalResourceType == "" && len(filter.OptionalSubjectsSelectors) > 0 {
		return IndexRelationshipBySubject
	}

	return IndexPrimaryKey
}

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

var _ common.IndexingHint = forcedIndex{}
