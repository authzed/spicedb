package schema

import (
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
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

var crdbAllIndexes = []common.IndexDefinition{
	IndexPrimaryKey,
	IndexRelationshipBySubject,
	IndexRelationshipBySubjectRelation,
	IndexRelationshipWithIntegrity,
}

var crdbWithoutIntegrityIndexes = []common.IndexDefinition{
	IndexPrimaryKey,
	IndexRelationshipBySubject,
	IndexRelationshipBySubjectRelation,
}

// TODO: add new indexes to integrity to match the existing ones on non-integrity.
var crdbWithIntegrityIndexes = []common.IndexDefinition{
	IndexRelationshipWithIntegrity,
}

var NoIndexingHint common.IndexingHint = nil

// IndexingHintForQueryShape returns an indexing hint for the given query shape, if any.
func IndexingHintForQueryShape(schema common.SchemaInformation, qs queryshape.Shape) common.IndexingHint {
	if schema.IntegrityEnabled {
		// Don't force anything since we don't have the other indexes.
		return NoIndexingHint
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

	default:
		return nil
	}
}

// IndexForFilter returns the index to use for a given relationships filter or nil if no index is forced.
func IndexForFilter(schema common.SchemaInformation, filter datastore.RelationshipsFilter) (*common.IndexDefinition, error) {
	indexesToCheck := crdbWithoutIntegrityIndexes
	if schema.IntegrityEnabled {
		indexesToCheck = crdbWithIntegrityIndexes
	}

	return forcedIndexForFilter(filter, indexesToCheck)
}
