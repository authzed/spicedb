package schema

import (
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
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
func IndexForFilter(schema common.SchemaInformation, filter datastore.RelationshipsFilter) *common.IndexDefinition {
	// Special case: if the filter specifies the resource type and relation and the subject type and relation, then
	// the schema diff index can be used.
	if filter.OptionalResourceType != "" &&
		filter.OptionalResourceRelation != "" &&
		len(filter.OptionalSubjectsSelectors) == 1 &&
		filter.OptionalSubjectsSelectors[0].OptionalSubjectType != "" &&
		filter.OptionalSubjectsSelectors[0].RelationFilter.NonEllipsisRelation != "" &&
		!filter.OptionalSubjectsSelectors[0].RelationFilter.IncludeEllipsisRelation &&
		!filter.OptionalSubjectsSelectors[0].RelationFilter.OnlyNonEllipsisRelations {
		return &IndexRelationshipBySubjectRelation
	}

	// Otherwise, determine an index based on whether the filter has a larger match on the resources or subject.
	resourceFieldDepth := 0
	if filter.OptionalResourceType != "" {
		resourceFieldDepth = 1
		if len(filter.OptionalResourceIds) > 0 || filter.OptionalResourceIDPrefix != "" {
			resourceFieldDepth = 2
			if filter.OptionalResourceRelation != "" {
				if filter.OptionalResourceIDPrefix != "" {
					return nil // Cannot use an index with a prefix and a relation.
				}

				resourceFieldDepth = 3
			}
		}
	}

	subjectFieldDepths := mapz.NewSet[int]()
	for _, subjectSelector := range filter.OptionalSubjectsSelectors {
		sfd := 0
		if len(subjectSelector.OptionalSubjectIds) > 0 {
			sfd = 1
			if subjectSelector.OptionalSubjectType != "" {
				sfd = 2
				if subjectSelector.RelationFilter.NonEllipsisRelation != "" {
					sfd = 3
				}
			}
		}
		subjectFieldDepths.Add(sfd)
	}

	if subjectFieldDepths.Len() > 1 {
		return nil
	}

	subjectFieldDepth := 0
	if !subjectFieldDepths.IsEmpty() {
		subjectFieldDepth = subjectFieldDepths.AsSlice()[0]
	}

	if resourceFieldDepth == 0 && subjectFieldDepth == 0 {
		return nil
	}

	if resourceFieldDepth > subjectFieldDepth {
		return &IndexPrimaryKey
	}

	if resourceFieldDepth < subjectFieldDepth {
		if schema.IntegrityEnabled {
			// Don't force this index since it doesn't exist for integrity-enabled schemas.
			return nil
		}

		return &IndexRelationshipBySubject
	}

	return nil
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
