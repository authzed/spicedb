package mysql

import (
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

// IndexUniqueLivingRelationships is the UNIQUE constraint index on living relationships.
// Because relation_tuple is clustered on the synthetic `id` primary key, this resource-keyed
// index plays the role that the primary key plays in CRDB/Postgres for forward query shapes.
var IndexUniqueLivingRelationships = common.IndexDefinition{
	Name:       `uq_relation_tuple_living`,
	ColumnsSQL: `UNIQUE (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, deleted_transaction)`,
	Shapes: []queryshape.Shape{
		queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
		queryshape.AllSubjectsForResources,
		queryshape.FindResourceOfType,
	},
	PreferredSortOrder: options.ByResource,
}

// IndexUniqueRelationships is the UNIQUE constraint index on all relationships (including
// historical ones). It enforces uniqueness; it is not used as a read index for any query shape.
var IndexUniqueRelationships = common.IndexDefinition{
	Name:       `uq_relation_tuple_namespace`,
	ColumnsSQL: `UNIQUE (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, created_transaction, deleted_transaction)`,
}

// IndexRelationshipBySubject is the index on the relationship table for looking up
// relationships by subject (the reverse lookup).
var IndexRelationshipBySubject = common.IndexDefinition{
	Name:       `ix_relation_tuple_by_subject`,
	ColumnsSQL: `INDEX ix_relation_tuple_by_subject (userset_object_id, userset_namespace, userset_relation, namespace, relation)`,
	Shapes: []queryshape.Shape{
		queryshape.MatchingResourcesForSubject,
	},
	PreferredSortOrder: options.BySubject,
}

// IndexRelationshipBySubjectRelation is the index on the relationship table for looking up
// relationships by subject type and relation.
var IndexRelationshipBySubjectRelation = common.IndexDefinition{
	Name:       `ix_relation_tuple_by_subject_relation`,
	ColumnsSQL: `INDEX ix_relation_tuple_by_subject_relation (userset_namespace, userset_relation, namespace, relation)`,
	Shapes: []queryshape.Shape{
		queryshape.FindResourceAndSubjectWithRelations,
		queryshape.FindSubjectOfTypeAndRelation,
		queryshape.FindResourceRelationForSubjectRelation,
	},
	PreferredSortOrder: options.BySubject,
}

// IndexRelationshipByDeletedTransaction is the index on the relationship table for
// looking up relationships by deleted transaction.
var IndexRelationshipByDeletedTransaction = common.IndexDefinition{
	Name:       `ix_relation_tuple_by_deleted_transaction`,
	ColumnsSQL: `INDEX ix_relation_tuple_by_deleted_transaction (deleted_transaction)`,
}

// IndexRelationTupleWatch is the index on the relationship table for watching relationships.
var IndexRelationTupleWatch = common.IndexDefinition{
	Name:       `ix_relation_tuple_watch`,
	ColumnsSQL: `INDEX ix_relation_tuple_watch (created_transaction, deleted_transaction DESC)`,
}

// IndexRelationTupleExpired is the index on the relationship table for expired relationships.
// This is used for garbage collection of expired relationships.
var IndexRelationTupleExpired = common.IndexDefinition{
	Name:       `ix_relation_tuple_expired`,
	ColumnsSQL: `INDEX ix_relation_tuple_expired (expiration)`,
}

var indexes = []common.IndexDefinition{
	IndexUniqueLivingRelationships,
	IndexUniqueRelationships,
	IndexRelationshipBySubject,
	IndexRelationshipBySubjectRelation,
	IndexRelationshipByDeletedTransaction,
	IndexRelationTupleWatch,
	IndexRelationTupleExpired,
}

// NoIndexingHint is the absence of an indexing hint.
var NoIndexingHint common.IndexingHint = nil

// IndexingHintForQueryShape returns the index hint to force for a given query shape, if any.
func IndexingHintForQueryShape(qs queryshape.Shape) common.IndexingHint {
	switch qs {
	case queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
		queryshape.AllSubjectsForResources,
		queryshape.FindResourceOfType:
		return forcedIndex{IndexUniqueLivingRelationships}

	case queryshape.MatchingResourcesForSubject:
		return forcedIndex{IndexRelationshipBySubject}

	case queryshape.FindResourceAndSubjectWithRelations,
		queryshape.FindSubjectOfTypeAndRelation,
		queryshape.FindResourceRelationForSubjectRelation:
		return forcedIndex{IndexRelationshipBySubjectRelation}

	default:
		return NoIndexingHint
	}
}

// forcedIndex is an index hint that forces use of a specific index via MySQL's FORCE INDEX clause.
type forcedIndex struct {
	index common.IndexDefinition
}

func (f forcedIndex) FromSQLSuffix() (string, error) {
	return "FORCE INDEX (" + f.index.Name + ")", nil
}

func (f forcedIndex) FromTable(existingTableName string) (string, error) {
	return existingTableName, nil
}

func (f forcedIndex) SQLPrefix() (string, error) {
	return "", nil
}

func (f forcedIndex) SortOrder() options.SortOrder {
	return f.index.PreferredSortOrder
}

var _ common.IndexingHint = forcedIndex{}
