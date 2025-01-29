package mysql

import (
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

// IndexUniqueLivingRelationships is the UNIQUE constraint index on
// living relationships.
var IndexUniqueLivingRelationships = common.IndexDefinition{
	Name:       `uq_relation_tuple_living`,
	ColumnsSQL: `UNIQUE (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, deleted_transaction)`,
	Shapes: []queryshape.Shape{
		queryshape.CheckPermissionSelectDirectSubjects,
	},
}

// IndexUniqueRelationships is the UNIQUE constraint index on all relationships.
var IndexUniqueRelationships = common.IndexDefinition{
	Name:       `uq_relation_tuple_namespace`,
	ColumnsSQL: `UNIQUE (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, created_transaction, deleted_transaction)`,
}

// IndexRelationshipBySubject is the index on the relationship table for
// looking up relationships by subject.
var IndexRelationshipBySubject = common.IndexDefinition{
	Name:       `ix_relation_tuple_by_subject`,
	ColumnsSQL: `INDEX ix_relation_tuple_by_subject (userset_object_id, userset_namespace, userset_relation, namespace, relation)`,
	Shapes: []queryshape.Shape{
		// TODO: this index isn't great to use for this query shape, so we probably need to fix
		// the indexes
		queryshape.CheckPermissionSelectDirectSubjects,

		queryshape.CheckPermissionSelectIndirectSubjects,
	},
}

// IndexRelationshipBySubjectRelation is the index on the relationship table for
// looking up relationships by subject and relation.
var IndexRelationshipBySubjectRelation = common.IndexDefinition{
	Name:       `ix_relation_tuple_by_subject_relation`,
	ColumnsSQL: `INDEX ix_relation_tuple_by_subject_relation (userset_namespace, userset_relation, namespace, relation)`,
	Shapes: []queryshape.Shape{
		// TODO: this index isn't great to use for these query shape, so we probably need to fix
		// the indexes
		queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
	},
}

// IndexRelationshipByDeletedTransaction is the index on the relationship table for
// looking up relationships by deleted transaction.
var IndexRelationshipByDeletedTransaction = common.IndexDefinition{
	Name:       `ix_relation_tuple_by_deleted_transaction`,
	ColumnsSQL: `INDEX ix_relation_tuple_by_deleted_transaction (deleted_transaction)`,
}

// IndexRelationTupleWatch is the index on the relationship table for
// watching relationships.
var IndexRelationTupleWatch = common.IndexDefinition{
	Name:       `ix_relation_tuple_watch`,
	ColumnsSQL: `INDEX ix_relation_tuple_watch (created_transaction, deleted_transaction DESC)`,
}

// IndexRelationTupleExpired is the index on the relationship table for
// expired relationships. This is used for garbage collection of expired
// relationships.
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
