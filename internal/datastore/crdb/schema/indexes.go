package schema

import (
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

// IndexPrimaryKey is a synthetic index that represents the primary key of the relation_tuple table.
var IndexPrimaryKey = common.IndexDefinition{
	Name:       "pk_relation_tuple",
	ColumnsSQL: `PRIMARY KEY (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)`,
	Shapes: []queryshape.Shape{
		queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
	},
}

// IndexRelationshipBySubject is an index for looking up relationships by subject.
var IndexRelationshipBySubject = common.IndexDefinition{
	Name:       "ix_relation_tuple_by_subject",
	ColumnsSQL: `relation_tuple (userset_object_id, userset_namespace, userset_relation, namespace, relation)`,
	Shapes: []queryshape.Shape{
		queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
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
