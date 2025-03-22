package schema

import (
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

// UniqueLivingRelationshipIndex is an index for unique living relationships.
var UniqueLivingRelationshipIndex = common.IndexDefinition{
	Name: `uq_relation_tuple_living_xid`,
	ColumnsSQL: `relation_tuple (namespace, object_id, relation, userset_namespace, userset_object_id,
				                 userset_relation, deleted_xid)`,
	Shapes: []queryshape.Shape{
		queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
	},
}

// IndexRelationshipBySubject is an index for relationships by subject. It is used for
// the reverse lookup of relationships.
var IndexRelationshipBySubject = common.IndexDefinition{
	Name:       `ix_relation_tuple_by_subject`,
	ColumnsSQL: `userset_object_id, userset_namespace, userset_relation, namespace, relation`,
	Shapes: []queryshape.Shape{
		queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
	},
}

// IndexRelationshipBySubjectRelation is an index for relationships by subject and relation.
var IndexRelationshipBySubjectRelation = common.IndexDefinition{
	Name:       `ix_relation_tuple_by_subject_relation`,
	ColumnsSQL: `userset_namespace, userset_relation, namespace, relation`,
	Shapes: []queryshape.Shape{
		// TODO: Use of this index for these query shapes is VERY inefficient,
		// and we need to remove it in the future.
		queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
	},
}

// IndexRelationshipAliveByResourceRelationSubjectCovering is an index for alive relationships
// by resource, relation, and subject. In includes the caveat information for the relationship.
var IndexRelationshipAliveByResourceRelationSubjectCovering = common.IndexDefinition{
	Name: `ix_relation_tuple_alive_by_resource_rel_subject_covering`,
	ColumnsSQL: `relation_tuple (namespace, relation, userset_namespace)
    			 INCLUDE (userset_object_id, userset_relation, caveat_name, caveat_context)
    			 WHERE deleted_xid = '9223372036854775807'::xid8`,
}

// IndexWatchAPI is an index for the Watch API. It is used for the Watch API, and provides
// filtering of alive relationships.
var IndexWatchAPI = common.IndexDefinition{
	Name:       `ix_watch_api_index`,
	ColumnsSQL: `relation_tuple (created_xid)`,
}

// IndexExpiringRelationships is an index for (possibly) expiring relationships. It is used
// for the GC process which checks for expired relationships.
var IndexExpiringRelationships = common.IndexDefinition{
	Name:       `ix_relation_tuple_expired`,
	ColumnsSQL: `relation_tuple (expiration) WHERE expiration IS NOT NULL`,
}

// IndexSortedRelationTupleTransaction adds an index to relation_tuple_transaction table
// to support garbage collection. This is in support of the query for selecting the most recent
// transaction: `SELECT xid, snapshot FROM relation_tuple_transaction WHERE timestamp < $1 ORDER BY xid DESC LIMIT 1`
//
// EXPLAIN before the index:
// Limit  (cost=0.56..1.78 rows=1 width=558) (actual time=5706.155..5706.156 rows=1 loops=1)
// ->  Index Scan Backward using pk_rttx on relation_tuple_transaction  (cost=0.56..30428800.04 rows=25023202 width=558) (actual time=5706.154..5706.155 rows=1 loops=1)
//
//	Filter: ("timestamp" < (now() - '04:00:00'::interval))
//	Rows Removed by Filter: 6638121
//
// Planning Time: 0.098 ms
// Execution Time: 5706.192 ms
var IndexSortedRelationTupleTransaction = common.IndexDefinition{
	Name:       `ix_relation_tuple_transaction_xid_desc_timestamp`,
	ColumnsSQL: `relation_tuple_transaction (xid DESC, timestamp)`,
}

// IndexRelationTupleTransactionTimestamp adds an index to relation_tuple_transaction table
// to support garbage collection.
// DEPRECATED: Superceded by IndexSortedRelationTupleTransaction and should be removed in
// the future.
var IndexRelationTupleTransactionTimestamp = common.IndexDefinition{
	Name:         `ix_relation_tuple_transaction_by_timestamp`,
	ColumnsSQL:   `relation_tuple_transaction(timestamp)`,
	IsDeprecated: true,
}

// IndexGCDeadRelationships is an index for the GC process to quickly find dead relationships
// to be garbage collected.
var IndexGCDeadRelationships = common.IndexDefinition{
	Name:       `ix_gc_index`,
	ColumnsSQL: `relation_tuple (deleted_xid DESC) WHERE deleted_xid < '9223372036854775807'::xid8`,
}

var pgIndexes = []common.IndexDefinition{
	UniqueLivingRelationshipIndex,
	IndexRelationshipBySubject,
	IndexRelationshipBySubjectRelation,
	IndexRelationshipAliveByResourceRelationSubjectCovering,
	IndexWatchAPI,
	IndexExpiringRelationships,
	IndexSortedRelationTupleTransaction,
	IndexGCDeadRelationships,
}
