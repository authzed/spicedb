package schema

import (
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

// IndexForwardRelationships is an index for forward relationships. It is used for
// the CheckPermission, Expand and LookupSubjects APIs, as well as reading relationships,
// and the forward checks for schema diffs.
var IndexForwardRelationships = common.IndexDefinition{
	Name:       `ix_relationship_covering_index_by_resource`,
	ColumnsSQL: `relation_tuple (namespace, relation, object_id, userset_namespace, userset_relation, userset_object_id) INCLUDE (expiration, created_xid, deleted_xid)`,
	Shapes: []queryshape.Shape{
		queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
	},
}

// IndexBackwardRelationships is an index for backward relationships. It is used for
// the LookupResources API, as well as reading relationships, and the backward checks for schema diffs.
var IndexBackwardRelationships = common.IndexDefinition{
	Name:       `ix_relationship_covering_index_by_subject`,
	ColumnsSQL: `relation_tuple (userset_namespace, userset_relation, userset_object_id, namespace, relation, object_id) INCLUDE (expiration, created_xid, deleted_xid)`,
	Shapes: []queryshape.Shape{
		queryshape.CheckPermissionSelectDirectSubjects,
		queryshape.CheckPermissionSelectIndirectSubjects,
	},
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

// IndexGCDeadRelationships is an index for the GC process to quickly find dead relationships
// to be garbage collected.
var IndexGCDeadRelationships = common.IndexDefinition{
	Name:       `ix_gc_index`,
	ColumnsSQL: `relation_tuple (deleted_xid DESC) WHERE deleted_xid < '9223372036854775807'::xid8`,
}

var pgIndexes = []common.IndexDefinition{
	IndexForwardRelationships,
	IndexBackwardRelationships,
	IndexWatchAPI,
	IndexExpiringRelationships,
	IndexSortedRelationTupleTransaction,
	IndexGCDeadRelationships,
}
