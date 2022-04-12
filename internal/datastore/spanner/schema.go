package spanner

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	tableNamespace     = "namespace_config"
	colNamespaceName   = "namespace"
	colNamespaceConfig = "serialized_config"
	colNamespaceTS     = "timestamp"

	tableRelationship   = "relation_tuple"
	colNamespace        = "namespace"
	colObjectID         = "object_id"
	colRelation         = "relation"
	colUsersetNamespace = "userset_namespace"
	colUsersetObjectID  = "userset_object_id"
	colUsersetRelation  = "userset_relation"
	colTimestamp        = "timestamp"

	tableChangelog            = "changelog"
	colChangeUUID             = "uuid"
	colChangeTS               = "timestamp"
	colChangeOp               = "operation"
	colChangeNamespace        = "namespace"
	colChangeObjectID         = "object_id"
	colChangeRelation         = "relation"
	colChangeUsersetNamespace = "userset_namespace"
	colChangeUsersetObjectID  = "userset_object_id"
	colChangeUsersetRelation  = "userset_relation"

	tableMetadata = "metadata"
	colUniqueID   = "unique_id"

	tableCounters = "relationship_estimate_counters"
	colID         = "id"
	colCount      = "count"

	colChangeOpCreate = 1
	colChangeOpTouch  = 2
	colChangeOpDelete = 3
)

var allRelationshipCols = []string{
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
	colTimestamp,
}

var allChangelogCols = []string{
	colChangeTS,
	colChangeUUID,
	colChangeOp,
	colChangeNamespace,
	colChangeObjectID,
	colChangeRelation,
	colChangeUsersetNamespace,
	colChangeUsersetObjectID,
	colChangeUsersetRelation,
}

// Both creates and touches are emitted as touched to match other datastores.
var opMap = map[int64]core.RelationTupleUpdate_Operation{
	colChangeOpCreate: core.RelationTupleUpdate_TOUCH,
	colChangeOpTouch:  core.RelationTupleUpdate_TOUCH,
	colChangeOpDelete: core.RelationTupleUpdate_DELETE,
}
