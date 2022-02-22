package spanner

import v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

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

	colChangeOpCreate = 1
	colChangeOpTouch  = 2
	colChangeOpDelete = 3

	funcPendingCommitTimestamp = "PENDING_COMMIT_TIMESTAMP()"
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
var opMap = map[int64]v0.RelationTupleUpdate_Operation{
	colChangeOpCreate: v0.RelationTupleUpdate_TOUCH,
	colChangeOpTouch:  v0.RelationTupleUpdate_TOUCH,
	colChangeOpDelete: v0.RelationTupleUpdate_DELETE,
}
