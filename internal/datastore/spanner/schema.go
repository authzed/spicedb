package spanner

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
	colCaveatName       = "caveat_name"
	colCaveatContext    = "caveat_context"
	colExpiration       = "expires_at"

	tableCaveat         = "caveat"
	colName             = "name"
	colCaveatDefinition = "definition"
	colCaveatTS         = "timestamp"

	tableMetadata = "metadata"
	colUniqueID   = "unique_id"

	tableRelationshipCounter     = "relationship_counter"
	colCounterName               = "name"
	colCounterSerializedFilter   = "serialized_filter"
	colCounterCurrentCount       = "current_count"
	colCounterUpdatedAtTimestamp = "updated_at_timestamp"

	tableTransactionMetadata = "transaction_metadata"
	colTransactionTag        = "transaction_tag"
	colMetadata              = "metadata"
)

var allRelationshipCols = []string{
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
	colTimestamp,
	colCaveatName,
	colCaveatContext,
	colExpiration,
}
