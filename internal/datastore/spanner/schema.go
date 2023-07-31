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

	tableCaveat         = "caveat"
	colName             = "name"
	colCaveatDefinition = "definition"
	colCaveatTS         = "timestamp"

	tableMetadata = "metadata"
	colUniqueID   = "unique_id"

	tableCounters = "relationship_estimate_counters"
	colID         = "id"
	colCount      = "count"
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
}
