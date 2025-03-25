package schema

import (
	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
)

const (
	TableNamespace           = "namespace_config"
	TableTuple               = "relation_tuple"
	TableTupleWithIntegrity  = "relation_tuple_with_integrity"
	TableTransactions        = "transactions"
	TableCaveat              = "caveat"
	TableRelationshipCounter = "relationship_counter"
	TableTransactionMetadata = "transaction_metadata"

	ColNamespace      = "namespace"
	ColConfig         = "serialized_config"
	ColTimestamp      = "timestamp"
	ColTransactionKey = "key"

	ColObjectID = "object_id"
	ColRelation = "relation"

	ColUsersetNamespace = "userset_namespace"
	ColUsersetObjectID  = "userset_object_id"
	ColUsersetRelation  = "userset_relation"

	ColCaveatName        = "name"
	ColCaveatDefinition  = "definition"
	ColCaveatContextName = "caveat_name"
	ColCaveatContext     = "caveat_context"
	ColExpiration        = "expires_at"

	ColIntegrityHash  = "integrity_hash"
	ColIntegrityKeyID = "integrity_key_id"

	ColCounterName             = "name"
	ColCounterSerializedFilter = "serialized_filter"
	ColCounterCurrentCount     = "current_count"
	ColCounterUpdatedAt        = "updated_at_timestamp"
	ColExpiresAt               = "expires_at"
	ColMetadata                = "metadata"
)

func Schema(colOptimizationOpt common.ColumnOptimizationOption, withIntegrity bool, expirationDisabled bool) *common.SchemaInformation {
	relTableName := TableTuple
	if withIntegrity {
		relTableName = TableTupleWithIntegrity
	}

	return common.NewSchemaInformationWithOptions(
		common.WithRelationshipTableName(relTableName),
		common.WithColNamespace(ColNamespace),
		common.WithColObjectID(ColObjectID),
		common.WithColRelation(ColRelation),
		common.WithColUsersetNamespace(ColUsersetNamespace),
		common.WithColUsersetObjectID(ColUsersetObjectID),
		common.WithColUsersetRelation(ColUsersetRelation),
		common.WithColCaveatName(ColCaveatContextName),
		common.WithColCaveatContext(ColCaveatContext),
		common.WithColExpiration(ColExpiration),
		common.WithColIntegrityKeyID(ColIntegrityKeyID),
		common.WithColIntegrityHash(ColIntegrityHash),
		common.WithColIntegrityTimestamp(ColTimestamp),
		common.WithPaginationFilterType(common.ExpandedLogicComparison),
		common.WithPlaceholderFormat(sq.Dollar),
		common.WithNowFunction("NOW"),
		common.WithColumnOptimization(colOptimizationOpt),
		common.WithIntegrityEnabled(withIntegrity),
		common.WithExpirationDisabled(expirationDisabled),
		common.SetIndexes(crdbIndexes),
	)
}
