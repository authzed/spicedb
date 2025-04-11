package schema

import (
	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
)

const (
	TableNamespace           = "namespace_config"
	TableTransaction         = "relation_tuple_transaction"
	TableTuple               = "relation_tuple"
	TableCaveat              = "caveat"
	TableRelationshipCounter = "relationship_counter"
	TableObjectData          = "object_data"

	ColXID               = "xid"
	ColTimestamp         = "timestamp"
	ColMetadata          = "metadata"
	ColNamespace         = "namespace"
	ColConfig            = "serialized_config"
	ColCreatedXid        = "created_xid"
	ColDeletedXid        = "deleted_xid"
	ColSnapshot          = "snapshot"
	ColObjectID          = "object_id"
	ColRelation          = "relation"
	ColUsersetNamespace  = "userset_namespace"
	ColUsersetObjectID   = "userset_object_id"
	ColUsersetRelation   = "userset_relation"
	ColCaveatName        = "name"
	ColCaveatDefinition  = "definition"
	ColCaveatContextName = "caveat_name"
	ColCaveatContext     = "caveat_context"
	ColExpiration        = "expiration"

	ColCounterName         = "name"
	ColCounterFilter       = "serialized_filter"
	ColCounterCurrentCount = "current_count"
	ColCounterSnapshot     = "updated_revision_snapshot"

	ColOdType       = "od_type"
	ColOdData       = "od_data"
	ColOdID         = "od_id"
	ColOdCreatedXid = "od_created_xid"
	ColOdDeletedXid = "od_deleted_xid"

	ConstrOdLiving = "uq_object_data_living"
)

func Schema(colOptimizationOpt common.ColumnOptimizationOption, expirationDisabled bool) *common.SchemaInformation {
	return common.NewSchemaInformationWithOptions(
		common.WithRelationshipTableName(TableTuple),
		common.WithColNamespace(ColNamespace),
		common.WithColObjectID(ColObjectID),
		common.WithColRelation(ColRelation),
		common.WithColUsersetNamespace(ColUsersetNamespace),
		common.WithColUsersetObjectID(ColUsersetObjectID),
		common.WithColUsersetRelation(ColUsersetRelation),
		common.WithColCaveatName(ColCaveatContextName),
		common.WithColCaveatContext(ColCaveatContext),
		common.WithColExpiration(ColExpiration),
		common.WithPaginationFilterType(common.TupleComparison),
		common.WithPlaceholderFormat(sq.Dollar),
		common.WithNowFunction("NOW"),
		common.WithColumnOptimization(colOptimizationOpt),
		common.WithExpirationDisabled(expirationDisabled),
		common.SetIndexes(pgIndexes),
	)
}
