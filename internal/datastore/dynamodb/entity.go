package dynamodb

import (
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Attr struct {
	Name  string
	Type  string
	Value *CompositeFields
}

type EntityAttr struct {
	PK         *CompositeFields
	SK         *CompositeFields
	LSI1SK     *CompositeFields
	LSI2SK     *CompositeFields
	GSI1PK     *CompositeFields
	GSI1SK     *CompositeFields
	GSI2PK     *CompositeFields
	GSI2SK     *CompositeFields
	Entity     string
	ExtraAttrs []Attr
}

const (
	EntityNamespace                = "namespace"
	EntityCaveat                   = "caveat"
	EntityRelationTuple            = "relation_tuple"
	EntityRelationTupleTransaction = "relation_tuple_transaction"
	EntityCounter                  = "relationship_counter"
	ColEntity                      = "entity"

	ColCreatedXid = "created_xid"
	ColDeletedXid = "deleted_xid"
	ColNamespace  = "namespace"
	ColCaveat     = "caveat"
	ColSerialized = "serialized"
	ColCreatedTxn = "created_transaction"
	ColDeletedTxn = "deleted_transaction"

	ColResourceType      = "namespace"
	ColObjectID          = "object_id"
	ColRelation          = "relation"
	ColUsersetNamespace  = "userset_namespace"
	ColUsersetObjectID   = "userset_object_id"
	ColUsersetRelation   = "userset_relation"
	ColCaveatContextName = "caveat_name"
	ColCaveatContext     = "caveat_context"
	ColExpiration        = "expiration"

	ColSnapshot     = "snapshot"
	ColMetadata     = "Metadata"
	ColCounterName  = "name"
	ColCurrentCount = "current_count"
	// ColCounterFilter       = "serialized_filter"
	ColCounterCurrentCount = "current_count"
	ColCounterSnapshot     = "updated_revision_snapshot"

	PREFIX_NS      = "NS"
	PREFIX_CAV     = "CAV"
	PREFIX_REL     = "REL"
	PREFIX_TXN     = "REL_TXN"
	PREFIX_COUNTER = "COUNT"

	TYPE_BINARY    = "B"
	TYPE_STRING    = "S"
	TYPE_COMPOSITE = "C"
	TYPE_NUMBER    = "N"

	PK     = "PK"
	SK     = "SK"
	LSI1SK = "LSI1SK"
	LSI2SK = "LSI2SK"
	GSI1PK = "GSI1PK"
	GSI1SK = "GSI1SK"
	GSI2PK = "GSI2PK"
	GSI2SK = "GSI2SK"

	IDX_LSI1 = "LSI1"
	IDX_LSI2 = "LSI2"
	IDX_GSI1 = "GSI1"
	IDX_GSI2 = "GSI2"
)

var (
	Namespace = EntityAttr{
		PK:     NewCompositeFields(PREFIX_NS, ColEntity),
		SK:     NewCompositeFields(PREFIX_NS, ColNamespace),
		LSI1SK: NewCompositeFields(PREFIX_NS, ColNamespace),
		Entity: EntityNamespace,
		ExtraAttrs: []Attr{
			{
				Name: ColSerialized,
				Type: TYPE_BINARY,
			},
			{
				Name:  ColCreatedXid,
				Type:  TYPE_COMPOSITE,
				Value: NewCompositeFieldsWithoutPrefix(ColCreatedXid),
			},
		},
	}

	Caveat = EntityAttr{
		PK:     NewCompositeFields(PREFIX_CAV, ColEntity),
		SK:     NewCompositeFields(PREFIX_CAV, ColCaveat),
		LSI1SK: NewCompositeFields(PREFIX_CAV, ColCaveat),
		Entity: EntityCaveat,
		ExtraAttrs: []Attr{
			{
				Name: ColSerialized,
				Type: TYPE_BINARY,
			},
			{
				Name: ColCreatedTxn,
				Type: TYPE_STRING,
			},
			{
				Name: ColDeletedTxn,
				Type: TYPE_STRING,
			},
			{
				Name:  ColCreatedXid,
				Type:  TYPE_COMPOSITE,
				Value: NewCompositeFieldsWithoutPrefix(ColCreatedXid),
			},
		},
	}

	RelationTuple = EntityAttr{
		PK: NewCompositeFields(
			PREFIX_REL,
			ColResourceType,
			ColRelation,
			ColObjectID,
		),
		SK: NewCompositeFields(
			PREFIX_REL,
			ColUsersetNamespace,
			ColUsersetObjectID,
			ColUsersetRelation,
		),
		LSI1SK: NewCompositeFields(
			PREFIX_REL,
			ColUsersetObjectID,
			ColUsersetNamespace,
			ColUsersetRelation,
		),
		GSI1PK: NewCompositeFields(
			PREFIX_REL,
			ColResourceType,
		),
		GSI1SK: NewCompositeFields(
			PREFIX_REL,
			ColRelation,
			ColUsersetNamespace,
			ColObjectID,
			ColUsersetObjectID,
		),
		GSI2PK: NewCompositeFields(
			PREFIX_REL,
			ColUsersetNamespace,
		),
		GSI2SK: NewCompositeFields(
			PREFIX_REL,
			ColResourceType,
			ColUsersetRelation,
			ColRelation,
			ColObjectID,
		),
		Entity: EntityRelationTuple,
		ExtraAttrs: []Attr{
			{
				Name: ColCaveatContextName,
				Type: TYPE_STRING,
			},
			{
				Name: ColCaveatContext,
				Type: TYPE_STRING,
			},
			{
				Name: ColExpiration,
				Type: TYPE_STRING,
			},
			{
				Name:  ColResourceType,
				Type:  TYPE_COMPOSITE,
				Value: NewCompositeFieldsWithoutPrefix(ColResourceType),
			},

			{
				Name:  ColRelation,
				Type:  TYPE_COMPOSITE,
				Value: NewCompositeFieldsWithoutPrefix(ColRelation),
			},
			{
				Name:  ColObjectID,
				Type:  TYPE_COMPOSITE,
				Value: NewCompositeFieldsWithoutPrefix(ColObjectID),
			},
			{
				Name:  ColUsersetNamespace,
				Type:  TYPE_COMPOSITE,
				Value: NewCompositeFieldsWithoutPrefix(ColUsersetNamespace),
			},
			{
				Name:  ColUsersetObjectID,
				Type:  TYPE_COMPOSITE,
				Value: NewCompositeFieldsWithoutPrefix(ColUsersetObjectID),
			},
			{
				Name:  ColUsersetRelation,
				Type:  TYPE_COMPOSITE,
				Value: NewCompositeFieldsWithoutPrefix(ColUsersetRelation),
			},
			{
				Name:  ColCreatedXid,
				Type:  TYPE_COMPOSITE,
				Value: NewCompositeFieldsWithoutPrefix(ColCreatedXid),
			},
			{
				Name: EntityRelationTuple,
				Type: TYPE_COMPOSITE,
				Value: NewCompositeFields(
					PREFIX_REL,
					ColNamespace,
					ColRelation,
					ColObjectID,
					ColUsersetNamespace,
					ColUsersetRelation,
					ColUsersetObjectID,
				),
			},
		},
	}

	Transaction = EntityAttr{
		PK:     NewCompositeFields(PREFIX_TXN),
		SK:     NewCompositeFieldsWithoutPrefix(ColCreatedXid),
		Entity: EntityRelationTupleTransaction,
		ExtraAttrs: []Attr{
			{
				Name: ColMetadata,
				Type: TYPE_STRING,
			},
		},
	}

	Counter = EntityAttr{
		PK:     NewCompositeFields(PREFIX_COUNTER),
		SK:     NewCompositeFieldsWithoutPrefix(ColCounterName),
		Entity: EntityCounter,
		ExtraAttrs: []Attr{
			{
				Name: ColSerialized,
				Type: TYPE_BINARY,
			},
			{
				Name: ColCurrentCount,
				Type: TYPE_NUMBER,
			},
			{
				Name:  ColCreatedXid,
				Type:  TYPE_COMPOSITE,
				Value: NewCompositeFieldsWithoutPrefix(ColCreatedXid),
			},
		},
	}
)

func (ea EntityAttr) GetExtraAttrField(field string) Attr {
	i := slices.IndexFunc[[]Attr, Attr](ea.ExtraAttrs, func(e Attr) bool {
		return e.Name == field
	})

	if i != -1 {
		return ea.ExtraAttrs[i]
	}

	return Attr{}
}

type Item map[string]types.AttributeValue

func addValueToItem(item Item, key string, comp *CompositeFields, kvp KeyValues) {
	if comp != nil && len(comp.Build(kvp)) > 0 {
		(item)[key] = &types.AttributeValueMemberS{
			Value: comp.Build(kvp),
		}
	}
}

func (ea EntityAttr) GetPutItem(kvp KeyValues, extraAttr map[string]interface{}) *types.PutRequest {

	kvp[ColEntity] = &ea.Entity

	item := Item{}

	addValueToItem(item, PK, ea.PK, kvp)
	addValueToItem(item, SK, ea.SK, kvp)
	addValueToItem(item, LSI1SK, ea.LSI1SK, kvp)
	addValueToItem(item, LSI2SK, ea.LSI2SK, kvp)
	addValueToItem(item, GSI1PK, ea.GSI1PK, kvp)
	addValueToItem(item, GSI1SK, ea.GSI1SK, kvp)

	for _, v := range ea.ExtraAttrs {
		if v.Type == TYPE_COMPOSITE {
			addValueToItem(item, v.Name, v.Value, kvp)
		}

		if extraAttr == nil {
			continue
		}

		if value, exists := extraAttr[v.Name]; exists {
			switch v.Type {
			case TYPE_STRING:
				{
					item[v.Name] = &types.AttributeValueMemberS{
						Value: value.(string),
					}
				}
			case TYPE_NUMBER:
				{
					item[v.Name] = &types.AttributeValueMemberN{
						Value: value.(string),
					}
				}
			case TYPE_BINARY:
				{
					item[v.Name] = &types.AttributeValueMemberB{
						Value: value.([]byte),
					}
				}
			}
		}
	}

	item[ColEntity] = &types.AttributeValueMemberS{
		Value: ea.Entity,
	}

	return &types.PutRequest{
		Item: item,
	}
}
