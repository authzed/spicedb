package dynamodb

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Attr struct {
	Name string
	Type string
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
	EntityNamespace     = "namespace"
	EntityCaveat        = "caveat"
	EntityRelationTuple = "relation_tuple"
	ColEntity           = "entity"

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

	ColSnapshot = "snapshot"

	ColCounterName         = "name"
	ColCounterFilter       = "serialized_filter"
	ColCounterCurrentCount = "current_count"
	ColCounterSnapshot     = "updated_revision_snapshot"

	PREFIX_NS  = "NS"
	PREFIX_CAV = "CAV"
	PREFIX_REL = "REL"

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
	sf        = fmt.Sprintf
	Namespace = EntityAttr{
		PK:     NewCompositeFields(sf("%s#${%s}", PREFIX_NS, ColNamespace)),
		SK:     NewCompositeFields(sf("${%s}", ColCreatedXid)),
		LSI1SK: NewCompositeFields(sf("${%s}", ColDeletedXid)),
		GSI1PK: NewCompositeFields(sf("${%s}", ColEntity)),
		GSI1SK: NewCompositeFields(sf("%s#${%s}", PREFIX_NS, ColNamespace)),
		Entity: EntityNamespace,
		ExtraAttrs: []Attr{
			{
				Name: ColSerialized,
				Type: "B",
			},
		},
	}

	Caveat = EntityAttr{
		PK:     NewCompositeFields(sf("%s#${%s}", PREFIX_CAV, ColCaveat)),
		SK:     NewCompositeFields(sf("${%s}", ColCreatedXid)),
		LSI1SK: NewCompositeFields(sf("${%s}", ColDeletedXid)),
		GSI1PK: NewCompositeFields(sf("${%s}", ColEntity)),
		GSI1SK: NewCompositeFields(sf("%s#${%s}", PREFIX_CAV, ColCaveat)),
		Entity: EntityCaveat,
		ExtraAttrs: []Attr{
			{
				Name: ColSerialized,
				Type: "B",
			},
			{
				Name: ColCreatedTxn,
				Type: "S",
			},
			{
				Name: ColDeletedTxn,
				Type: "S",
			},
		},
	}

	RelationTuple = EntityAttr{
		PK: NewCompositeFields(sf("%s#${%s}#${%s}", PREFIX_REL, ColResourceType, ColObjectID)),
		SK: NewCompositeFields(sf("${%s}#${%s}", ColCreatedXid, ColDeletedXid)),
		LSI1SK: NewCompositeFields(
			sf("${%s}#${%s}#${%s}",
				ColRelation,
				ColUsersetObjectID,
				ColUsersetNamespace,
			)),
		LSI2SK: NewCompositeFields(sf("${%s}#${%s}#${%s}#${%s}",
			ColRelation,
			ColUsersetNamespace,
			ColUsersetObjectID,
			ColUsersetRelation,
		)),

		GSI1PK: NewCompositeFields(sf("%s#${%s}#${%s}",
			PREFIX_REL,
			ColUsersetNamespace,
			ColUsersetObjectID,
		)),
		GSI1SK: NewCompositeFields(sf("${%s}#${%s}#${%s}#${%s}",
			ColResourceType,
			ColRelation,
			ColUsersetRelation,
			ColObjectID,
		)),
		GSI2PK: NewCompositeFields(sf("${%s}", ColEntity)),
		Entity: EntityRelationTuple,
		ExtraAttrs: []Attr{
			{
				Name: ColCaveatContextName,
				Type: "S",
			},
			{
				Name: ColCaveatContext,
				Type: "S",
			},
			{
				Name: ColExpiration,
				Type: "S",
			},
		},
	}
)

func (ea EntityAttr) GetPutItem(kvp KeyValues, extraAttr map[string]interface{}) *types.PutRequest {

	kvp[ColEntity] = &ea.Entity

	Item := map[string]types.AttributeValue{}
	if ea.PK != nil && len(ea.PK.Build(kvp)) > 0 {
		Item[PK] = &types.AttributeValueMemberS{
			Value: ea.PK.Build(kvp),
		}
	}
	if ea.SK != nil && len(ea.SK.Build(kvp)) > 0 {
		Item[SK] = &types.AttributeValueMemberS{
			Value: ea.SK.Build(kvp),
		}
	}
	if ea.LSI1SK != nil && len(ea.LSI1SK.Build(kvp)) > 0 {
		Item[LSI1SK] = &types.AttributeValueMemberS{
			Value: ea.LSI1SK.Build(kvp),
		}
	}
	if ea.LSI2SK != nil && len(ea.LSI2SK.Build(kvp)) > 0 {
		Item[LSI2SK] = &types.AttributeValueMemberS{
			Value: ea.LSI2SK.Build(kvp),
		}
	}
	if ea.GSI1PK != nil && len(ea.GSI1PK.Build(kvp)) > 0 {
		Item[GSI1PK] = &types.AttributeValueMemberS{
			Value: ea.GSI1PK.Build(kvp),
		}
	}
	if ea.GSI1SK != nil && len(ea.GSI1SK.Build(kvp)) > 0 {
		Item[GSI1SK] = &types.AttributeValueMemberS{
			Value: ea.GSI1SK.Build(kvp),
		}
	}

	for _, v := range ea.ExtraAttrs {
		if value, exists := extraAttr[v.Name]; exists {
			switch v.Type {
			case "S":
				{
					Item[v.Name] = &types.AttributeValueMemberS{
						Value: value.(string),
					}
				}
			case "B":
				{
					Item[v.Name] = &types.AttributeValueMemberB{
						Value: value.([]byte),
					}
				}
			}
		}
	}

	Item[ColEntity] = &types.AttributeValueMemberS{
		Value: ea.Entity,
	}

	fmt.Printf("%#v\n", Item)

	return &types.PutRequest{
		Item: Item,
	}
}
