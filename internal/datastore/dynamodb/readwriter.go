package dynamodb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamodbReadWriterTx struct {
	datastore.Reader
	ds  DynamodbDatastore
	xid revisions.HLCRevision
}

var (
	update = partiql.Update(TableName)
	delete = partiql.Delete(TableName)
)

func NewDynamodbReadWriterTx(ds DynamodbDatastore, xid revisions.HLCRevision) datastore.ReadWriteTransaction {
	return DynamodbReadWriterTx{
		&dynamodbReader{
			ds,
		},
		ds,
		xid,
	}
}

// BulkLoad implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	panic("unimplemented")
}

// DeleteNamespaces implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	statements := []types.BatchStatementRequest{}

	for _, name := range nsNames {
		deleteNamespace := delete

		query := deleteNamespace.
			Where(sq.Eq{PK: Namespace.PK.Build(KeyValues{ColEntity: &Namespace.Entity})}).
			Where(sq.Eq{SK: Namespace.SK.Build(KeyValues{ColNamespace: &name})})

		sql, args, err := query.ToSql()
		if err != nil {
			return err
		}
		parameters := []types.AttributeValue{}
		for _, v := range args {
			parameters = append(parameters, &types.AttributeValueMemberS{
				Value: v.(string),
			})
		}

		statements = append(statements, types.BatchStatementRequest{
			Statement:  aws.String(sql),
			Parameters: parameters,
		})
	}

	res, err := d.ds.client.BatchExecuteStatement(ctx, &ddbv2.BatchExecuteStatementInput{
		Statements: statements,
	})
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	fmt.Printf("output - %#v\n", res)

	return nil
}

// DeleteRelationships implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, options ...options.DeleteOptionsOption) (uint64, bool, error) {
	dsFilter, err := datastore.RelationshipsFilterFromPublicFilter(filter)
	if err != nil {
		return 0, false, fmt.Errorf("unable to translate relationship filter: %w", err)
	}

	iter, err := d.QueryRelationships(ctx, dsFilter)
	if err != nil {
		return 0, false, err
	}

	statements := []types.BatchStatementRequest{}

	for rel, err := range iter {
		if err != nil {
			fmt.Println(err)
			continue
		}

		kvp := KeyValues{}
		kvp[ColResourceType] = &rel.Resource.ObjectType
		kvp[ColRelation] = &rel.Resource.Relation
		kvp[ColObjectID] = &rel.Resource.ObjectID
		kvp[ColUsersetNamespace] = &rel.Subject.ObjectType
		kvp[ColUsersetRelation] = &rel.Subject.Relation
		kvp[ColUsersetObjectID] = &rel.Subject.ObjectID
		kvp[ColEntity] = &RelationTuple.Entity

		deleteRelationship := delete

		query := deleteRelationship.
			Where(sq.Eq{PK: RelationTuple.PK.Build(kvp)}).
			Where(sq.Eq{SK: RelationTuple.SK.Build(kvp)})

		sql, args, err := query.ToSql()
		if err != nil {
			fmt.Println(err)
			continue
		}
		parameters := []types.AttributeValue{}
		for _, v := range args {
			parameters = append(parameters, &types.AttributeValueMemberS{
				Value: v.(string),
			})
		}

		statements = append(statements, types.BatchStatementRequest{
			Statement:  aws.String(sql),
			Parameters: parameters,
		})
	}

	res, err := d.ds.client.BatchExecuteStatement(ctx, &ddbv2.BatchExecuteStatementInput{
		Statements: statements,
	})
	if err != nil {
		fmt.Println(err.Error())
		return 0, false, err
	}

	fmt.Printf("output - %#v\n", res)
	return 0, false, nil
}

// StoreCounterValue implements datastore.ReadWriteTransaction.
// TODO: computedAtRevision
func (d DynamodbReadWriterTx) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	// update the counter with value
	updateCounter := update
	query := updateCounter.
		Where(sq.Eq{PK: Counter.PK.Build(KeyValues{ColEntity: &Counter.Entity})}).
		Where(sq.Eq{SK: Counter.SK.Build(KeyValues{ColCounterName: &name})}).
		Set(ColCurrentCount, value)

	sql, args, err := query.ToSql()
	if err != nil {
		return err
	}
	parameters := []types.AttributeValue{}
	for _, v := range args {
		parameters = append(parameters, &types.AttributeValueMemberS{
			Value: v.(string),
		})
	}

	res, err := d.ds.client.ExecuteStatement(ctx, &ddbv2.ExecuteStatementInput{
		Statement:  aws.String(sql),
		Parameters: parameters,
	})
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	fmt.Printf("output - %#v\n", res)

	return nil
}

// RegisterCounter implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) RegisterCounter(ctx context.Context, name string, filter *corev1.RelationshipFilter) error {
	// create new counter

	kvp := KeyValues{}

	serialized, err := filter.MarshalVT()
	if err != nil {
		return fmt.Errorf("%d", err)
	}

	kvp[ColCounterName] = &name
	kvp[ColCreatedXid] = aws.String(d.xid.String())
	kvp[ColDeletedXid] = aws.String("")

	putRequest := Counter.GetPutItem(kvp, map[string]interface{}{
		ColSerialized: serialized,
	})

	out, err := d.ds.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &d.ds.tableName,
		Item:      putRequest.Item,
	})

	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("output - %#v\n", out)
	return nil
}

// UnregisterCounter implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) UnregisterCounter(ctx context.Context, name string) error {
	// delete the counter
	deleteCounter := delete

	query := deleteCounter.
		Where(sq.Eq{PK: Counter.PK.Build(KeyValues{ColEntity: &Counter.Entity})}).
		Where(sq.Eq{SK: Counter.SK.Build(KeyValues{ColCounterName: &name})})

	sql, args, err := query.ToSql()
	if err != nil {
		return err
	}
	parameters := []types.AttributeValue{}
	for _, v := range args {
		parameters = append(parameters, &types.AttributeValueMemberS{
			Value: v.(string),
		})
	}

	res, err := d.ds.client.ExecuteStatement(ctx, &ddbv2.ExecuteStatementInput{
		Statement:  aws.String(sql),
		Parameters: parameters,
	})
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	fmt.Printf("output - %#v\n", res)

	return nil
}

// WriteNamespaces implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) WriteNamespaces(ctx context.Context, newConfigs ...*corev1.NamespaceDefinition) error {
	wr := []types.WriteRequest{}

	for _, newNamespace := range newConfigs {
		serialized, err := newNamespace.MarshalVT()
		if err != nil {
			return fmt.Errorf("%d", err)
		}

		kvp := KeyValues{}

		kvp[ColNamespace] = &newNamespace.Name
		kvp[ColCreatedXid] = aws.String(d.xid.String())
		kvp[ColDeletedXid] = aws.String("")

		wr = append(wr, types.WriteRequest{
			PutRequest: Namespace.GetPutItem(kvp, map[string]interface{}{
				ColSerialized: serialized,
			}),
		})

	}

	out, err := d.ds.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			TableName: wr,
		},
	})

	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("output - %#v\n", out)
	return nil

}

// WriteRelationships implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	wr := []types.WriteRequest{}
	statements := []types.BatchStatementRequest{}

	for _, mut := range mutations {

		kvp := KeyValues{}
		kvp[ColResourceType] = &mut.Relationship.Resource.ObjectType
		kvp[ColObjectID] = &mut.Relationship.Resource.ObjectID
		kvp[ColRelation] = &mut.Relationship.Resource.Relation
		kvp[ColUsersetNamespace] = &mut.Relationship.Subject.ObjectType
		kvp[ColUsersetObjectID] = &mut.Relationship.Subject.ObjectID
		kvp[ColUsersetRelation] = &mut.Relationship.Subject.Relation
		kvp[ColEntity] = &RelationTuple.Entity

		kvp[ColCreatedXid] = aws.String(d.xid.String())
		kvp[ColDeletedXid] = aws.String("0")

		extraAttr := map[string]interface{}{}

		if mut.Relationship.OptionalCaveat != nil {
			extraAttr[ColCaveatContextName] = mut.Relationship.OptionalCaveat.CaveatName
			extraAttr[ColCaveatContext] = mut.Relationship.OptionalCaveat.Context.String()
		}

		switch mut.Operation {
		case tuple.UpdateOperationCreate, tuple.UpdateOperationTouch:
			wr = append(wr, types.WriteRequest{
				PutRequest: RelationTuple.GetPutItem(kvp, extraAttr),
			})
		case tuple.UpdateOperationDelete:
			deleteRelation := delete

			query := deleteRelation.
				Where(sq.Eq{PK: RelationTuple.PK.Build(kvp)}).
				Where(sq.Eq{SK: RelationTuple.SK.Build(kvp)})

			sql, args, err := query.ToSql()
			if err != nil {
				fmt.Println(err)
				continue
			}
			parameters := []types.AttributeValue{}
			for _, v := range args {
				parameters = append(parameters, &types.AttributeValueMemberS{
					Value: v.(string),
				})
			}
			statements = append(statements, types.BatchStatementRequest{
				Statement:  aws.String(sql),
				Parameters: parameters,
			})
		default:
			return spiceerrors.MustBugf("unknown tuple mutation: %v", mut)
		}
	}

	if len(wr) != 0 {
		_, err := d.ds.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				TableName: wr,
			},
		})

		if err != nil {
			return err
		}
	}

	if len(statements) != 0 {
		res, err := d.ds.client.BatchExecuteStatement(ctx, &ddbv2.BatchExecuteStatementInput{
			Statements: statements,
		})
		if err != nil {
			fmt.Println(err.Error())
			return err
		}

		fmt.Printf("output - %#v\n", res)
	}

	return nil
}

var _ datastore.ReadWriteTransaction = DynamodbReadWriterTx{}
