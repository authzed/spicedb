package dynamodb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/authzed/spicedb/pkg/datastore"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// WriteCaveats implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) WriteCaveats(ctx context.Context, caveats []*corev1.CaveatDefinition) error {

	wr := []types.WriteRequest{}
	for _, caveat := range caveats {
		serialized, err := caveat.MarshalVT()
		if err != nil {
			return fmt.Errorf("%d", err)
		}

		kvp := KeyValues{}

		// fmt.Printf("%#t\n", caveat)

		kvp[ColCaveat] = &caveat.Name
		kvp[ColCreatedXid] = aws.String(d.xid.String())
		kvp[ColDeletedXid] = aws.String("")

		wr = append(wr, types.WriteRequest{
			PutRequest: Caveat.GetPutItem(kvp, map[string]interface{}{
				ColSerialized: serialized,
			}),
		})

	}
	d.ds.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			TableName: wr,
		},
	})

	return nil
}

// ReadCaveatByName implements datastore.ReadWriteTransaction.
func (d dynamodbReader) ReadCaveatByName(ctx context.Context, name string) (caveat *corev1.CaveatDefinition, lastWritten datastore.Revision, err error) {

	key := expression.KeyEqual(expression.Key(SK), expression.Value(Caveat.SK.Build(
		KeyValues{
			ColCaveat: &name,
		})))

	key = key.And(expression.KeyEqual(expression.Key(PK), expression.Value(Caveat.PK.Build(
		KeyValues{
			ColEntity: &Caveat.Entity,
		}))))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		Build()
	if err != nil {
		return nil, nil, err
	}

	res, err := d.ds.QueryItem(ctx, &expr)

	item, ok := res[0].(map[string]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("")
	}

	serializedAttr, exists := item[ColSerialized]
	if !exists {
		return nil, nil, fmt.Errorf("serialized attribute not found")
	}

	binaryAttr, ok := serializedAttr.([]byte)
	if !ok {
		return nil, nil, fmt.Errorf("serialized attribute is not binary type")
	}

	caveat = &corev1.CaveatDefinition{}

	err = caveat.UnmarshalVT(binaryAttr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	return caveat, nil, nil
}

// LookupCaveatsWithNames implements datastore.ReadWriteTransaction.
func (d dynamodbReader) LookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("names list cannot be empty")
	}

	values := []expression.OperandBuilder{}
	for _, name := range names {
		n := Caveat.LSI1SK.Build(KeyValues{
			ColCaveat: &name,
		})
		values = append(values, expression.Value(n))
	}
	var filter expression.ConditionBuilder

	filter = expression.Name(LSI1SK).In(values[0], values[1:]...)

	key := expression.KeyEqual(expression.Key(PK), expression.Value(Caveat.PK.Build(KeyValues{
		ColEntity: &Caveat.Entity,
	})))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		WithFilter(filter).
		Build()
	if err != nil {
		return nil, err
	}

	res, err := d.ds.QueryItem(ctx, &expr)

	resNs := []datastore.RevisionedCaveat{}

	for _, r := range res {
		item, ok := r.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("")
		}

		serializedAttr, exists := item[ColSerialized]
		if !exists {
			return nil, fmt.Errorf("serialized attribute not found")
		}

		binaryAttr, ok := serializedAttr.([]byte)
		if !ok {
			return nil, fmt.Errorf("serialized attribute is not binary type")
		}

		caveat := &corev1.CaveatDefinition{}

		err := caveat.UnmarshalVT(binaryAttr)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal protobuf: %w", err)
		}

		resNs = append(resNs, datastore.RevisionedCaveat{
			Definition:          caveat,
			LastWrittenRevision: datastore.NoRevision,
		})
	}

	return resNs, nil
}

func (d dynamodbReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	key := expression.KeyEqual(expression.Key(PK), expression.Value(Caveat.PK.Build(KeyValues{
		ColEntity: &Caveat.Entity,
	})))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		Build()
	if err != nil {
		return nil, err
	}

	res, err := d.ds.QueryItem(ctx, &expr)

	resNs := []datastore.RevisionedCaveat{}

	for _, r := range res {
		item, ok := r.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("")
		}

		serializedAttr, exists := item[ColSerialized]
		if !exists {
			return nil, fmt.Errorf("serialized attribute not found")
		}

		binaryAttr, ok := serializedAttr.([]byte)
		if !ok {
			return nil, fmt.Errorf("serialized attribute is not binary type")
		}

		caveat := &corev1.CaveatDefinition{}

		err := caveat.UnmarshalVT(binaryAttr)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal protobuf: %w", err)
		}

		resNs = append(resNs, datastore.RevisionedCaveat{
			Definition:          caveat,
			LastWrittenRevision: datastore.NoRevision,
		})
	}

	return resNs, nil
}

// DeleteCaveats implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) DeleteCaveats(ctx context.Context, names []string) error {
	statements := []types.BatchStatementRequest{}

	for _, name := range names {
		deleteCaveat := delete
		query := deleteCaveat.
			Where(sq.Eq{PK: Caveat.PK.Build(KeyValues{ColEntity: &Caveat.Entity})}).
			Where(sq.Eq{SK: Caveat.SK.Build(KeyValues{ColCaveat: &name})})

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
