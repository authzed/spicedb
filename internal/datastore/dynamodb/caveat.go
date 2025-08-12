package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/aws"
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

		fmt.Printf("%#t\n", caveat)

		kvp[ColCaveat] = &caveat.Name
		kvp[ColCreatedXid] = aws.String(time.Now().String())
		kvp[ColDeletedXid] = aws.String("")

		wr = append(wr, types.WriteRequest{
			PutRequest: Caveat.GetPutItem(kvp, map[string]interface{}{
				ColSerialized: serialized,
			}),
		})

	}
	d.ds.DynamoDbClient.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			TableName: wr,
		},
	})

	return nil
}

// ReadCaveatByName implements datastore.ReadWriteTransaction.
func (d dynamodbReader) ReadCaveatByName(ctx context.Context, name string) (caveat *corev1.CaveatDefinition, lastWritten datastore.Revision, err error) {
	panic("unimplemented")
}

// LookupCaveatsWithNames implements datastore.ReadWriteTransaction.
func (d dynamodbReader) LookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("names list cannot be empty")
	}

	values := []expression.OperandBuilder{}
	for _, name := range names {
		n := Caveat.PK.Build(KeyValues{
			ColCaveat: &name,
		})
		values = append(values, expression.Value(n))
	}
	var filter expression.ConditionBuilder

	filter = expression.Name(PK).In(values[0], values[1:]...)

	key := expression.KeyEqual(expression.Key(GSI1PK), expression.Value(EntityCaveat))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		WithFilter(filter).
		Build()
	if err != nil {
		return nil, err
	}

	res, err := d.ds.QueryItem(ctx, expr, IDX_GSI1)

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
	key := expression.KeyEqual(expression.Key(GSI1PK), expression.Value(EntityCaveat))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		Build()
	if err != nil {
		return nil, err
	}

	res, err := d.ds.QueryItem(ctx, expr, IDX_GSI1)

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
	panic("unimplemented")
}
