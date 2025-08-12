package dynamodb

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamodbReadWriterTx struct {
	datastore.Reader
	ds DynamodbDatastore
}

func NewDynamodbReadWriterTx(ds DynamodbDatastore) datastore.ReadWriteTransaction {
	return DynamodbReadWriterTx{
		&dynamodbReader{
			ds,
		},
		ds,
	}
}

// BulkLoad implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	panic("unimplemented")
}

// CountRelationships implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) CountRelationships(ctx context.Context, name string) (int, error) {
	panic("unimplemented")
}

// DeleteNamespaces implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	panic("unimplemented")
}

// DeleteRelationships implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, options ...options.DeleteOptionsOption) (uint64, bool, error) {
	panic("unimplemented")
}

// ReverseQueryRelationships implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, options ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	panic("unimplemented")
}

// StoreCounterValue implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	panic("unimplemented")
}

// RegisterCounter implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) RegisterCounter(ctx context.Context, name string, filter *corev1.RelationshipFilter) error {
	panic("unimplemented")
}

// UnregisterCounter implements datastore.ReadWriteTransaction.
func (d DynamodbReadWriterTx) UnregisterCounter(ctx context.Context, name string) error {
	panic("unimplemented")
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
		kvp[ColCreatedXid] = aws.String(time.Now().String())
		kvp[ColDeletedXid] = aws.String("")

		wr = append(wr, types.WriteRequest{
			PutRequest: Namespace.GetPutItem(kvp, map[string]interface{}{
				ColSerialized: serialized,
			}),
		})

	}

	out, err := d.ds.DynamoDbClient.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
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
	for _, mutation := range mutations {

		kvp := KeyValues{}
		kvp[ColResourceType] = &mutation.Relationship.Resource.ObjectType
		kvp[ColObjectID] = &mutation.Relationship.Resource.ObjectID
		kvp[ColRelation] = &mutation.Relationship.Resource.Relation
		kvp[ColUsersetNamespace] = &mutation.Relationship.Subject.ObjectType
		kvp[ColUsersetObjectID] = &mutation.Relationship.Subject.ObjectID
		kvp[ColUsersetRelation] = &mutation.Relationship.Subject.Relation

		kvp[ColCreatedXid] = aws.String(time.Now().String())
		kvp[ColDeletedXid] = aws.String("0")

		extraAttr := map[string]interface{}{}

		if mutation.Relationship.OptionalCaveat != nil {
			extraAttr[ColCaveatContextName] = mutation.Relationship.OptionalCaveat.CaveatName
			extraAttr[ColCaveatContext] = mutation.Relationship.OptionalCaveat.Context.String()
		}

		wr = append(wr, types.WriteRequest{
			PutRequest: RelationTuple.GetPutItem(kvp, extraAttr),
		})
	}

	d.ds.DynamoDbClient.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			TableName: wr,
		},
	})

	return nil
}

var _ datastore.ReadWriteTransaction = DynamodbReadWriterTx{}
