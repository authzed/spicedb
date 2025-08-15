package dynamodb

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

type dynamodbReader struct {
	ds DynamodbDatastore
}

func NewDynamodbReader(ds DynamodbDatastore) datastore.Reader {
	return dynamodbReader{
		ds,
	}
}

// CountRelationships implements datastore.Reader.
func (d dynamodbReader) CountRelationships(ctx context.Context, name string) (int, error) {
	panic("unimplemented")
}

// ListAllNamespaces implements datastore.Reader.
func (d dynamodbReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	key := expression.KeyEqual(expression.Key(GSI1PK), expression.Value(EntityNamespace))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		Build()
	if err != nil {
		return nil, err
	}

	res, err := d.ds.QueryItem(ctx, expr, IDX_GSI1)

	resNs := []datastore.RevisionedNamespace{}

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

		namespace := &corev1.NamespaceDefinition{}

		err := namespace.UnmarshalVT(binaryAttr)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal protobuf: %w", err)
		}

		resNs = append(resNs, datastore.RevisionedNamespace{
			Definition:          namespace,
			LastWrittenRevision: datastore.NoRevision,
		})
	}

	return resNs, nil
}

// LookupCounters implements datastore.Reader.
func (d dynamodbReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	panic("unimplemented")
}

// LookupNamespacesWithNames implements datastore.Reader.
func (d dynamodbReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {

	if len(nsNames) == 0 {
		return nil, fmt.Errorf("nsNames list cannot be empty")
	}

	values := []expression.OperandBuilder{}
	for _, name := range nsNames {
		ns := Namespace.PK.Build(KeyValues{
			ColNamespace: &name,
		})
		values = append(values, expression.Value(ns))
	}
	var filter expression.ConditionBuilder

	key := expression.KeyEqual(expression.Key(GSI1PK), expression.Value(EntityNamespace))

	filter = expression.Name(PK).In(values[0], values[1:]...)

	expr, err := expression.NewBuilder().
		WithFilter(filter).
		WithKeyCondition(key).
		Build()
	if err != nil {
		return nil, err
	}

	res, err := d.ds.QueryItem(ctx, expr, IDX_GSI1)
	if err != nil {
		return nil, err
	}

	resNs := []datastore.RevisionedNamespace{}

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

		namespace := &corev1.NamespaceDefinition{}

		err := namespace.UnmarshalVT(binaryAttr)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal protobuf: %w", err)
		}

		resNs = append(resNs, datastore.RevisionedNamespace{
			Definition:          namespace,
			LastWrittenRevision: datastore.NoRevision,
		})
	}

	return resNs, nil
}

// QueryRelationships implements datastore.Reader.
func (d dynamodbReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, options ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	// panic("unimplemented")

	return nil, nil
}

// ReadNamespaceByName implements datastore.Reader.
func (d dynamodbReader) ReadNamespaceByName(ctx context.Context, nsName string) (ns *corev1.NamespaceDefinition, lastWritten datastore.Revision, err error) {

	n := Namespace.PK.Build(KeyValues{
		ColNamespace: &nsName,
	})

	key := expression.KeyEqual(expression.Key(GSI1PK), expression.Value(EntityNamespace))

	key = key.And(expression.KeyEqual(expression.Key(GSI1SK), expression.Value(n)))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		Build()
	if err != nil {
		return nil, nil, err
	}

	res, err := d.ds.QueryItem(ctx, expr, IDX_GSI1)

	if len(res) < 1 {
		return nil, nil, fmt.Errorf("No namespace for given name %s", nsName)
	}

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

	namespace := &corev1.NamespaceDefinition{}

	err = namespace.UnmarshalVT(binaryAttr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	return namespace, nil, nil
}

// ReverseQueryRelationships implements datastore.Reader.
func (d dynamodbReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, options ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	panic("unimplemented")
}

var _ datastore.Reader = &dynamodbReader{}
