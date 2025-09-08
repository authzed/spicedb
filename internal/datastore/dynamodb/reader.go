package dynamodb

import (
	"context"
	"fmt"
	"math"

	sq "github.com/Masterminds/squirrel"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddbv2 "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	// get the counter by name
	key := expression.KeyEqual(expression.Key(PK), expression.Value(Counter.PK.Build(KeyValues{
		ColEntity: &Namespace.Entity,
	})))

	key = key.And(expression.KeyEqual(expression.Key(SK), expression.Value(Counter.SK.Build(KeyValues{
		ColCounterName: &name,
	}))))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		Build()

	if err != nil {
		return 0, err
	}

	res, err := d.ds.QueryItem(ctx, &expr)

	if len(res) < 1 {
		return 0, fmt.Errorf("No Counter for given name %s", name)
	}

	item, ok := res[0].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("")
	}

	count := 0

	if v, exists := item[ColCounterCurrentCount]; exists {
		count = v.(int)
	}

	return count, nil
}

// ListAllNamespaces implements datastore.Reader.
func (d dynamodbReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	key := expression.
		KeyEqual(
			expression.Key(PK),
			expression.Value(Namespace.
				PK.Build(KeyValues{
				ColEntity: &Namespace.Entity,
			})))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		Build()
	if err != nil {
		return nil, err
	}

	res, err := d.ds.QueryItem(ctx, &expr)

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
	// return all counters
	key := expression.KeyEqual(expression.Key(PK), expression.Value(Counter.PK.Build(KeyValues{
		ColEntity: &Counter.Entity,
	})))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		Build()
	if err != nil {
		return nil, err
	}

	res, err := d.ds.QueryItem(ctx, &expr)

	resCounters := []datastore.RelationshipCounter{}

	for _, r := range res {
		item, ok := r.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("")
		}

		name := ""
		count := 0

		if v, exists := item[ColCounterName]; exists {
			name = v.(string)
		}
		if v, exists := item[ColCounterCurrentCount]; exists {
			count = v.(int)
		}

		serializedAttr, exists := item[ColSerialized]
		if !exists {
			return nil, fmt.Errorf("serialized attribute not found")
		}

		binaryAttr, ok := serializedAttr.([]byte)
		if !ok {
			return nil, fmt.Errorf("serialized attribute is not binary type")
		}

		filter := &corev1.RelationshipFilter{}

		err := filter.UnmarshalVT(binaryAttr)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal protobuf: %w", err)
		}

		resCounters = append(resCounters, datastore.RelationshipCounter{
			Name:               name,
			Filter:             filter,
			Count:              count,
			ComputedAtRevision: datastore.NoRevision,
		})
	}

	return resCounters, nil
}

// LookupNamespacesWithNames implements datastore.Reader.
func (d dynamodbReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {

	if len(nsNames) == 0 {
		return nil, fmt.Errorf("nsNames list cannot be empty")
	}

	values := []expression.OperandBuilder{}
	for _, name := range nsNames {
		ns := Namespace.LSI1SK.Build(KeyValues{
			ColNamespace: &name,
		})
		values = append(values, expression.Value(ns))
	}
	var filter expression.ConditionBuilder

	key := expression.KeyEqual(expression.Key(PK), expression.Value(Namespace.PK.Build(KeyValues{
		ColEntity: &Namespace.Entity,
	})))

	filter = expression.Name(LSI1SK).In(values[0], values[1:]...)

	expr, err := expression.NewBuilder().
		WithFilter(filter).
		WithKeyCondition(key).
		Build()
	if err != nil {
		return nil, err
	}

	res, err := d.ds.QueryItem(ctx, &expr)
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
func (d dynamodbReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	query := Query(filter)

	query = query.Column(EntityRelationTuple)

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}
	parameters := []types.AttributeValue{}
	for _, v := range args {
		parameters = append(parameters, &types.AttributeValueMemberS{
			Value: v.(string),
		})
	}

	fmt.Printf("%s, \t %s\n", sql, args)

	// for _, item := range res.Items {
	// 	item[EntityRelationTuple]
	// }

	// var out interface{}

	// err = attributevalue.UnmarshalListOfMaps(res.Items, &out)
	// if err != nil {
	// 	fmt.Printf("%#v\n ", err)
	// 	return nil, err
	// }

	// fmt.Printf("%#v", out[0][EntityRelationTuple])

	var resourceObjectType string
	var resourceObjectID string
	var resourceRelation string
	var subjectObjectType string
	var subjectObjectID string
	var subjectRelation string
	// var caveatName sql.NullString
	// var caveatCtx C
	// var expiration *time.Time

	// var integrityKeyID string
	// var integrityHash []byte
	// var timestamp time.Time

	return func(yield func(tuple.Relationship, error) bool) {
		fmt.Println("In side yield function")
		res, err := d.ds.client.ExecuteStatement(ctx, &ddbv2.ExecuteStatementInput{
			Statement:  aws.String(sql),
			Parameters: parameters,
		})
		if err != nil {
			fmt.Println(err.Error())
			if !yield(tuple.Relationship{}, err) {
				return
			}
		}

		kvp := KeyValues{}
		kvp[ColNamespace] = &resourceObjectType
		kvp[ColObjectID] = &resourceObjectID
		kvp[ColRelation] = &resourceRelation
		kvp[ColUsersetNamespace] = &subjectObjectType
		kvp[ColUsersetObjectID] = &subjectObjectID
		kvp[ColUsersetRelation] = &subjectRelation
		kvp[ColEntity] = &RelationTuple.Entity

		rtAttr := RelationTuple.GetExtraAttrField(EntityRelationTuple)

		for _, item := range res.Items {
			var out map[string]string
			attributevalue.UnmarshalMap(item, &out)

			// if !exists {
			// 	if !yield(tuple.Relationship{}, err) {
			// 		return
			// 	}
			// }

			rt, exists := out[EntityRelationTuple]

			if !exists {
				fmt.Println(rt)
				if !yield(tuple.Relationship{}, err) {
					return
				}
			}

			rtAttr.Value.Extract(rt, kvp)

			fmt.Printf("%#v\n", out)

			fmt.Printf("%#v\n", kvp)

			if !yield(tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: resourceObjectType,
						ObjectID:   resourceObjectID,
						Relation:   resourceRelation,
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: subjectObjectType,
						ObjectID:   subjectObjectID,
						Relation:   subjectRelation,
					},
				},
				// OptionalCaveat:     caveat,
				// OptionalExpiration: expiration,
				// OptionalIntegrity:  integrity,
			}, nil) {
				return
			}
		}

	}, nil
}

// ReadNamespaceByName implements datastore.Reader.
func (d dynamodbReader) ReadNamespaceByName(ctx context.Context, nsName string) (ns *corev1.NamespaceDefinition, lastWritten datastore.Revision, err error) {

	n := Namespace.SK.Build(KeyValues{
		ColNamespace: &nsName,
	})

	key := expression.KeyEqual(expression.Key(PK), expression.Value(Namespace.PK.Build(KeyValues{
		ColEntity: &Namespace.Entity,
	})))

	key = key.And(expression.KeyEqual(expression.Key(SK), expression.Value(n)))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		Build()
	if err != nil {
		return nil, nil, err
	}

	res, err := d.ds.QueryItem(ctx, &expr)

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
	filter := datastore.RelationshipsFilter{
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			subjectsFilter.AsSelector(),
		},
	}

	query := Query(filter)

	query = query.Column(EntityRelationTuple)

	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}
	parameters := []types.AttributeValue{}
	for _, v := range args {
		parameters = append(parameters, &types.AttributeValueMemberS{
			Value: v.(string),
		})
	}

	fmt.Printf("%s, \t %s\n", sql, args)

	var resourceObjectType string
	var resourceObjectID string
	var resourceRelation string
	var subjectObjectType string
	var subjectObjectID string
	var subjectRelation string
	// var caveatName sql.NullString
	// var caveatCtx C
	// var expiration *time.Time

	// var integrityKeyID string
	// var integrityHash []byte
	// var timestamp time.Time

	return func(yield func(tuple.Relationship, error) bool) {
		fmt.Println("In side yield function")
		res, err := d.ds.client.ExecuteStatement(ctx, &ddbv2.ExecuteStatementInput{
			Statement:  aws.String(sql),
			Parameters: parameters,
		})
		if err != nil {
			fmt.Println(err.Error())
			if !yield(tuple.Relationship{}, err) {
				return
			}
		}

		kvp := KeyValues{}
		kvp[ColNamespace] = &resourceObjectType
		kvp[ColObjectID] = &resourceObjectID
		kvp[ColRelation] = &resourceRelation
		kvp[ColUsersetNamespace] = &subjectObjectType
		kvp[ColUsersetObjectID] = &subjectObjectID
		kvp[ColUsersetRelation] = &subjectRelation
		kvp[ColEntity] = &RelationTuple.Entity

		rtAttr := RelationTuple.GetExtraAttrField(EntityRelationTuple)

		for _, item := range res.Items {
			var out map[string]string
			attributevalue.UnmarshalMap(item, &out)

			// if !exists {
			// 	if !yield(tuple.Relationship{}, err) {
			// 		return
			// 	}
			// }

			rt, exists := out[EntityRelationTuple]

			if !exists {
				fmt.Println(rt)
				if !yield(tuple.Relationship{}, err) {
					return
				}
			}

			rtAttr.Value.Extract(rt, kvp)

			fmt.Printf("%#v\n", out)

			fmt.Printf("%#v\n", kvp)

			if !yield(tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: resourceObjectType,
						ObjectID:   resourceObjectID,
						Relation:   resourceRelation,
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: subjectObjectType,
						ObjectID:   subjectObjectID,
						Relation:   subjectRelation,
					},
				},
				// OptionalCaveat:     caveat,
				// OptionalExpiration: expiration,
				// OptionalIntegrity:  integrity,
			}, nil) {
				return
			}
		}

	}, nil
}

var _ datastore.Reader = &dynamodbReader{}

func QueryHint(filter datastore.RelationshipsFilter) (*DynamodbIndex, KeyValues) {
	avaiableFields := []string{}

	kvp := KeyValues{}

	if filter.OptionalResourceType != "" {
		avaiableFields = append(avaiableFields, ColResourceType)
		kvp[ColResourceType] = &filter.OptionalResourceType
	}

	if len(filter.OptionalResourceIds) > 0 {
		avaiableFields = append(avaiableFields, ColObjectID)
		kvp[ColObjectID] = &filter.OptionalResourceIds[0]
	}

	if filter.OptionalResourceRelation != "" {
		avaiableFields = append(avaiableFields, ColRelation)
		kvp[ColRelation] = &filter.OptionalResourceRelation
	}

	if len(filter.OptionalSubjectsSelectors) > 0 {
		hasST := false
		hasSOid := false
		hasRel := false
		for _, ss := range filter.OptionalSubjectsSelectors {
			if ss.OptionalSubjectType != "" {
				hasST = true
			}

			if len(ss.OptionalSubjectIds) > 0 {
				hasSOid = true
			}

			if !ss.RelationFilter.IsEmpty() {
				hasRel = true
			}
		}

		if hasST {
			avaiableFields = append(avaiableFields, ColUsersetNamespace)
			kvp[ColUsersetNamespace] = &filter.OptionalSubjectsSelectors[0].OptionalSubjectType
		}

		if hasSOid {
			avaiableFields = append(avaiableFields, ColUsersetObjectID)
			kvp[ColUsersetObjectID] = &filter.OptionalSubjectsSelectors[0].OptionalSubjectIds[0]
		}

		if hasRel {
			avaiableFields = append(avaiableFields, ColUsersetRelation)
			kvp[ColUsersetRelation] = &filter.OptionalSubjectsSelectors[0].RelationFilter.NonEllipsisRelation
		}
	}

	bestScore := math.MaxInt64
	var bestIndex *DynamodbIndex = primary
	for _, i := range Indexes {
		unused, required := i.pk.Check(avaiableFields)
		if len(required) != 0 {
			continue
		}

		unused, _ = i.sk.Check(unused)

		if bestScore > len(unused) {
			bestIndex = i
			bestScore = len(unused)
		}
	}

	return bestIndex, kvp
}

func Query(filter datastore.RelationshipsFilter) sq.SelectBuilder {
	query := sq.StatementBuilder.PlaceholderFormat(sq.Question).Select()

	idx, kvp := QueryHint(filter)

	pk, remaining := idx.pk.BuildFull(kvp)

	remainingKvp := KeyValues{}
	for _, r := range remaining {
		if v, exists := kvp[r]; exists {
			remainingKvp[r] = v
		}
	}
	sk, remaining, _ := idx.sk.BuildPartial(remainingKvp)

	tableName := TableName

	if idx.indexName != "" {
		tableName += "." + idx.indexName
	}

	query = query.From(tableName)

	if pk != "" {
		query = query.Where(sq.Eq{idx.pkKey: pk})
	}

	if sk != "" {
		query = query.Where(sq.Expr(fmt.Sprintf("begins_with(%s, ?)", idx.skKey), sk))
	}

	for _, r := range remaining {
		query = query.Where(sq.Eq{r: kvp[r]})
	}

	return query
}
