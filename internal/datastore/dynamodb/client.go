package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (d DynamodbDatastore) CreateTable(ctx context.Context) (*types.TableDescription, error) {
	var tableDesc *types.TableDescription
	table, err := d.client.CreateTable(ctx, TableDefinition())

	if err != nil {
		fmt.Printf("Couldn't create table %v. Here's why: %v\n", d.tableName, err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(d.client)
		err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(d.tableName)}, 5*time.Minute)
		if err != nil {
			fmt.Printf("Wait for table exists failed. Here's why: %v\n", err)
		}
		tableDesc = table.TableDescription
		fmt.Printf("Ccreating table test")
	}
	return tableDesc, err

}

func (d DynamodbDatastore) TableExists(ctx context.Context) (bool, error) {
	exists := true
	_, err := d.client.DescribeTable(
		ctx, &ddb.DescribeTableInput{TableName: aws.String(d.tableName)},
	)
	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			fmt.Printf("Table %v does not exist.\n", d.tableName)
			err = nil
		} else {
			fmt.Printf("Couldn't determine existence of table %v. Here's why: %v\n", d.tableName, err)
		}
		exists = false
	}
	return exists, err
}

func (d DynamodbDatastore) PutItem(ctx context.Context, item map[string]types.AttributeValue) error {

	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      item,
	})
	if err != nil {
		fmt.Printf("Couldn't add item to table. Here's why: %v\n", err)
		fmt.Printf("%#v\n", item)
	}
	return err
}

func (d DynamodbDatastore) DeleteItem(ctx context.Context, namespace string) error {
	_, err := d.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(d.tableName),
	})
	if err != nil {
		fmt.Printf("Couldn't delete %v from the table. Here's why: %v\n", namespace, err)
	}
	return err
}

func (d DynamodbDatastore) QueryItem(ctx context.Context, expr *expression.Expression, options ...QueryOption) ([]interface{}, error) {
	var err error
	var response *dynamodb.QueryOutput
	var items []interface{}

	opt, _ := generateQueryOpt(options)

	if err != nil {
		return nil, fmt.Errorf("Couldn't build expression for query. Here's why: %v\n", err)
	}

	queryInputReq := &dynamodb.QueryInput{
		TableName:                 aws.String(d.tableName),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
	}

	if opt.indexName != "" {
		queryInputReq.IndexName = aws.String(opt.indexName)
	}

	if opt.consistentReads {
		queryInputReq.ConsistentRead = aws.Bool(opt.consistentReads)
	}

	if opt.scanIndexForward {
		queryInputReq.ScanIndexForward = aws.Bool(opt.scanIndexForward)
	}

	if opt.limit != 0 {
		queryInputReq.Limit = aws.Int32(opt.limit)
	}

	queryPaginator := dynamodb.NewQueryPaginator(d.client, queryInputReq)
	for queryPaginator.HasMorePages() {
		response, err = queryPaginator.NextPage(ctx)
		if err != nil {
			fmt.Printf("Couldn't query for items released in %v. Here's why: %v\n", "namespace", err)
			break
		}

		var itemPage []interface{}
		err = attributevalue.UnmarshalListOfMaps(response.Items, &itemPage)
		if err != nil {
			fmt.Printf("Couldn't unmarshal query response. Here's why: %v\n", err)
			break
		}

		items = append(items, itemPage...)
	}

	return items, err
}

func TableDefinition() *ddb.CreateTableInput {
	return &ddb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(PK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(SK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(LSI1SK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(LSI2SK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(GSI1PK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(GSI1SK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(GSI2PK),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(GSI2SK),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(PK),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(SK),
				KeyType:       types.KeyTypeRange,
			},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(IDX_LSI1),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(PK),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(LSI1SK),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
			{
				IndexName: aws.String(IDX_LSI2),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(PK),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(LSI2SK),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(IDX_GSI1),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(GSI1PK),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(GSI1SK),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
			{
				IndexName: aws.String(IDX_GSI2),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(GSI2PK),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(GSI2SK),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		TableName:   aws.String(TableName),
		BillingMode: types.BillingModePayPerRequest,
	}
}
