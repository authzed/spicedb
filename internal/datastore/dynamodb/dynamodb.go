package dynamodb

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"

	"github.com/aws/aws-sdk-go-v2/config"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

const (
	Engine    = "dynamodb"
	TableName = "spicedb"
)

func NewDynamodbDatastore(ctx context.Context, options ...Option) datastore.Datastore {
	cfg, err := generateConfig(options)
	if err != nil {
		fmt.Println("error")
	}

	fmt.Printf("aws url %s", cfg.awsUrl)

	awscfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion("us-west-2"),
		// config.WithSharedConfigProfile(cfg.awsSharedProfile),
		config.WithBaseEndpoint(cfg.awsUrl),
	)

	awscfg.BaseEndpoint = &cfg.awsUrl

	client := ddb.NewFromConfig(awscfg)

	d := DynamodbDatastore{
		DynamoDbClient: client,
		TableName:      TableName,
	}

	exist, err := d.TableExists(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	if !exist {
		_, err = d.CreateTable(ctx)
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	return d
}

type DynamodbDatastore struct {
	DynamoDbClient *ddb.Client
	TableName      string
}

/*
each table, has its own composite pattern for
	pk, sk, lsi1sk, lsi2_zedtoken,
	gsi1pk, gsi1sk, gsi2pk, gsi2sk

	here most critical, composite values formation
	this should be able to do parital filling and return remaining

	remaining will be added in filter


	insert, update, query, delete

	type dynamodb_attr struct {
		pk: compositeFiledAsPattern
		sk: compositeFiledAsPattern
		lsi1sk: compositeFiledAsPattern
		lsi2_zedtoken: compositeFiledAsPattern

		gsi1pk: compositeFiledAsPattern
		gsi1sk: compositeFiledAsPattern
		gsi2pk: compositeFiledAsPattern
		gsi2sk: compositeFiledAsPattern
		fields
	}

*/

// CheckRevision implements datastore.Datastore.
func (d DynamodbDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	panic("unimplemented")
}

// Close implements datastore.Datastore.
func (d DynamodbDatastore) Close() error {
	return nil
}

// Features implements datastore.Datastore.
func (d DynamodbDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	return &datastore.Features{
		Watch: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
		IntegrityData: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
		ContinuousCheckpointing: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
		WatchEmitsImmediately: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
	}, nil
}

// HeadRevision implements datastore.Datastore.
func (d DynamodbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return datastore.NoRevision, nil
}

// MetricsID implements datastore.Datastore.
func (d DynamodbDatastore) MetricsID() (string, error) {
	panic("unimplemented")
}

// OfflineFeatures implements datastore.Datastore.
func (d DynamodbDatastore) OfflineFeatures() (*datastore.Features, error) {
	panic("unimplemented")
}

// OptimizedRevision implements datastore.Datastore.
func (d DynamodbDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	panic("unimplemented")
}

// ReadWriteTx implements datastore.Datastore.
func (d DynamodbDatastore) ReadWriteTx(ctx context.Context, fn datastore.TxUserFunc, options ...options.RWTOptionsOption) (datastore.Revision, error) {
	rwt := NewDynamodbReadWriterTx(d)

	fn(ctx, rwt)

	return datastore.NoRevision, nil
}

// ReadyState implements datastore.Datastore.
func (d DynamodbDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{
		IsReady: true,
	}, nil
}

// RevisionFromString implements datastore.Datastore.
func (d DynamodbDatastore) RevisionFromString(serialized string) (datastore.Revision, error) {
	panic("unimplemented")
}

// SnapshotReader implements datastore.Datastore.
func (d DynamodbDatastore) SnapshotReader(datastore.Revision) datastore.Reader {
	return NewDynamodbReader(d)
}

// Statistics implements datastore.Datastore.
func (d DynamodbDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	return datastore.Stats{}, nil
}

// Watch implements datastore.Datastore.
func (d DynamodbDatastore) Watch(ctx context.Context, afterRevision datastore.Revision, options datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	panic("unimplemented")
}
