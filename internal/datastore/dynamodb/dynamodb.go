package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"go.opentelemetry.io/otel"

	sq "github.com/Masterminds/squirrel"
	datastoreinternal "github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws/awserr"
)

var (
	tracer = otel.Tracer("spicedb/internal/datastore/dynamodb")

	partiql = sq.StatementBuilder.PlaceholderFormat(sq.Question)
)

const (
	Engine    = "dynamodb"
	TableName = "spicedb"

	liveDeletedTxnID = uint64(9223372036854775807)
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

func NewDynamodbDatastore(ctx context.Context, options ...Option) (datastore.Datastore, error) {
	ds, err := newDynamodbDatastore(ctx, options...)
	if err != nil {
		return nil, err
	}
	return datastoreinternal.NewSeparatingContextDatastoreProxy(ds), nil
}

func newDynamodbDatastore(ctx context.Context, options ...Option) (datastore.Datastore, error) {
	config, err := generateConfig(options)
	if err != nil {
		fmt.Println("error")
	}

	fmt.Printf("aws url %s", config.awsUrl)

	awscfg, err := awsconfig.LoadDefaultConfig(
		ctx,
	)

	if config.awsRegion != "" {
		awscfg.Region = config.awsRegion
	}

	if config.awsUrl != "" {
		awscfg.BaseEndpoint = &config.awsUrl
	}

	// if config.awsScreteAccessKey != "" {
	// 	awscfg.Credentials
	// }

	// if credentialsProvider != nil {
	// 	// awscfg.CredentialsProvider = credentialsProvider
	// 	// awscfg.Credentials = credentialsProvider
	// }

	client := ddb.NewFromConfig(awscfg)

	d := &DynamodbDatastore{
		hlc:       NewHybridLogicalClock(),
		client:    client,
		tableName: TableName,
		url:       config.awsUrl,
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

	return d, nil

}

type DynamodbDatastore struct {
	client    *ddb.Client
	tableName string
	url       string
	hlc       *HybridLogicalClock
}

// CheckRevision implements datastore.Datastore.
func (d *DynamodbDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	panic("unimplemented")
}

// Close implements datastore.Datastore.
func (d DynamodbDatastore) Close() error {
	return nil
}

// Features implements datastore.Datastore.
func (d DynamodbDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	return d.OfflineFeatures()
}

// HeadRevision implements datastore.Datastore.
func (d DynamodbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return d.hlc.Now(), nil
}

// MetricsID implements datastore.Datastore.
func (d DynamodbDatastore) MetricsID() (string, error) {
	return common.MetricsIDFromURL(d.url)
}

// OfflineFeatures implements datastore.Datastore.
func (d DynamodbDatastore) OfflineFeatures() (*datastore.Features, error) {
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

// OptimizedRevision implements datastore.Datastore.
func (d DynamodbDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return d.hlc.Now(), nil
}

// ReadWriteTx implements datastore.Datastore.
func (d DynamodbDatastore) ReadWriteTx(ctx context.Context, fn datastore.TxUserFunc, options ...options.RWTOptionsOption) (datastore.Revision, error) {
	xid := d.CreateNewTransaction(ctx).(revisions.HLCRevision)

	rwt := NewDynamodbReadWriterTx(d, xid)

	fn(ctx, rwt)

	return xid, nil
}

// ReadyState implements datastore.Datastore.
func (d DynamodbDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{
		IsReady: true,
	}, nil
}

// RevisionFromString implements datastore.Datastore.
func (d DynamodbDatastore) RevisionFromString(serialized string) (datastore.Revision, error) {
	return revisions.NewHLCForTime(time.Now()), nil
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

func (d DynamodbDatastore) CreateNewTransaction(ctx context.Context) datastore.Revision {
	latestXid, err := d.getLatestTransaction(ctx)
	if err != nil {
		return d.hlc.Now()
	}

	d.hlc.Update(latestXid.(revisions.HLCRevision))

	newXid := d.hlc.Now()

	for {
		kvp := KeyValues{
			ColCreatedXid: aws.String(newXid.String()),
		}

		putRequest := Transaction.GetPutItem(kvp, nil)

		out, err := d.client.PutItem(ctx, &ddb.PutItemInput{
			TableName:           &d.tableName,
			Item:                putRequest.Item,
			ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", SK)),
		})

		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == "ConditionalCheckFailedException" {
					newXid = d.hlc.Now()
					continue
				}
			}

			fmt.Println(err)
		}
		fmt.Printf("output - %#v\n", out)
		break
	}

	return newXid
}

func (d DynamodbDatastore) getLatestTransaction(ctx context.Context) (datastore.Revision, error) {
	key := expression.KeyEqual(expression.Key(PK), expression.Value(Transaction.PK.Build(KeyValues{})))

	expr, err := expression.NewBuilder().
		WithKeyCondition(key).
		Build()
	if err != nil {
		return nil, fmt.Errorf(err.Error())
	}

	res, err := d.QueryItem(ctx, &expr,
		WithLimit(1),
		WithConsistentReads(true),
		WithScanIndexForward(true),
	)

	if err != nil {
		return nil, fmt.Errorf(err.Error())
	}

	if len(res) == 0 {
		return d.hlc.Now(), nil
	}

	item, ok := res[0].(map[string]interface{})
	if !ok {
		return d.hlc.Now(), nil
	}

	if v, ok := item[SK].(string); ok {
		return revisions.HLCRevisionFromString(v)
	}

	return d.hlc.Now(), nil
}
