package spanner

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"google.golang.org/api/option"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/internal/datastore/spanner/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

const (
	Engine = "spanner"

	errUnableToInstantiate = "unable to instantiate spanner client: %w"

	errRevision = "unable to load revision: %w"

	errUnableToWriteRelationships  = "unable to write relationships: %w"
	errUnableToDeleteRelationships = "unable to delete relationships: %w"

	errUnableToWriteConfig    = "unable to write namespace config: %w"
	errUnableToReadConfig     = "unable to read namespace config: %w"
	errUnableToDeleteConfig   = "unable to delete namespace config: %w"
	errUnableToListNamespaces = "unable to list namespaces: %w"
)

var (
	sql    = sq.StatementBuilder.PlaceholderFormat(sq.AtP)
	tracer = otel.Tracer("spicedb/internal/datastore/spanner")
)

type spannerDatastore struct {
	*revisions.RemoteClockRevisions
	client *spanner.Client
	config spannerOptions
	stopGC context.CancelFunc
}

// NewSpannerDatastore returns a datastore backed by cloud spanner
func NewSpannerDatastore(database string, opts ...Option) (datastore.Datastore, error) {
	config, err := generateConfig(opts)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	if len(config.emulatorHost) > 0 {
		os.Setenv("SPANNER_EMULATOR_HOST", config.emulatorHost)
	}

	config.gcInterval = common.WithJitter(0.2, config.gcInterval)
	log.Info().Float64("factor", 0.2).Msg("gc configured with jitter")
	log.Info().Str("spanner-emulator-host", os.Getenv("SPANNER_EMULATOR_HOST")).Msg("spanner emulator")

	client, err := spanner.NewClient(context.Background(), database, option.WithCredentialsFile(config.credentialsFilePath))
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

	ds := spannerDatastore{
		RemoteClockRevisions: revisions.NewRemoteClockRevisions(
			config.gcWindow,
			maxRevisionStaleness,
			config.followerReadDelay,
			config.revisionQuantization,
		),
		client: client,
		config: config,
	}
	ds.RemoteClockRevisions.SetNowFunc(ds.HeadRevision)

	ctx, cancel := context.WithCancel(context.Background())
	if err := ds.runGC(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}
	ds.stopGC = cancel

	return ds, nil
}

func (sd spannerDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	txSource := func() readTX {
		return sd.client.Single().WithTimestampBound(spanner.ReadTimestamp(timestampFromRevision(revision)))
	}
	querySplitter := common.TupleQuerySplitter{
		Executor:         queryExecutor(txSource),
		UsersetBatchSize: 100,
	}

	return spannerReader{querySplitter, txSource}
}

func (sd spannerDatastore) ReadWriteTx(
	ctx context.Context,
	fn datastore.TxUserFunc,
) (datastore.Revision, error) {
	ts, err := sd.client.ReadWriteTransaction(ctx, func(ctx context.Context, spannerRWT *spanner.ReadWriteTransaction) error {
		txSource := func() readTX {
			return spannerRWT
		}

		querySplitter := common.TupleQuerySplitter{
			Executor:         queryExecutor(txSource),
			UsersetBatchSize: 100,
		}
		rwt := spannerReadWriteTXN{spannerReader{querySplitter, txSource}, ctx, spannerRWT}
		return fn(ctx, rwt)
	})
	if err != nil {
		return datastore.NoRevision, err
	}

	return revisionFromTimestamp(ts), nil
}

func (sd spannerDatastore) IsReady(ctx context.Context) (bool, error) {
	headMigration, err := migrations.SpannerMigrations.HeadRevision()
	if err != nil {
		return false, fmt.Errorf("invalid head migration found for postgres: %w", err)
	}

	currentRevision, err := migrations.NewSpannerDriver(sd.client.DatabaseName(), sd.config.credentialsFilePath, sd.config.emulatorHost)
	if err != nil {
		return false, err
	}
	defer currentRevision.Close()

	version, err := currentRevision.Version()
	if err != nil {
		return false, err
	}

	return version == headMigration, nil
}

func (sd spannerDatastore) Close() error {
	sd.stopGC()
	sd.client.Close()
	return nil
}

func statementFromSQL(sql string, args []interface{}) spanner.Statement {
	params := make(map[string]interface{}, len(args))
	for index, arg := range args {
		paramName := fmt.Sprintf("p%d", index+1)
		params[paramName] = arg
	}

	return spanner.Statement{
		SQL:    sql,
		Params: params,
	}
}
