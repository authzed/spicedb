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

	errUnableToWriteTuples  = "unable to write tuples: %w"
	errUnableToDeleteTuples = "unable to delete tuples: %w"

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
	client        *spanner.Client
	querySplitter common.TupleQuerySplitter
	config        spannerOptions
	stopGC        context.CancelFunc
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

	querySplitter := common.TupleQuerySplitter{
		Executor:         queryExecutor(client),
		UsersetBatchSize: 100,
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
		client:        client,
		querySplitter: querySplitter,
		config:        config,
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
