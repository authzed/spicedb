package spanner

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"time"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/internal/datastore/spanner/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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

	// Spanner requires a much smaller userset batch size than other datastores because of the
	// limitation on the maximum number of function calls.
	// https://cloud.google.com/spanner/quotas
	// We can't share a default or config option with other datastore implementations.
	usersetBatchsize = 100
)

var (
	sql    = sq.StatementBuilder.PlaceholderFormat(sq.AtP)
	tracer = otel.Tracer("spicedb/internal/datastore/spanner")

	alreadyExistsRegex = regexp.MustCompile(`^Table relation_tuple: Row {String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\)} already exists.$`)
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
	if len(os.Getenv("SPANNER_EMULATOR_HOST")) > 0 {
		log.Info().Str("spanner-emulator-host", os.Getenv("SPANNER_EMULATOR_HOST")).Msg("running against spanner emulator")
	}

	config.gcInterval = common.WithJitter(0.2, config.gcInterval)
	log.Info().Float64("factor", 0.2).Msg("gc configured with jitter")

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

	if config.gcInterval > 0*time.Minute && config.gcEnabled {
		ctx, cancel := context.WithCancel(context.Background())
		if err := ds.runGC(ctx); err != nil {
			cancel()
			return nil, fmt.Errorf(errUnableToInstantiate, err)
		}
		ds.stopGC = cancel
	} else {
		log.Warn().Msg("datastore garbage collection disabled")
	}

	return ds, nil
}

func (sd spannerDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	txSource := func() readTX {
		return sd.client.Single().WithTimestampBound(spanner.ReadTimestamp(timestampFromRevision(revision)))
	}
	querySplitter := common.TupleQuerySplitter{
		Executor:         queryExecutor(txSource),
		UsersetBatchSize: usersetBatchsize,
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
			UsersetBatchSize: usersetBatchsize,
		}
		rwt := spannerReadWriteTXN{spannerReader{querySplitter, txSource}, ctx, spannerRWT}
		return fn(ctx, rwt)
	})
	if err != nil {
		if cerr := convertToWriteConstraintError(err); cerr != nil {
			return datastore.NoRevision, cerr
		}
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
	defer currentRevision.Close(ctx)

	version, err := currentRevision.Version(ctx)
	if err != nil {
		return false, err
	}

	return version == headMigration, nil
}

func (sd spannerDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	return &datastore.Features{Watch: datastore.Feature{Enabled: true}}, nil
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

func convertToWriteConstraintError(err error) error {
	if spanner.ErrCode(err) == codes.AlreadyExists {
		description := spanner.ErrDesc(err)
		found := alreadyExistsRegex.FindStringSubmatch(description)
		if found != nil {
			return common.NewCreateRelationshipExistsError(&core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: found[1],
					ObjectId:  found[2],
					Relation:  found[3],
				},
				Subject: &core.ObjectAndRelation{
					Namespace: found[4],
					ObjectId:  found[5],
					Relation:  found[6],
				},
			})
		}

		return common.NewCreateRelationshipExistsError(nil)
	}
	return nil
}
