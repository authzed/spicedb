package spanner

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"
	"go.opentelemetry.io/otel"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/internal/datastore/spanner/migrations"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

const (
	Engine = "spanner"

	errUnableToInstantiate = "unable to instantiate spanner client: %w"

	errRevision = "unable to load revision: %w"

	errUnableToWriteRelationships    = "unable to write relationships: %w"
	errUnableToBulkLoadRelationships = "unable to bulk load relationships: %w"
	errUnableToDeleteRelationships   = "unable to delete relationships: %w"

	errUnableToWriteConfig    = "unable to write namespace config: %w"
	errUnableToReadConfig     = "unable to read namespace config: %w"
	errUnableToDeleteConfig   = "unable to delete namespace config: %w"
	errUnableToListNamespaces = "unable to list namespaces: %w"

	errUnableToReadCaveat   = "unable to read caveat: %w"
	errUnableToWriteCaveat  = "unable to write caveat: %w"
	errUnableToListCaveats  = "unable to list caveats: %w"
	errUnableToDeleteCaveat = "unable to delete caveat: %w"

	// See https://cloud.google.com/spanner/docs/change-streams#data-retention
	// See https://github.com/authzed/spicedb/issues/1457
	defaultChangeStreamRetention = 24 * time.Hour
)

var (
	sql    = sq.StatementBuilder.PlaceholderFormat(sq.AtP)
	tracer = otel.Tracer("spicedb/internal/datastore/spanner")

	alreadyExistsRegex = regexp.MustCompile(`^Table relation_tuple: Row {String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\)} already exists.$`)
)

type spannerDatastore struct {
	*revisions.RemoteClockRevisions
	revision.DecimalDecoder

	client   *spanner.Client
	config   spannerOptions
	database string
}

// NewSpannerDatastore returns a datastore backed by cloud spanner
func NewSpannerDatastore(database string, opts ...Option) (datastore.Datastore, error) {
	config, err := generateConfig(opts)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	if len(config.emulatorHost) > 0 {
		if err := os.Setenv("SPANNER_EMULATOR_HOST", config.emulatorHost); err != nil {
			log.Error().Err(err).Msg("failed to set SPANNER_EMULATOR_HOST env variable")
		}
	}
	if len(os.Getenv("SPANNER_EMULATOR_HOST")) > 0 {
		log.Info().Str("spanner-emulator-host", os.Getenv("SPANNER_EMULATOR_HOST")).Msg("running against spanner emulator")
	}

	client, err := spanner.NewClient(context.Background(), database, option.WithCredentialsFile(config.credentialsFilePath))
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

	ds := spannerDatastore{
		RemoteClockRevisions: revisions.NewRemoteClockRevisions(
			defaultChangeStreamRetention,
			maxRevisionStaleness,
			config.followerReadDelay,
			config.revisionQuantization,
		),
		client:   client,
		config:   config,
		database: database,
	}
	ds.RemoteClockRevisions.SetNowFunc(ds.headRevisionInternal)

	return ds, nil
}

func (sd spannerDatastore) SnapshotReader(revisionRaw datastore.Revision) datastore.Reader {
	r := revisionRaw.(revision.Decimal)

	txSource := func() readTX {
		return sd.client.Single().WithTimestampBound(spanner.ReadTimestamp(timestampFromRevision(r)))
	}
	executor := common.QueryExecutor{
		Executor: queryExecutor(txSource),
	}
	return spannerReader{executor, txSource}
}

func (sd spannerDatastore) ReadWriteTx(
	ctx context.Context,
	fn datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	config := options.NewRWTOptionsWithOptions(opts...)

	ctx, cancel := context.WithCancel(ctx)
	ts, err := sd.client.ReadWriteTransaction(ctx, func(ctx context.Context, spannerRWT *spanner.ReadWriteTransaction) error {
		txSource := func() readTX {
			return spannerRWT
		}

		executor := common.QueryExecutor{
			Executor: queryExecutor(txSource),
		}
		rwt := spannerReadWriteTXN{
			spannerReader{executor, txSource},
			spannerRWT,
			sd.config.disableStats,
		}
		if err := fn(rwt); err != nil {
			if config.DisableRetries {
				defer cancel()
			}
			return err
		}

		return nil
	})
	if err != nil {
		if cerr := convertToWriteConstraintError(err); cerr != nil {
			return datastore.NoRevision, cerr
		}
		return datastore.NoRevision, err
	}

	return revisionFromTimestamp(ts), nil
}

func (sd spannerDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	headMigration, err := migrations.SpannerMigrations.HeadRevision()
	if err != nil {
		return datastore.ReadyState{}, fmt.Errorf("invalid head migration found for spanner: %w", err)
	}

	currentRevision, err := migrations.NewSpannerDriver(sd.client.DatabaseName(), sd.config.credentialsFilePath, sd.config.emulatorHost)
	if err != nil {
		return datastore.ReadyState{}, err
	}
	defer func() {
		if err := currentRevision.Close(ctx); err != nil {
			log.Error().Err(err).Msg("failed to close current revision in Datastore.ReadyState")
		}
	}()

	version, err := currentRevision.Version(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}

	// TODO(jschorr): Remove register-tuple-change-stream once the multi-phase is done.
	if version == headMigration || version == "register-tuple-change-stream" {
		return datastore.ReadyState{IsReady: true}, nil
	}

	return datastore.ReadyState{
		Message: fmt.Sprintf(
			"datastore is not migrated: currently at revision `%s`, but requires `%s`. Please run `spicedb migrate`.",
			version,
			headMigration,
		),
		IsReady: false,
	}, nil
}

func (sd spannerDatastore) Features(_ context.Context) (*datastore.Features, error) {
	return &datastore.Features{Watch: datastore.Feature{Enabled: true}}, nil
}

func (sd spannerDatastore) Close() error {
	sd.client.Close()
	return nil
}

func statementFromSQL(sql string, args []interface{}) spanner.Statement {
	params := make(map[string]interface{}, len(args))
	for index, arg := range args {
		params["p"+strconv.Itoa(index+1)] = arg
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
