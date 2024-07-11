package spanner

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	ocprom "contrib.go.opencensus.io/exporter/prometheus"
	sq "github.com/Masterminds/squirrel"
	"github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/datastore/spanner/migrations"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

const (
	Engine = "spanner"

	errUnableToInstantiate = "unable to instantiate spanner client"

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

	errUnableToSerializeFilter = "unable to serialize filter: %w"
	errUnableToWriteCounter    = "unable to write counter: %w"
	errUnableToDeleteCounter   = "unable to delete counter: %w"
	errUnableToUpdateCounter   = "unable to update counter: %w"

	// See https://cloud.google.com/spanner/docs/change-streams#data-retention
	// See https://github.com/authzed/spicedb/issues/1457
	defaultChangeStreamRetention = 24 * time.Hour
)

const tableSizesStatsTable = "spanner_sys.table_sizes_stats_1hour"

var (
	sql    = sq.StatementBuilder.PlaceholderFormat(sq.AtP)
	tracer = otel.Tracer("spicedb/internal/datastore/spanner")

	alreadyExistsRegex = regexp.MustCompile(`^Table relation_tuple: Row {String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\), String\("([^\"]+)"\)} already exists.$`)
)

type spannerDatastore struct {
	*revisions.RemoteClockRevisions
	revisions.CommonDecoder

	watchBufferLength       uint16
	watchBufferWriteTimeout time.Duration

	client   *spanner.Client
	config   spannerOptions
	database string

	cachedEstimatedBytesPerRelationshipLock sync.RWMutex
	cachedEstimatedBytesPerRelationship     uint64

	tableSizesStatsTable string
	filterMaximumIDCount uint16
}

// NewSpannerDatastore returns a datastore backed by cloud spanner
func NewSpannerDatastore(ctx context.Context, database string, opts ...Option) (datastore.Datastore, error) {
	config, err := generateConfig(opts)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, database)
	}

	if config.migrationPhase != "" {
		log.Info().
			Str("phase", config.migrationPhase).
			Msg("spanner configured to use intermediate migration phase")
	}

	if len(config.emulatorHost) > 0 {
		if err := os.Setenv("SPANNER_EMULATOR_HOST", config.emulatorHost); err != nil {
			log.Error().Err(err).Msg("failed to set SPANNER_EMULATOR_HOST env variable")
		}
	}
	if len(os.Getenv("SPANNER_EMULATOR_HOST")) > 0 {
		log.Info().Str("spanner-emulator-host", os.Getenv("SPANNER_EMULATOR_HOST")).Msg("running against spanner emulator")
	}

	// TODO(jschorr): Replace with OpenTelemetry instrumentation once available.
	err = spanner.EnableStatViews() // nolint: staticcheck
	if err != nil {
		return nil, fmt.Errorf("failed to enable spanner session metrics: %w", err)
	}
	err = spanner.EnableGfeLatencyAndHeaderMissingCountViews() // nolint: staticcheck
	if err != nil {
		return nil, fmt.Errorf("failed to enable spanner GFE metrics: %w", err)
	}

	// Register Spanner client gRPC metrics (include round-trip latency, received/sent bytes...)
	if err := view.Register(ocgrpc.DefaultClientViews...); err != nil {
		return nil, fmt.Errorf("failed to enable gRPC metrics for Spanner client: %w", err)
	}

	_, err = ocprom.NewExporter(ocprom.Options{
		Namespace:  "spicedb",
		Registerer: prometheus.DefaultRegisterer,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to enable spanner GFE latency stats: %w", err)
	}

	cfg := spanner.DefaultSessionPoolConfig
	cfg.MinOpened = config.minSessions
	cfg.MaxOpened = config.maxSessions

	var spannerOpts []option.ClientOption
	if config.credentialsJSON != nil {
		spannerOpts = append(spannerOpts, option.WithCredentialsJSON(config.credentialsJSON))
	}

	spannerOpts = append(spannerOpts,
		option.WithCredentialsFile(config.credentialsFilePath),
		option.WithGRPCConnectionPool(max(config.readMaxOpen, config.writeMaxOpen)),
		option.WithGRPCDialOption(
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		),
	)

	client, err := spanner.NewClientWithConfig(
		context.Background(),
		database,
		spanner.ClientConfig{SessionPoolConfig: cfg},
		spannerOpts...,
	)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, database)
	}

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

	ds := &spannerDatastore{
		RemoteClockRevisions: revisions.NewRemoteClockRevisions(
			defaultChangeStreamRetention,
			maxRevisionStaleness,
			config.followerReadDelay,
			config.revisionQuantization,
		),
		CommonDecoder: revisions.CommonDecoder{
			Kind: revisions.Timestamp,
		},
		client:                                  client,
		config:                                  config,
		database:                                database,
		watchBufferWriteTimeout:                 config.watchBufferWriteTimeout,
		watchBufferLength:                       config.watchBufferLength,
		cachedEstimatedBytesPerRelationship:     0,
		cachedEstimatedBytesPerRelationshipLock: sync.RWMutex{},
		tableSizesStatsTable:                    tableSizesStatsTable,
	}
	// Optimized revision and revision checking use a stale read for the
	// current timestamp.
	// TODO: Still investigating whether a stale read can be used for
	//       HeadRevision for FullConsistency queries.
	ds.RemoteClockRevisions.SetNowFunc(ds.staleHeadRevision)

	return ds, nil
}

type traceableRTX struct {
	delegate readTX
}

func (t *traceableRTX) ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error) {
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("spannerAPI", "ReadOnlyTransaction.ReadRow"),
		attribute.String("table", table),
		attribute.String("key", key.String()),
		attribute.StringSlice("columns", columns))

	return t.delegate.ReadRow(ctx, table, key, columns)
}

func (t *traceableRTX) Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator {
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("spannerAPI", "ReadOnlyTransaction.Read"),
		attribute.String("table", table),
		attribute.StringSlice("columns", columns))

	return t.delegate.Read(ctx, table, keys, columns)
}

func (t *traceableRTX) Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator {
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("spannerAPI", "ReadOnlyTransaction.Query"),
		attribute.String("statement", statement.SQL))

	return t.delegate.Query(ctx, statement)
}

func (sd *spannerDatastore) SnapshotReader(revisionRaw datastore.Revision) datastore.Reader {
	r := revisionRaw.(revisions.TimestampRevision)

	txSource := func() readTX {
		return &traceableRTX{delegate: sd.client.Single().WithTimestampBound(spanner.ReadTimestamp(r.Time()))}
	}
	executor := common.QueryExecutor{Executor: queryExecutor(txSource)}
	return spannerReader{executor, txSource, sd.filterMaximumIDCount}
}

func (sd *spannerDatastore) ReadWriteTx(ctx context.Context, fn datastore.TxUserFunc, opts ...options.RWTOptionsOption) (datastore.Revision, error) {
	config := options.NewRWTOptionsWithOptions(opts...)

	ctx, span := tracer.Start(ctx, "ReadWriteTx")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	ts, err := sd.client.ReadWriteTransaction(ctx, func(ctx context.Context, spannerRWT *spanner.ReadWriteTransaction) error {
		txSource := func() readTX {
			return &traceableRTX{delegate: spannerRWT}
		}

		executor := common.QueryExecutor{Executor: queryExecutor(txSource)}
		rwt := spannerReadWriteTXN{
			spannerReader{executor, txSource, sd.filterMaximumIDCount},
			spannerRWT,
		}
		err := func() error {
			innerCtx, innerSpan := tracer.Start(ctx, "TxUserFunc")
			defer innerSpan.End()

			return fn(innerCtx, rwt)
		}()
		if err != nil {
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

	return revisions.NewForTime(ts), nil
}

func (sd *spannerDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	headMigration, err := migrations.SpannerMigrations.HeadRevision()
	if err != nil {
		return datastore.ReadyState{}, fmt.Errorf("invalid head migration found for spanner: %w", err)
	}

	checker := migrations.NewSpannerVersionChecker(sd.client)
	version, err := checker.Version(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}

	// TODO: once phased migration is complete, remove the extra allowed version
	if version == headMigration || version == "register-combined-change-stream" {
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

func (sd *spannerDatastore) Features(_ context.Context) (*datastore.Features, error) {
	return &datastore.Features{Watch: datastore.Feature{Enabled: true}}, nil
}

func (sd *spannerDatastore) Close() error {
	sd.client.Close()
	return nil
}

func statementFromSQL(sql string, args []any) spanner.Statement {
	params := make(map[string]any, len(args))
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
