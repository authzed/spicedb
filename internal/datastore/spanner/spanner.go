package spanner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	ocprom "contrib.go.opencensus.io/exporter/prometheus"
	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	slogzerolog "github.com/samber/slog-zerolog/v2"
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
	"github.com/authzed/spicedb/internal/telemetry/otelconv"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
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
	*common.MigrationValidator

	watchBufferLength            uint16
	watchChangeBufferMaximumSize uint64
	watchBufferWriteTimeout      time.Duration
	watchEnabled                 bool

	client     *spanner.Client
	config     spannerOptions
	database   string
	schema     common.SchemaInformation
	schemaMode dsoptions.SchemaMode

	schemaReaderWriter                  *common.SQLSchemaReaderWriter[any, revisions.TimestampRevision]
	cachedEstimatedBytesPerRelationship atomic.Uint64

	tableSizesStatsTable string
	filterMaximumIDCount uint16
	uniqueID             atomic.Pointer[string]
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

	if config.datastoreMetricsOption == DatastoreMetricsOptionOpenTelemetry {
		log.Info().Msg("enabling OpenTelemetry metrics for Spanner datastore")
		spanner.EnableOpenTelemetryMetrics()
	}

	if config.datastoreMetricsOption == DatastoreMetricsOptionLegacyPrometheus {
		log.Info().Msg("enabling legacy Prometheus metrics for Spanner datastore")
		err = spanner.EnableStatViews() // nolint: staticcheck
		if err != nil {
			return nil, fmt.Errorf("failed to enable spanner session metrics: %w", err)
		}
		err = spanner.EnableGfeLatencyAndHeaderMissingCountViews() // nolint: staticcheck
		if err != nil {
			return nil, fmt.Errorf("failed to enable spanner GFE metrics: %w", err)
		}
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

	slogger := slog.New(slogzerolog.Option{Level: slog.LevelDebug, Logger: &log.Logger}.NewZerologHandler())
	spannerOpts = append(spannerOpts,
		option.WithCredentialsFile(config.credentialsFilePath),
		option.WithGRPCConnectionPool(max(config.readMaxOpen, config.writeMaxOpen)),
		option.WithGRPCDialOption(
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		),
		option.WithLogger(slogger),
	)

	client, err := spanner.NewClientWithConfig(
		context.Background(),
		database,
		spanner.ClientConfig{
			SessionPoolConfig:    cfg,
			DisableNativeMetrics: config.datastoreMetricsOption != DatastoreMetricsOptionNative,
		},
		spannerOpts...,
	)
	if err != nil {
		return nil, common.RedactAndLogSensitiveConnString(ctx, errUnableToInstantiate, err, database)
	}

	maxRevisionStaleness := time.Duration(float64(config.revisionQuantization.Nanoseconds())*
		config.maxRevisionStalenessPercent) * time.Nanosecond

	headMigration, err := migrations.SpannerMigrations.HeadRevision()
	if err != nil {
		return nil, fmt.Errorf("invalid head migration found for spanner: %w", err)
	}

	schema := common.NewSchemaInformationWithOptions(
		common.WithRelationshipTableName(tableRelationship),
		common.WithColNamespace(colNamespace),
		common.WithColObjectID(colObjectID),
		common.WithColRelation(colRelation),
		common.WithColUsersetNamespace(colUsersetNamespace),
		common.WithColUsersetObjectID(colUsersetObjectID),
		common.WithColUsersetRelation(colUsersetRelation),
		common.WithColCaveatName(colCaveatName),
		common.WithColCaveatContext(colCaveatContext),
		common.WithColExpiration(colExpiration),
		common.WithPaginationFilterType(common.ExpandedLogicComparison),
		common.WithPlaceholderFormat(sq.AtP),
		common.WithNowFunction("CURRENT_TIMESTAMP"),
		common.WithColumnOptimization(config.columnOptimizationOption),

		// NOTE: this order differs from the default because the index
		// used for sorting by subject (ix_relation_tuple_by_subject) is
		// defined with the userset object ID first.
		common.SetSortBySubjectColumnOrder([]string{
			colUsersetObjectID,
			colUsersetNamespace,
			colUsersetRelation,
			colNamespace,
			colRelation,
			colObjectID,
		}),
	)

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
		MigrationValidator:                  common.NewMigrationValidator(headMigration, config.allowedMigrations),
		client:                              client,
		config:                              config,
		database:                            database,
		watchBufferWriteTimeout:             config.watchBufferWriteTimeout,
		watchChangeBufferMaximumSize:        config.watchChangeBufferMaximumSize,
		watchBufferLength:                   config.watchBufferLength,
		watchEnabled:                        !config.watchDisabled,
		cachedEstimatedBytesPerRelationship: atomic.Uint64{},
		tableSizesStatsTable:                tableSizesStatsTable,
		filterMaximumIDCount:                config.filterMaximumIDCount,
		schema:                              *schema,
		schemaMode:                          config.schemaMode,
	}

	// Initialize schema reader/writer
	ds.schemaReaderWriter, err = common.NewSQLSchemaReaderWriter[any, revisions.TimestampRevision](BaseSchemaChunkerConfig, config.schemaCacheOptions)
	if err != nil {
		return nil, err
	}

	// Optimized revision and revision checking use a stale read for the
	// current timestamp.
	// TODO: Still investigating whether a stale read can be used for
	//       HeadRevision for FullConsistency queries.
	ds.SetNowFunc(ds.staleHeadRevision)

	// Warm the schema cache on startup
	if err := warmSchemaCache(ctx, ds); err != nil {
		log.Warn().Err(err).Msg("failed to warm schema cache on startup")
	}

	return ds, nil
}

type traceableRTX struct {
	delegate readTX
}

func (t *traceableRTX) ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error) {
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String(otelconv.AttrDatastoreSpannerAPI, "ReadOnlyTransaction.ReadRow"),
		attribute.String(otelconv.AttrDatastoreSpannerTable, table),
		attribute.String(otelconv.AttrDatastoreSpannerKey, key.String()),
		attribute.StringSlice(otelconv.AttrDatastoreSpannerColumns, columns))

	return t.delegate.ReadRow(ctx, table, key, columns)
}

func (t *traceableRTX) Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator {
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String(otelconv.AttrDatastoreSpannerAPI, "ReadOnlyTransaction.Read"),
		attribute.String(otelconv.AttrDatastoreSpannerTable, table),
		attribute.StringSlice(otelconv.AttrDatastoreSpannerColumns, columns))

	return t.delegate.Read(ctx, table, keys, columns)
}

func (t *traceableRTX) Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator {
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String(otelconv.AttrDatastoreSpannerAPI, "ReadOnlyTransaction.Query"),
		attribute.String(otelconv.AttrDatastoreSpannerStatement, statement.SQL))

	return t.delegate.Query(ctx, statement)
}

// warmSchemaCache attempts to warm the schema cache by loading the current schema.
// This is called during datastore initialization to avoid cold-start latency on first requests.
func warmSchemaCache(ctx context.Context, ds *spannerDatastore) error {
	// Get the current revision and schema hash
	rev, schemaHash, err := ds.HeadRevision(ctx)
	if err != nil {
		return fmt.Errorf("failed to get head revision: %w", err)
	}

	// If there's no schema hash, there's no schema to warm
	if schemaHash == "" {
		log.Ctx(ctx).Debug().Msg("no schema hash found, skipping cache warming")
		return nil
	}

	// Create a simple executor for schema reading using a single-use read transaction
	txSource := func() readTX {
		return &traceableRTX{delegate: ds.client.Single()}
	}
	executor := &spannerSchemaReadExecutor{txSource: txSource}

	// Load the schema to populate the cache
	_, err = ds.schemaReaderWriter.ReadSchema(ctx, executor, rev, schemaHash)
	if err != nil {
		if errors.Is(err, datastore.ErrSchemaNotFound) {
			// Schema not found is not an error during warming - just means no schema yet
			log.Ctx(ctx).Debug().Msg("no schema found, skipping cache warming")
			return nil
		}
		return fmt.Errorf("failed to read schema: %w", err)
	}

	log.Ctx(ctx).Info().Str("schema_hash", string(schemaHash)).Msg("schema cache warmed successfully")
	return nil
}

func (sd *spannerDatastore) SnapshotReader(revisionRaw datastore.Revision, hash datastore.SchemaHash) datastore.Reader {
	r := revisionRaw.(revisions.TimestampRevision)

	txSource := func() readTX {
		return &traceableRTX{delegate: sd.client.Single().WithTimestampBound(spanner.ReadTimestamp(r.Time()))}
	}
	executor := common.QueryRelationshipsExecutor{Executor: queryExecutor(txSource)}
	return &spannerReader{executor, txSource, sd.filterMaximumIDCount, sd.schema, sd.schemaMode, revisionRaw, string(hash), sd.schemaReaderWriter}
}

func (sd *spannerDatastore) MetricsID() (string, error) {
	return sd.database, nil
}

func (sd *spannerDatastore) readTransactionMetadata(ctx context.Context, transactionTag string) (common.TransactionMetadata, error) {
	row, err := sd.client.Single().ReadRow(ctx, tableTransactionMetadata, spanner.Key{transactionTag}, []string{colMetadata})
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			log.Err(err).Str("key", transactionTag).Send()
			return map[string]any{}, nil
		}

		return nil, err
	}

	var metadataJSON spanner.NullJSON
	if err := row.Columns(&metadataJSON); err != nil {
		log.Err(err).Str("key", transactionTag).Msg("error unmarshaling transaction metadata json")
		return map[string]any{}, nil
	}

	if !metadataJSON.Valid || metadataJSON.Value == nil {
		log.Err(err).Str("key", transactionTag).Msg("error validating transaction metadata json")
		return map[string]any{}, nil
	}

	metadata := metadataJSON.Value.(map[string]any)
	return metadata, nil
}

func (sd *spannerDatastore) ReadWriteTx(ctx context.Context, fn datastore.TxUserFunc, opts ...dsoptions.RWTOptionsOption) (datastore.Revision, error) {
	config := dsoptions.NewRWTOptionsWithOptions(opts...)

	ctx, span := tracer.Start(ctx, "ReadWriteTx")
	defer span.End()

	transactionTag := "sdb-rwt-" + uuid.NewString()
	transactionTag = transactionTag[:36] // there is a column constraint on the length

	ctx, cancel := context.WithCancel(ctx)
	rs, err := sd.client.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, spannerRWT *spanner.ReadWriteTransaction) error {
		txSource := func() readTX {
			return &traceableRTX{delegate: spannerRWT}
		}

		if config.Metadata != nil && len(config.Metadata.GetFields()) > 0 {
			// Insert the metadata into the transaction metadata table.
			mutation := spanner.Insert(tableTransactionMetadata,
				[]string{colTransactionTag, colMetadata},
				[]any{transactionTag, spanner.NullJSON{
					Value: config.Metadata.AsMap(),
					Valid: true,
				}},
			)

			if err := spannerRWT.BufferWrite([]*spanner.Mutation{mutation}); err != nil {
				return fmt.Errorf("unable to write metadata: %w", err)
			}
		}

		executor := common.QueryRelationshipsExecutor{Executor: queryExecutor(txSource)}
		rwt := &spannerReadWriteTXN{
			spannerReader{executor, txSource, sd.filterMaximumIDCount, sd.schema, sd.schemaMode, datastore.NoRevision, string(datastore.NoSchemaHashInTransaction), sd.schemaReaderWriter},
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
	}, spanner.TransactionOptions{TransactionTag: transactionTag})
	if err != nil {
		if cerr := convertToWriteConstraintError(err); cerr != nil {
			return datastore.NoRevision, cerr
		}
		return datastore.NoRevision, err
	}

	return revisions.NewForTime(rs.CommitTs), nil
}

func (sd *spannerDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	checker := migrations.NewSpannerVersionChecker(sd.client)
	version, err := checker.Version(ctx)
	if err != nil {
		return datastore.ReadyState{}, err
	}

	return sd.MigrationReadyState(version), nil
}

func (sd *spannerDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	return sd.OfflineFeatures()
}

func (sd *spannerDatastore) OfflineFeatures() (*datastore.Features, error) {
	watchSupported := datastore.FeatureUnsupported
	if sd.watchEnabled {
		watchSupported = datastore.FeatureSupported
	}

	return &datastore.Features{
		Watch: datastore.Feature{
			Status: watchSupported,
		},
		IntegrityData: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
		ContinuousCheckpointing: datastore.Feature{
			Status: datastore.FeatureSupported,
		},
		WatchEmitsImmediately: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
	}, nil
}

func (sd *spannerDatastore) Close() error {
	sd.client.Close()
	return nil
}

// SchemaHashReaderForTesting returns a test-only interface for reading the schema hash directly from schema_revision table.
func (sd *spannerDatastore) SchemaHashReaderForTesting() interface {
	ReadSchemaHash(ctx context.Context) (string, error)
} {
	return &spannerSchemaHashReaderForTesting{client: sd.client}
}

// SchemaHashWatcherForTesting returns a test-only interface for watching schema hash changes.
func (sd *spannerDatastore) SchemaHashWatcherForTesting() datastore.SingleStoreSchemaHashWatcher {
	return newSpannerSchemaHashWatcher(sd.client)
}

type spannerSchemaHashReaderForTesting struct {
	client *spanner.Client
}

func (r *spannerSchemaHashReaderForTesting) ReadSchemaHash(ctx context.Context) (string, error) {
	watcher := &spannerSchemaHashWatcher{client: r.client}
	return watcher.readSchemaHash(ctx)
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
			return common.NewCreateRelationshipExistsError(&tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: found[1],
						ObjectID:   found[2],
						Relation:   found[3],
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: found[4],
						ObjectID:   found[5],
						Relation:   found[6],
					},
				},
			})
		}

		return common.NewCreateRelationshipExistsError(nil)
	}
	return nil
}
