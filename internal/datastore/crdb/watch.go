package crdb

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	queryChangefeed       = "EXPERIMENTAL CHANGEFEED FOR %s WITH updated, cursor = '%s', resolved = '1s', min_checkpoint_frequency = '0';"
	queryChangefeedPreV22 = "EXPERIMENTAL CHANGEFEED FOR %s WITH updated, cursor = '%s', resolved = '1s';"
)

type changeDetails struct {
	Resolved string
	Updated  string
	After    *struct {
		CaveatContext map[string]any `json:"caveat_context"`
		CaveatName    string         `json:"caveat_name"`
	}
}

var retryHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "datastore",
	Name:      "crdb_schema_watch_retries",
	Help:      "schema watch retry distribution",
	Buckets:   []float64{0, 1, 2, 5, 10, 20, 50},
})

func init() {
	prometheus.MustRegister(retryHistogram)
}

func (cds *crdbDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, cds.watchBufferLength)
	errs := make(chan error, 1)

	features, err := cds.Features(ctx)
	if err != nil {
		errs <- err
		return updates, errs
	}

	if !features.Watch.Enabled {
		errs <- datastore.NewWatchDisabledErr(fmt.Sprintf("%s. See https://spicedb.dev/d/enable-watch-api-crdb", features.Watch.Reason))
		return updates, errs
	}

	// get non-pooled connection for watch
	// "applications should explicitly create dedicated connections to consume
	// changefeed data, instead of using a connection pool as most client
	// drivers do by default."
	// see: https://www.cockroachlabs.com/docs/v22.2/changefeed-for#considerations
	conn, err := pgx.Connect(ctx, cds.dburl)
	if err != nil {
		errs <- err
		return updates, errs
	}

	interpolated := fmt.Sprintf(cds.beginChangefeedQuery, tableTuple, afterRevision)

	go func() {
		defer close(updates)
		defer close(errs)
		defer func() { _ = conn.Close(ctx) }()

		pendingChanges := make(map[string]*datastore.RevisionChanges)

		changes, err := conn.Query(ctx, interpolated)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				errs <- datastore.NewWatchCanceledErr()
			} else {
				errs <- err
			}
			return
		}

		// We call Close async here because it can be slow and blocks closing the channels. There is
		// no return value so we're not really losing anything.
		defer func() { go changes.Close() }()

		for changes.Next() {
			var unused interface{}
			var changeJSON []byte
			var primaryKeyValuesJSON []byte

			if err := changes.Scan(&unused, &primaryKeyValuesJSON, &changeJSON); err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					errs <- datastore.NewWatchCanceledErr()
				} else {
					errs <- err
				}
				return
			}

			var details changeDetails
			if err := json.Unmarshal(changeJSON, &details); err != nil {
				errs <- err
				return
			}

			if details.Resolved != "" {
				// This entry indicates that we are ready to potentially emit some changes
				resolved, err := cds.RevisionFromString(details.Resolved)
				if err != nil {
					errs <- err
					return
				}

				var toEmit []*datastore.RevisionChanges
				for ts, values := range pendingChanges {
					if resolved.GreaterThan(values.Revision) {
						delete(pendingChanges, ts)

						toEmit = append(toEmit, values)
					}
				}

				sort.Slice(toEmit, func(i, j int) bool {
					return toEmit[i].Revision.LessThan(toEmit[j].Revision)
				})

				for _, change := range toEmit {
					select {
					case updates <- change:
					default:
						errs <- datastore.NewWatchDisconnectedErr()
						return
					}
				}

				continue
			}

			var pkValues [6]string
			if err := json.Unmarshal(primaryKeyValuesJSON, &pkValues); err != nil {
				errs <- err
				return
			}

			revision, err := cds.RevisionFromString(details.Updated)
			if err != nil {
				errs <- fmt.Errorf("malformed update timestamp: %w", err)
				return
			}

			var caveatName string
			var caveatContext map[string]any
			if details.After != nil && details.After.CaveatName != "" {
				caveatName = details.After.CaveatName
				caveatContext = details.After.CaveatContext
			}
			ctxCaveat, err := common.ContextualizedCaveatFrom(caveatName, caveatContext)
			if err != nil {
				errs <- err
				return
			}

			oneChange := &core.RelationTupleUpdate{
				Tuple: &core.RelationTuple{
					ResourceAndRelation: &core.ObjectAndRelation{
						Namespace: pkValues[0],
						ObjectId:  pkValues[1],
						Relation:  pkValues[2],
					},
					Subject: &core.ObjectAndRelation{
						Namespace: pkValues[3],
						ObjectId:  pkValues[4],
						Relation:  pkValues[5],
					},
					Caveat: ctxCaveat,
				},
			}

			if details.After == nil {
				oneChange.Operation = core.RelationTupleUpdate_DELETE
			} else {
				oneChange.Operation = core.RelationTupleUpdate_TOUCH
			}

			pending, ok := pendingChanges[details.Updated]
			if !ok {
				pending = &datastore.RevisionChanges{
					Revision: revision,
				}
				pendingChanges[details.Updated] = pending
			}
			pending.Changes = append(pending.Changes, oneChange)
		}

		if changes.Err() != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				if err := conn.Close(closeCtx); err != nil {
					errs <- err
				}
				errs <- datastore.NewWatchCanceledErr()
			} else {
				errs <- err
			}
			return
		}
	}()
	return updates, errs
}

type schemaChangeDetails struct {
	Resolved string
	Updated  string
	After    *struct {
		Namespace                 string `json:"namespace"`
		SerializedNamespaceConfig string `json:"serialized_config"`

		CaveatName                 string `json:"name"`
		SerializedCaveatDefinition string `json:"definition"`
	}
}

const maxSchemaWatchRetryCount = 15

func (cds *crdbDatastore) WatchSchema(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.SchemaState, <-chan error) {
	updates := make(chan *datastore.SchemaState, cds.watchBufferLength)
	errs := make(chan error, 1)

	features, err := cds.Features(ctx)
	if err != nil {
		errs <- err
		return updates, errs
	}

	if !features.Watch.Enabled {
		errs <- datastore.NewWatchDisabledErr(fmt.Sprintf("%s. See https://spicedb.dev/d/enable-watch-api-crdb", features.Watch.Reason))
		return updates, errs
	}

	lastRevision := afterRevision
	retryCount := -1
	go (func() {
		running := true
		for running {
			retryCount++
			retryHistogram.Observe(float64(retryCount))
			if retryCount >= maxSchemaWatchRetryCount {
				errs <- fmt.Errorf("schema watch terminated after %d retries", retryCount)
				return
			}

			log.Debug().Str("revision", lastRevision.String()).Int("retry-count", retryCount).Msg("starting schema watch connection")

			cds.watchSchemaWithoutRetry(ctx, lastRevision, func(update *datastore.SchemaState, err error) {
				if err != nil {
					if pool.IsResettableError(ctx, err) || pool.IsRetryableError(ctx, err) {
						running = true

						// Sleep a bit for retrying.
						pgxcommon.SleepOnErr(ctx, err, uint8(retryCount))
						return
					}

					running = false
					errs <- err
					return
				}

				if update.Revision.GreaterThan(lastRevision) {
					lastRevision = update.Revision
				}

				updates <- update
				retryCount = -1
			})
		}
	})()

	return updates, errs
}

func (cds *crdbDatastore) watchSchemaWithoutRetry(ctx context.Context, afterRevision datastore.Revision, processUpdate func(update *datastore.SchemaState, err error)) {
	// get non-pooled connection for watch
	// "applications should explicitly create dedicated connections to consume
	// changefeed data, instead of using a connection pool as most client
	// drivers do by default."
	// see: https://www.cockroachlabs.com/docs/v22.2/changefeed-for#considerations
	conn, err := pgx.Connect(ctx, cds.dburl)
	if err != nil {
		processUpdate(nil, err)
		return
	}
	defer func() { _ = conn.Close(ctx) }()

	interpolated := fmt.Sprintf(cds.beginChangefeedQuery, tableNamespace+","+tableCaveat, afterRevision)

	changes, err := conn.Query(ctx, interpolated)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			processUpdate(nil, datastore.NewWatchCanceledErr())
			return
		}

		processUpdate(nil, err)
		return
	}

	// We call Close async here because it can be slow and blocks closing the channels. There is
	// no return value so we're not really losing anything.
	defer func() { go changes.Close() }()

	for changes.Next() {
		var tableNameBytes []byte
		var changeJSON []byte
		var primaryKeyValuesJSON []byte

		if err := changes.Scan(&tableNameBytes, &primaryKeyValuesJSON, &changeJSON); err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				processUpdate(nil, datastore.NewWatchCanceledErr())
				return
			}

			processUpdate(nil, err)
			return
		}

		var details schemaChangeDetails
		if err := json.Unmarshal(changeJSON, &details); err != nil {
			processUpdate(nil, err)
			return
		}

		if details.Resolved != "" {
			revision, err := cds.RevisionFromString(details.Resolved)
			if err != nil {
				processUpdate(nil, fmt.Errorf("malformed resolved timestamp: %w", err))
				return
			}

			processUpdate(&datastore.SchemaState{
				Revision:     revision,
				IsCheckpoint: true,
			}, nil)
			continue
		}

		tableName := string(tableNameBytes)

		var pkValues []string
		if err := json.Unmarshal(primaryKeyValuesJSON, &pkValues); err != nil {
			processUpdate(nil, err)
			return
		}

		if len(pkValues) != 1 {
			processUpdate(nil, spiceerrors.MustBugf("expected a single definition name for the primary key in schema change feed. found: %s", string(primaryKeyValuesJSON)))
			return
		}

		definitionName := pkValues[0]

		deletedNamespaces := make([]string, 0, 1)
		deletedCaveats := make([]string, 0, 1)
		changedDefinitions := make([]datastore.SchemaDefinition, 0, 1)

		switch tableName {
		case tableNamespace:
			if details.After != nil && details.After.SerializedNamespaceConfig != "" {
				namespaceDef := &core.NamespaceDefinition{}
				defBytes, err := hex.DecodeString(details.After.SerializedNamespaceConfig[2:]) // drop the \x
				if err != nil {
					processUpdate(nil, fmt.Errorf("could not decode namespace definition: %w", err))
					return
				}

				if err := namespaceDef.UnmarshalVT(defBytes); err != nil {
					processUpdate(nil, fmt.Errorf("could not unmarshal namespace definition: %w", err))
					return
				}
				changedDefinitions = append(changedDefinitions, namespaceDef)
			} else {
				deletedNamespaces = append(deletedNamespaces, definitionName)
			}

		case tableCaveat:
			if details.After != nil && details.After.SerializedCaveatDefinition != "" {
				caveatDef := &core.CaveatDefinition{}
				defBytes, err := hex.DecodeString(details.After.SerializedCaveatDefinition[2:]) // drop the \x
				if err != nil {
					processUpdate(nil, fmt.Errorf("could not decode caveat definition: %w", err))
					return
				}

				if err := caveatDef.UnmarshalVT(defBytes); err != nil {
					processUpdate(nil, fmt.Errorf("could not unmarshal caveat definition: %w", err))
					return
				}
				changedDefinitions = append(changedDefinitions, caveatDef)
			} else {
				deletedCaveats = append(deletedCaveats, definitionName)
			}

		default:
			processUpdate(nil, spiceerrors.MustBugf("unknown table in schema change feed: %s", tableName))
			return
		}

		revision, err := cds.RevisionFromString(details.Updated)
		if err != nil {
			processUpdate(nil, fmt.Errorf("malformed resolved timestamp: %w", err))
			return
		}

		processUpdate(&datastore.SchemaState{
			Revision:           revision,
			ChangedDefinitions: changedDefinitions,
			DeletedNamespaces:  deletedNamespaces,
			DeletedCaveats:     deletedCaveats,
			IsCheckpoint:       false,
		}, nil)
	}

	if changes.Err() != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			if err := conn.Close(closeCtx); err != nil {
				processUpdate(nil, err)
				return
			}
			processUpdate(nil, datastore.NewWatchCanceledErr())
		} else {
			processUpdate(nil, changes.Err())
		}
		return
	}
}
