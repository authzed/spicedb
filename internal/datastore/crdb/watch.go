package crdb

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	queryChangefeed       = "EXPERIMENTAL CHANGEFEED FOR %s WITH updated, cursor = '%s', resolved = '%s', min_checkpoint_frequency = '0';"
	queryChangefeedPreV22 = "EXPERIMENTAL CHANGEFEED FOR %s WITH updated, cursor = '%s', resolved = '%s';"
)

var retryHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "datastore",
	Name:      "crdb_watch_retries",
	Help:      "watch retry distribution",
	Buckets:   []float64{0, 1, 2, 5, 10, 20, 50},
})

func init() {
	prometheus.MustRegister(retryHistogram)
}

type changeDetails struct {
	Resolved string
	Updated  string
	After    *struct {
		Namespace                 string `json:"namespace"`
		SerializedNamespaceConfig string `json:"serialized_config"`

		CaveatName                 string `json:"name"`
		SerializedCaveatDefinition string `json:"definition"`

		RelationshipCaveatContext map[string]any `json:"caveat_context"`
		RelationshipCaveatName    string         `json:"caveat_name"`
	}
}

func (cds *crdbDatastore) Watch(ctx context.Context, afterRevision datastore.Revision, options datastore.WatchOptions) (<-chan *datastore.RevisionChanges, <-chan error) {
	watchBufferLength := options.WatchBufferLength
	if watchBufferLength <= 0 {
		watchBufferLength = cds.watchBufferLength
	}

	updates := make(chan *datastore.RevisionChanges, watchBufferLength)
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

	go cds.watch(ctx, afterRevision, options, updates, errs)

	return updates, errs
}

func (cds *crdbDatastore) watch(
	ctx context.Context,
	afterRevision datastore.Revision,
	opts datastore.WatchOptions,
	updates chan *datastore.RevisionChanges,
	errs chan error,
) {
	defer close(updates)
	defer close(errs)

	watchConnectTimeout := opts.WatchConnectTimeout
	if watchConnectTimeout <= 0 {
		watchConnectTimeout = cds.watchConnectTimeout
	}

	// get non-pooled connection for watch
	// "applications should explicitly create dedicated connections to consume
	// changefeed data, instead of using a connection pool as most client
	// drivers do by default."
	// see: https://www.cockroachlabs.com/docs/v22.2/changefeed-for#considerations
	conn, err := pgxcommon.ConnectWithInstrumentationAndTimeout(ctx, cds.dburl, watchConnectTimeout)
	if err != nil {
		errs <- err
		return
	}
	defer func() { _ = conn.Close(ctx) }()

	tableNames := make([]string, 0, 3)
	if opts.Content&datastore.WatchRelationships == datastore.WatchRelationships {
		tableNames = append(tableNames, tableTuple)
	}
	if opts.Content&datastore.WatchSchema == datastore.WatchSchema {
		tableNames = append(tableNames, tableNamespace)
		tableNames = append(tableNames, tableCaveat)
	}

	if len(tableNames) == 0 {
		errs <- fmt.Errorf("at least relationships or schema must be specified")
		return
	}

	if opts.CheckpointInterval < 0 {
		errs <- fmt.Errorf("invalid checkpoint interval given")
		return
	}

	// Default: 1s
	resolvedDuration := 1 * time.Second
	if opts.CheckpointInterval > 0 {
		resolvedDuration = opts.CheckpointInterval
	}

	resolvedDurationString := strconv.FormatInt(resolvedDuration.Milliseconds(), 10) + "ms"
	interpolated := fmt.Sprintf(cds.beginChangefeedQuery, strings.Join(tableNames, ","), afterRevision, resolvedDurationString)

	sendError := func(err error) {
		if errors.Is(ctx.Err(), context.Canceled) {
			errs <- datastore.NewWatchCanceledErr()
			return
		}

		if pool.IsResettableError(ctx, err) || pool.IsRetryableError(ctx, err) {
			errs <- datastore.NewWatchTemporaryErr(err)
			return
		}

		errs <- err
	}

	watchBufferWriteTimeout := opts.WatchBufferWriteTimeout
	if watchBufferWriteTimeout <= 0 {
		watchBufferWriteTimeout = cds.watchBufferWriteTimeout
	}

	sendChange := func(change *datastore.RevisionChanges) bool {
		select {
		case updates <- change:
			return true

		default:
			// If we cannot immediately write, setup the timer and try again.
		}

		timer := time.NewTimer(watchBufferWriteTimeout)
		defer timer.Stop()

		select {
		case updates <- change:
			return true

		case <-timer.C:
			errs <- datastore.NewWatchDisconnectedErr()
			return false
		}
	}

	changes, err := conn.Query(ctx, interpolated)
	if err != nil {
		sendError(err)
		return
	}

	// We call Close async here because it can be slow and blocks closing the channels. There is
	// no return value so we're not really losing anything.
	defer func() { go changes.Close() }()

	tracked := common.NewChanges(revisions.HLCKeyFunc, opts.Content, opts.MaximumBufferedChangesByteSize)

	for changes.Next() {
		var tableNameBytes []byte
		var changeJSON []byte
		var primaryKeyValuesJSON []byte

		// Pull in the table name, the primary key(s) and change information.
		if err := changes.Scan(&tableNameBytes, &primaryKeyValuesJSON, &changeJSON); err != nil {
			sendError(err)
			return
		}

		var details changeDetails
		if err := json.Unmarshal(changeJSON, &details); err != nil {
			sendError(err)
			return
		}

		// Resolved indicates that the specified revision is "complete"; no additional updates can come in before or at it.
		// Therefore, at this point, we issue tracked updates from before that time, and the checkpoint update.
		if details.Resolved != "" {
			rev, err := revisions.HLCRevisionFromString(details.Resolved)
			if err != nil {
				sendError(fmt.Errorf("malformed resolved timestamp: %w", err))
				return
			}

			for _, revChange := range tracked.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev) {
				revChange := revChange
				if !sendChange(&revChange) {
					return
				}
			}

			if opts.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints {
				if !sendChange(&datastore.RevisionChanges{
					Revision:     rev,
					IsCheckpoint: true,
				}) {
					return
				}
			}
			continue
		}

		// Otherwise, this a notification of a row change.
		tableName := string(tableNameBytes)

		var pkValues []string
		if err := json.Unmarshal(primaryKeyValuesJSON, &pkValues); err != nil {
			sendError(err)
			return
		}

		switch tableName {
		case tableTuple:
			var caveatName string
			var caveatContext map[string]any
			if details.After != nil && details.After.RelationshipCaveatName != "" {
				caveatName = details.After.RelationshipCaveatName
				caveatContext = details.After.RelationshipCaveatContext
			}
			ctxCaveat, err := common.ContextualizedCaveatFrom(caveatName, caveatContext)
			if err != nil {
				sendError(err)
				return
			}

			tuple := &core.RelationTuple{
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
			}

			rev, err := revisions.HLCRevisionFromString(details.Updated)
			if err != nil {
				sendError(fmt.Errorf("malformed update timestamp: %w", err))
				return
			}

			if details.After == nil {
				if err := tracked.AddRelationshipChange(ctx, rev, tuple, core.RelationTupleUpdate_DELETE); err != nil {
					sendError(err)
					return
				}
			} else {
				if err := tracked.AddRelationshipChange(ctx, rev, tuple, core.RelationTupleUpdate_TOUCH); err != nil {
					sendError(err)
					return
				}
			}

		case tableNamespace:
			if len(pkValues) != 1 {
				sendError(spiceerrors.MustBugf("expected a single definition name for the primary key in change feed. found: %s", string(primaryKeyValuesJSON)))
				return
			}

			definitionName := pkValues[0]

			rev, err := revisions.HLCRevisionFromString(details.Updated)
			if err != nil {
				sendError(fmt.Errorf("malformed update timestamp: %w", err))
				return
			}

			if details.After != nil && details.After.SerializedNamespaceConfig != "" {
				namespaceDef := &core.NamespaceDefinition{}
				defBytes, err := hex.DecodeString(details.After.SerializedNamespaceConfig[2:]) // drop the \x
				if err != nil {
					sendError(fmt.Errorf("could not decode namespace definition: %w", err))
					return
				}

				if err := namespaceDef.UnmarshalVT(defBytes); err != nil {
					sendError(fmt.Errorf("could not unmarshal namespace definition: %w", err))
					return
				}
				err = tracked.AddChangedDefinition(ctx, rev, namespaceDef)
				if err != nil {
					sendError(err)
					return
				}
			} else {
				err = tracked.AddDeletedNamespace(ctx, rev, definitionName)
				if err != nil {
					sendError(err)
					return
				}
			}

		case tableCaveat:
			if len(pkValues) != 1 {
				sendError(spiceerrors.MustBugf("expected a single definition name for the primary key in change feed. found: %s", string(primaryKeyValuesJSON)))
				return
			}

			definitionName := pkValues[0]

			rev, err := revisions.HLCRevisionFromString(details.Updated)
			if err != nil {
				sendError(fmt.Errorf("malformed update timestamp: %w", err))
				return
			}

			if details.After != nil && details.After.SerializedCaveatDefinition != "" {
				caveatDef := &core.CaveatDefinition{}
				defBytes, err := hex.DecodeString(details.After.SerializedCaveatDefinition[2:]) // drop the \x
				if err != nil {
					sendError(fmt.Errorf("could not decode caveat definition: %w", err))
					return
				}

				if err := caveatDef.UnmarshalVT(defBytes); err != nil {
					sendError(fmt.Errorf("could not unmarshal caveat definition: %w", err))
					return
				}

				err = tracked.AddChangedDefinition(ctx, rev, caveatDef)
				if err != nil {
					sendError(err)
					return
				}
			} else {
				err = tracked.AddDeletedCaveat(ctx, rev, definitionName)
				if err != nil {
					sendError(err)
					return
				}
			}
		}
	}

	if changes.Err() != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			if err := conn.Close(closeCtx); err != nil {
				errs <- err
				return
			}
			errs <- datastore.NewWatchCanceledErr()
		} else {
			errs <- changes.Err()
		}
		return
	}
}
