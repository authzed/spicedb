package crdb

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/dustin/go-humanize"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
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

		IntegrityKeyID     *string `json:"integrity_key_id"`
		IntegrityHashAsHex *string `json:"integrity_hash"`
		TimestampAsString  *string `json:"timestamp"`

		Metadata map[string]any `json:"metadata"`
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

	if features.Watch.Status != datastore.FeatureSupported {
		errs <- datastore.NewWatchDisabledErr(fmt.Sprintf("%s. See https://spicedb.dev/d/enable-watch-api-crdb", features.Watch.Reason))
		return updates, errs
	}

	// Default: 1s
	windowInterval := 1 * time.Second
	if options.CheckpointInterval > 0 {
		windowInterval = options.CheckpointInterval
	}

	watchBufferWriteTimeout := options.WatchBufferWriteTimeout
	if watchBufferWriteTimeout <= 0 {
		watchBufferWriteTimeout = cds.watchBufferWriteTimeout
	}
	sendChange := sendChangeWithTimeoutFunc(updates, errs, watchBufferWriteTimeout)

	// CRDB does not emit checkpoints for historical, non-real time data. If "afterRevision" is way in the past,
	// that would lead to accumulating ALL the changes until the current time before starting to emit them.
	// To address that, we compute the current checkpoint, and emit all revisions between "afterRevision" and the
	// current checkpoint without waiting for checkpointing from CRDB's side.
	// Run the watch, with the most recent checkpoint known.

	cHandler := newCongestionHandler(windowInterval)
	startRevision := afterRevision.(revisions.HLCRevision)
	endRevision := cHandler.windowFor(startRevision)
	go func() {
		for {
			windowStart := time.Unix(0, startRevision.TimestampNanoSec())
			windowEnd := time.Unix(0, endRevision.TimestampNanoSec())
			log.Info().
				Stringer("windowStart", windowStart).
				Stringer("windowEnd", windowEnd).
				Stringer("windowSize", windowEnd.Sub(windowStart)).
				Stringer("backlogApproximate", time.Now().Sub(windowStart)).
				Msg("processing watch window")
			windowUpdates, err := cds.watch(ctx, afterRevision, endRevision, options)

			var sizeExceededError datastore.MaximumChangesSizeExceededError
			if errors.Is(ctx.Err(), context.Canceled) {
				errs <- datastore.NewWatchCanceledErr()
				break
			} else if err != nil && (pool.IsResettableError(ctx, err) || pool.IsRetryableError(ctx, err)) {
				errs <- datastore.NewWatchTemporaryErr(err)
				break
			} else if errors.As(err, &sizeExceededError) {
				cHandler.congestion(true)
				endRevision = cHandler.windowFor(startRevision)
				log.Warn().Str("maxSize", humanize.Bytes(sizeExceededError.MaxSize())).
					Msg("watch window size exceeded, retrying with smaller window")
				// let's retry the current window with a smaller end revision
				continue
			} else if err != nil {
				errs <- err
				break
			}

			cHandler.congestion(false)
			log.Info().
				Stringer("windowStart", time.Unix(0, startRevision.TimestampNanoSec())).
				Stringer("windowEnd", time.Unix(0, endRevision.TimestampNanoSec())).
				Int("updateCount", len(windowUpdates)).
				Msg("sending watch updates to client")
			for _, change := range windowUpdates {
				change := change
				// this handles sending over the error channel in case of error
				if !sendChange(&change) {
					return
				}
			}

			if options.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints {
				if !sendChange(&datastore.RevisionChanges{
					Revision:     endRevision,
					IsCheckpoint: true,
				}) {
					return
				}
			}

			// move to the next window, increase window size inverse proportionally to the number of changes retrieved
			// from the previous window.
			startRevision = endRevision
			endRevision = cHandler.windowFor(startRevision)
		}

		close(updates)
		close(errs)
	}()

	return updates, errs
}

func newCongestionHandler(interval time.Duration) congestionHandler {
	// to account for a scenario where folks are starting in the past, we set
	// an initial interval of 30 seconds to quickly speed things up if working
	// through a large backlog of changes
	return congestionHandler{
		minInterval:      interval,
		currentInterval:  30 * time.Second,
		congestionFactor: 0.5,
		successFactor:    2,
	}
}

type congestionHandler struct {
	congestionFactor float64
	successFactor    float64
	congested        bool
	minInterval      time.Duration
	currentInterval  time.Duration
}

func (ch *congestionHandler) congestion(isCongested bool) {
	if isCongested {
		ch.congested = true

		congestedIntervalNanos := int64(float64(ch.currentInterval.Nanoseconds()) * ch.congestionFactor)
		congestedInterval := time.Nanosecond * time.Duration(congestedIntervalNanos)
		ch.currentInterval = max(congestedInterval, ch.minInterval)

		return
	}

	ch.congested = false

	successIntervalNanos := int64(float64(ch.currentInterval.Nanoseconds()) * ch.successFactor)
	successInterval := time.Nanosecond * time.Duration(successIntervalNanos)
	ch.currentInterval = successInterval
}

func (ch *congestionHandler) windowFor(initial revisions.HLCRevision) revisions.HLCRevision {
	afterTime := time.Unix(0, initial.TimestampNanoSec())
	if ch.congested {
		return revisions.NewHLCForTime(afterTime.Add(ch.currentInterval))
	}

	now := time.Now()
	newAfterTime := afterTime.Add(ch.currentInterval)
	if newAfterTime.After(now) {
		// we are close to now(), either make the window as large as the difference, or take the minimum allowed
		// interval if bigger than the difference
		ch.currentInterval = max(now.Sub(afterTime), ch.minInterval)
		// cap the window once we approach now() to avoid having to wait very long
		// until the updates are streamed
		return revisions.NewHLCForTime(afterTime.Add(ch.currentInterval))
	}

	// if not beyond now, we may be streaming from the past, so we want to quickly
	// increase the window until we hit congestion
	return revisions.NewHLCForTime(afterTime.Add(ch.currentInterval))
}

func (cds *crdbDatastore) watch(
	ctx context.Context,
	afterRevision datastore.Revision,
	endRevision datastore.Revision,
	opts datastore.WatchOptions,
) ([]datastore.RevisionChanges, error) {
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
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()

	tableNames := make([]string, 0, 4)
	tableNames = append(tableNames, tableTransactionMetadata)
	if opts.Content&datastore.WatchRelationships == datastore.WatchRelationships {
		tableNames = append(tableNames, cds.tableTupleName())
	}
	if opts.Content&datastore.WatchSchema == datastore.WatchSchema {
		tableNames = append(tableNames, tableNamespace)
		tableNames = append(tableNames, tableCaveat)
	}

	if opts.CheckpointInterval < 0 {
		return nil, fmt.Errorf("invalid checkpoint interval given")
	}

	interpolated := fmt.Sprintf("EXPERIMENTAL CHANGEFEED FOR %s WITH updated, cursor = '%s', end_time = '%s', initial_scan = 'no';",
		strings.Join(tableNames, ","), afterRevision, endRevision)

	watchBufferWriteTimeout := opts.WatchBufferWriteTimeout
	if watchBufferWriteTimeout <= 0 {
		watchBufferWriteTimeout = cds.watchBufferWriteTimeout
	}

	changes, err := conn.Query(ctx, interpolated)
	if err != nil {
		return nil, err
	}

	if err := changes.Err(); err != nil {
		return nil, err
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
			return nil, err
		}

		var details changeDetails
		if err := json.Unmarshal(changeJSON, &details); err != nil {
			return nil, err
		}

		if details.Resolved != "" {
			// let the pgx.Rows exit by retuning Next() == false, when we would have reached "end_time"
			continue
		}

		// Otherwise, this a notification of a row change in one of the watched table(s).
		tableName := string(tableNameBytes)

		var pkValues []string
		if err := json.Unmarshal(primaryKeyValuesJSON, &pkValues); err != nil {
			return nil, err
		}

		switch tableName {
		case cds.tableTupleName():
			var caveatName string
			var caveatContext map[string]any
			if details.After != nil && details.After.RelationshipCaveatName != "" {
				caveatName = details.After.RelationshipCaveatName
				caveatContext = details.After.RelationshipCaveatContext
			}
			ctxCaveat, err := common.ContextualizedCaveatFrom(caveatName, caveatContext)
			if err != nil {
				return nil, err
			}

			var integrity *core.RelationshipIntegrity

			if details.After != nil && details.After.IntegrityKeyID != nil && details.After.IntegrityHashAsHex != nil && details.After.TimestampAsString != nil {
				hexString := *details.After.IntegrityHashAsHex
				hashBytes, err := hex.DecodeString(hexString[2:]) // drop the \x
				if err != nil {
					return nil, fmt.Errorf("could not decode hash bytes: %w", err)
				}

				timestampString := *details.After.TimestampAsString
				parsedTime, err := time.Parse("2006-01-02T15:04:05.999999999", timestampString)
				if err != nil {
					return nil, fmt.Errorf("could not parse timestamp: %w", err)
				}

				integrity = &core.RelationshipIntegrity{
					KeyId:    *details.After.IntegrityKeyID,
					Hash:     hashBytes,
					HashedAt: timestamppb.New(parsedTime),
				}
			}

			relationship := tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: pkValues[0],
						ObjectID:   pkValues[1],
						Relation:   pkValues[2],
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: pkValues[3],
						ObjectID:   pkValues[4],
						Relation:   pkValues[5],
					},
				},
				OptionalCaveat:    ctxCaveat,
				OptionalIntegrity: integrity,
			}

			rev, err := revisions.HLCRevisionFromString(details.Updated)
			if err != nil {
				return nil, fmt.Errorf("malformed update timestamp: %w", err)
			}

			if details.After == nil {
				if err := tracked.AddRelationshipChange(ctx, rev, relationship, tuple.UpdateOperationDelete); err != nil {
					return nil, err
				}
			} else {
				if err := tracked.AddRelationshipChange(ctx, rev, relationship, tuple.UpdateOperationTouch); err != nil {
					return nil, err
				}
			}

		case tableNamespace:
			if len(pkValues) != 1 {
				return nil, spiceerrors.MustBugf("expected a single definition name for the primary key in change feed. found: %s", string(primaryKeyValuesJSON))
			}

			definitionName := pkValues[0]

			rev, err := revisions.HLCRevisionFromString(details.Updated)
			if err != nil {
				return nil, fmt.Errorf("malformed update timestamp: %w", err)
			}

			if details.After != nil && details.After.SerializedNamespaceConfig != "" {
				namespaceDef := &core.NamespaceDefinition{}
				defBytes, err := hex.DecodeString(details.After.SerializedNamespaceConfig[2:]) // drop the \x
				if err != nil {
					return nil, fmt.Errorf("could not decode namespace definition: %w", err)
				}

				if err := namespaceDef.UnmarshalVT(defBytes); err != nil {
					return nil, fmt.Errorf("could not unmarshal namespace definition: %w", err)
				}
				err = tracked.AddChangedDefinition(ctx, rev, namespaceDef)
				if err != nil {
					return nil, err
				}
			} else {
				err = tracked.AddDeletedNamespace(ctx, rev, definitionName)
				if err != nil {
					return nil, err
				}
			}

		case tableCaveat:
			if len(pkValues) != 1 {
				return nil, spiceerrors.MustBugf("expected a single definition name for the primary key in change feed. found: %s", string(primaryKeyValuesJSON))
			}

			definitionName := pkValues[0]

			rev, err := revisions.HLCRevisionFromString(details.Updated)
			if err != nil {
				return nil, fmt.Errorf("malformed update timestamp: %w", err)
			}

			if details.After != nil && details.After.SerializedCaveatDefinition != "" {
				caveatDef := &core.CaveatDefinition{}
				defBytes, err := hex.DecodeString(details.After.SerializedCaveatDefinition[2:]) // drop the \x
				if err != nil {
					return nil, fmt.Errorf("could not decode caveat definition: %w", err)
				}

				if err := caveatDef.UnmarshalVT(defBytes); err != nil {
					return nil, fmt.Errorf("could not unmarshal caveat definition: %w", err)
				}

				err = tracked.AddChangedDefinition(ctx, rev, caveatDef)
				if err != nil {
					return nil, err
				}
			} else {
				err = tracked.AddDeletedCaveat(ctx, rev, definitionName)
				if err != nil {
					return nil, err
				}
			}

		case tableTransactionMetadata:
			if details.After != nil {
				rev, err := revisions.HLCRevisionFromString(details.Updated)
				if err != nil {
					return nil, fmt.Errorf("malformed update timestamp: %w", err)
				}

				if err := tracked.SetRevisionMetadata(ctx, rev, details.After.Metadata); err != nil {
					return nil, err
				}
			}

		default:
			return nil, spiceerrors.MustBugf("unexpected table name in changefeed: %s", tableName)
		}
	}

	if err := changes.Err(); err != nil { // FIXME calling changes.Err() 2 times does not return the error on the 2nd call?
		if errors.Is(ctx.Err(), context.Canceled) {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			if err := conn.Close(closeCtx); err != nil {
				return nil, err
			}

			return nil, datastore.NewWatchCanceledErr()
		} else {
			return nil, err
		}
	}

	// if CRDB stops streaming changes over the changefeed, we've reached "end_time"
	filtered, err := tracked.AsRevisionChanges(revisions.HLCKeyLessThanFunc)
	if err != nil {
		return nil, err
	}

	return filtered, nil
}

func sendChangeWithTimeoutFunc(updates chan *datastore.RevisionChanges, errs chan error, watchBufferWriteTimeout time.Duration) func(change *datastore.RevisionChanges) bool {
	return func(change *datastore.RevisionChanges) bool {
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
}
