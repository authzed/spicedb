package crdb

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

	windowCompleted := make(chan error)
	endRevision := increaseWithDuration(afterRevision, windowInterval)
	go func() {
		for {
			go func() {
				fmt.Printf("processing window: [%v -> %v]\n", afterRevision, endRevision)
				windowCompleted <- cds.watch(ctx, afterRevision, endRevision, options, updates, errs)
			}()

			err := <-windowCompleted
			if err != nil {
				// no need to bubble up the error, it would have been already sent over the channel
				break
			}

			if options.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints {
				if !sendChange(&datastore.RevisionChanges{
					Revision:     endRevision,
					IsCheckpoint: true,
				}) {
					return
				}
			}

			// move to the next window

			afterRevision = endRevision
			endRevision = increaseWithDuration(afterRevision, windowInterval)
		}

		close(updates)
		close(errs)
	}()

	return updates, errs
}

func increaseWithDuration(revision datastore.Revision, windowInterval time.Duration) revisions.HLCRevision {
	hclRev := revision.(revisions.HLCRevision)
	afterTime := time.Unix(0, hclRev.TimestampNanoSec())
	newRev := revisions.NewHLCForTime(afterTime.Add(windowInterval))
	return newRev
}

func (cds *crdbDatastore) watch(
	ctx context.Context,
	afterRevision datastore.Revision,
	endRevision datastore.Revision,
	opts datastore.WatchOptions,
	updates chan *datastore.RevisionChanges,
	errs chan error,
) error {
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
		return err
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
		err := fmt.Errorf("invalid checkpoint interval given")
		errs <- err
		return err
	}

	interpolated := fmt.Sprintf("EXPERIMENTAL CHANGEFEED FOR %s WITH updated, cursor = '%s', end_time = '%s';",
		strings.Join(tableNames, ","), afterRevision, endRevision)

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

	sendChange := sendChangeWithTimeoutFunc(updates, errs, watchBufferWriteTimeout)
	changes, err := conn.Query(ctx, interpolated)
	if err != nil {
		sendError(err)
		return err
	}

	if err := changes.Err(); err != nil {
		sendError(err)
		return err
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
			return err
		}

		var details changeDetails
		if err := json.Unmarshal(changeJSON, &details); err != nil {
			sendError(err)
			return err
		}

		if details.Resolved != "" {
			// let the pgx.Rows exit by retuning Next() == false, when we would have reached "end_time"
			continue
		}

		// Otherwise, this a notification of a row change in one of the watched table(s).
		tableName := string(tableNameBytes)

		var pkValues []string
		if err := json.Unmarshal(primaryKeyValuesJSON, &pkValues); err != nil {
			sendError(err)
			return err
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
				sendError(err)
				return err
			}

			var integrity *core.RelationshipIntegrity

			if details.After != nil && details.After.IntegrityKeyID != nil && details.After.IntegrityHashAsHex != nil && details.After.TimestampAsString != nil {
				hexString := *details.After.IntegrityHashAsHex
				hashBytes, err := hex.DecodeString(hexString[2:]) // drop the \x
				if err != nil {
					newErr := fmt.Errorf("could not decode hash bytes: %w", err)
					sendError(newErr)
					return newErr
				}

				timestampString := *details.After.TimestampAsString
				parsedTime, err := time.Parse("2006-01-02T15:04:05.999999999", timestampString)
				if err != nil {
					newErr := fmt.Errorf("could not parse timestamp: %w", err)
					sendError(newErr)
					return newErr
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
				newErr := fmt.Errorf("malformed update timestamp: %w", err)
				sendError(newErr)
				return newErr
			}

			if details.After == nil {
				if err := tracked.AddRelationshipChange(ctx, rev, relationship, tuple.UpdateOperationDelete); err != nil {
					sendError(err)
					return err
				}
			} else {
				if err := tracked.AddRelationshipChange(ctx, rev, relationship, tuple.UpdateOperationTouch); err != nil {
					sendError(err)
					return err
				}
			}

		case tableNamespace:
			if len(pkValues) != 1 {
				newErr := spiceerrors.MustBugf("expected a single definition name for the primary key in change feed. found: %s", string(primaryKeyValuesJSON))
				sendError(newErr)
				return newErr
			}

			definitionName := pkValues[0]

			rev, err := revisions.HLCRevisionFromString(details.Updated)
			if err != nil {
				newErr := fmt.Errorf("malformed update timestamp: %w", err)
				sendError(newErr)
				return newErr
			}

			if details.After != nil && details.After.SerializedNamespaceConfig != "" {
				namespaceDef := &core.NamespaceDefinition{}
				defBytes, err := hex.DecodeString(details.After.SerializedNamespaceConfig[2:]) // drop the \x
				if err != nil {
					newErr := fmt.Errorf("could not decode namespace definition: %w", err)
					sendError(newErr)
					return newErr
				}

				if err := namespaceDef.UnmarshalVT(defBytes); err != nil {
					newErr := fmt.Errorf("could not unmarshal namespace definition: %w", err)
					sendError(newErr)
					return newErr
				}
				err = tracked.AddChangedDefinition(ctx, rev, namespaceDef)
				if err != nil {
					sendError(err)
					return err
				}
			} else {
				err = tracked.AddDeletedNamespace(ctx, rev, definitionName)
				if err != nil {
					sendError(err)
					return err
				}
			}

		case tableCaveat:
			if len(pkValues) != 1 {
				newErr := spiceerrors.MustBugf("expected a single definition name for the primary key in change feed. found: %s", string(primaryKeyValuesJSON))
				sendError(newErr)
				return newErr
			}

			definitionName := pkValues[0]

			rev, err := revisions.HLCRevisionFromString(details.Updated)
			if err != nil {
				newErr := fmt.Errorf("malformed update timestamp: %w", err)
				sendError(newErr)
				return newErr
			}

			if details.After != nil && details.After.SerializedCaveatDefinition != "" {
				caveatDef := &core.CaveatDefinition{}
				defBytes, err := hex.DecodeString(details.After.SerializedCaveatDefinition[2:]) // drop the \x
				if err != nil {
					newErr := fmt.Errorf("could not decode caveat definition: %w", err)
					sendError(newErr)
					return newErr
				}

				if err := caveatDef.UnmarshalVT(defBytes); err != nil {
					newErr := fmt.Errorf("could not unmarshal caveat definition: %w", err)
					sendError(newErr)
					return newErr
				}

				err = tracked.AddChangedDefinition(ctx, rev, caveatDef)
				if err != nil {
					sendError(err)
					return err
				}
			} else {
				err = tracked.AddDeletedCaveat(ctx, rev, definitionName)
				if err != nil {
					sendError(err)
					return err
				}
			}

		case tableTransactionMetadata:
			if details.After != nil {
				rev, err := revisions.HLCRevisionFromString(details.Updated)
				if err != nil {
					newErr := fmt.Errorf("malformed update timestamp: %w", err)
					sendError(newErr)
					return newErr
				}

				if err := tracked.SetRevisionMetadata(ctx, rev, details.After.Metadata); err != nil {
					sendError(err)
					return err
				}
			}

		default:
			newErr := spiceerrors.MustBugf("unexpected table name in changefeed: %s", tableName)
			sendError(newErr)
			return newErr
		}

		// If we have a most recent checkpoint, we check if the updated revision is less than it. If it is,
		// we can emit all changes that are less than the most recent checkpoint, as CRDB has guarenteed that
		// no other updates will come in before that revision.

		filtered, err := tracked.AsRevisionChanges(revisions.HLCKeyLessThanFunc)
		if err != nil {
			sendError(err)
			return err
		}

		for _, revChange := range filtered {
			spiceerrors.DebugAssert(func() bool {
				return !revChange.IsCheckpoint
			}, "expected non-checkpoint update")

			if !sendChange(&revChange) {
				return nil
			}
		}
	}

	if err := changes.Err(); err != nil { // FIXME calling changes.Err() 2 times does not return the error on the 2nd call?
		if errors.Is(ctx.Err(), context.Canceled) {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			if err := conn.Close(closeCtx); err != nil {
				errs <- err
				return err
			}
			canceled := datastore.NewWatchCanceledErr()
			errs <- canceled
			return canceled
		} else {
			errs <- err
			return err
		}
	}

	return nil
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
