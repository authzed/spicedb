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

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
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

		RelationshipExpiration *time.Time `json:"expires_at"`

		IntegrityKeyID     *string `json:"integrity_key_id"`
		IntegrityHashAsHex *string `json:"integrity_hash"`
		TimestampAsString  *string `json:"timestamp"`

		Metadata map[string]any `json:"metadata"`
	}
}

func (cds *crdbDatastore) Watch(ctx context.Context, afterRevision datastore.Revision, options datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	watchBufferLength := options.WatchBufferLength
	if watchBufferLength <= 0 {
		watchBufferLength = cds.watchBufferLength
	}

	updates := make(chan datastore.RevisionChanges, watchBufferLength)
	errs := make(chan error, 1)

	features, err := cds.Features(ctx)
	if err != nil {
		close(updates)
		errs <- err
		return updates, errs
	}

	if features.Watch.Status != datastore.FeatureSupported {
		close(updates)
		errs <- datastore.NewWatchDisabledErr(fmt.Sprintf("%s. See https://spicedb.dev/d/enable-watch-api-crdb", features.Watch.Reason))
		return updates, errs
	}

	if options.EmissionStrategy == datastore.EmitImmediatelyStrategy && !(options.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints) {
		close(updates)
		errs <- errors.New("EmitImmediatelyStrategy requires WatchCheckpoints to be set")
		return updates, errs
	}

	go cds.watch(ctx, afterRevision, options, updates, errs)

	return updates, errs
}

func (cds *crdbDatastore) watch(
	ctx context.Context,
	afterRevision datastore.Revision,
	opts datastore.WatchOptions,
	updates chan datastore.RevisionChanges,
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

	tableNames := make([]string, 0, 4)
	tableNames = append(tableNames, schema.TableTransactionMetadata)
	if opts.Content&datastore.WatchRelationships == datastore.WatchRelationships {
		tableNames = append(tableNames, cds.schema.RelationshipTableName)
	}
	if opts.Content&datastore.WatchSchema == datastore.WatchSchema {
		tableNames = append(tableNames, schema.TableNamespace)
		tableNames = append(tableNames, schema.TableCaveat)
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

		if strings.Contains(err.Error(), "must be after replica GC threshold") {
			errs <- datastore.NewInvalidRevisionErr(afterRevision, datastore.RevisionStale)
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

	sendChange := func(change datastore.RevisionChanges) error {
		select {
		case updates <- change:
			return nil

		default:
			// If we cannot immediately write, setup the timer and try again.
		}

		timer := time.NewTimer(watchBufferWriteTimeout)
		defer timer.Stop()

		select {
		case updates <- change:
			return nil

		case <-timer.C:
			return datastore.NewWatchDisconnectedErr()
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

	cds.processChanges(ctx, changes, sendError, sendChange, opts, opts.EmissionStrategy == datastore.EmitImmediatelyStrategy)
}

// changeTracker takes care of accumulating received from CockroachDB until a checkpoint is emitted
type changeTracker[R datastore.Revision, K comparable] interface {
	FilterAndRemoveRevisionChanges(lessThanFunc func(lhs, rhs K) bool, boundRev R) ([]datastore.RevisionChanges, error)
	AddRelationshipChange(ctx context.Context, rev R, rel tuple.Relationship, op tuple.UpdateOperation) error
	AddChangedDefinition(ctx context.Context, rev R, def datastore.SchemaDefinition) error
	AddDeletedNamespace(ctx context.Context, rev R, namespaceName string) error
	AddDeletedCaveat(ctx context.Context, rev R, caveatName string) error
	SetRevisionMetadata(ctx context.Context, rev R, metadata map[string]any) error
}

// streamingChangeProvider is a changeTracker that streams changes as they are processed. Instead of accumulating
// changes in memory before a checkpoint is reached, it leaves the responsibility of accumulating, deduplicating,
// normalizing changes, and waiting for a checkpoints to the caller.
//
// It's used when WatchOptions.EmissionStrategy is set to EmitImmediatelyStrategy.
type streamingChangeProvider struct {
	content    datastore.WatchContent
	sendChange sendChangeFunc
	sendError  sendErrorFunc
}

func (s streamingChangeProvider) FilterAndRemoveRevisionChanges(_ func(lhs revisions.HLCRevision, rhs revisions.HLCRevision) bool, _ revisions.HLCRevision) ([]datastore.RevisionChanges, error) {
	// we do not accumulate in this implementation, but stream right away
	return nil, nil
}

func (s streamingChangeProvider) AddRelationshipChange(ctx context.Context, rev revisions.HLCRevision, rel tuple.Relationship, op tuple.UpdateOperation) error {
	if s.content&datastore.WatchRelationships != datastore.WatchRelationships {
		return nil
	}

	changes := datastore.RevisionChanges{
		Revision: rev,
	}
	switch op {
	case tuple.UpdateOperationCreate:
		changes.RelationshipChanges = append(changes.RelationshipChanges, tuple.Create(rel))
	case tuple.UpdateOperationTouch:
		changes.RelationshipChanges = append(changes.RelationshipChanges, tuple.Touch(rel))
	case tuple.UpdateOperationDelete:
		changes.RelationshipChanges = append(changes.RelationshipChanges, tuple.Delete(rel))
	default:
		return spiceerrors.MustBugf("unknown change operation")
	}

	return s.sendChange(changes)
}

func (s streamingChangeProvider) AddChangedDefinition(_ context.Context, rev revisions.HLCRevision, def datastore.SchemaDefinition) error {
	if s.content&datastore.WatchSchema != datastore.WatchSchema {
		return nil
	}

	changes := datastore.RevisionChanges{
		Revision:           rev,
		ChangedDefinitions: []datastore.SchemaDefinition{def},
	}

	return s.sendChange(changes)
}

func (s streamingChangeProvider) AddDeletedNamespace(_ context.Context, rev revisions.HLCRevision, namespaceName string) error {
	if s.content&datastore.WatchSchema != datastore.WatchSchema {
		return nil
	}

	changes := datastore.RevisionChanges{
		Revision:          rev,
		DeletedNamespaces: []string{namespaceName},
	}

	return s.sendChange(changes)
}

func (s streamingChangeProvider) AddDeletedCaveat(_ context.Context, rev revisions.HLCRevision, caveatName string) error {
	if s.content&datastore.WatchSchema != datastore.WatchSchema {
		return nil
	}

	changes := datastore.RevisionChanges{
		Revision:       rev,
		DeletedCaveats: []string{caveatName},
	}

	return s.sendChange(changes)
}

func (s streamingChangeProvider) SetRevisionMetadata(_ context.Context, rev revisions.HLCRevision, metadata map[string]any) error {
	if len(metadata) > 0 {
		parsedMetadata, err := structpb.NewStruct(metadata)
		if err != nil {
			return spiceerrors.MustBugf("failed to convert metadata to structpb: %v", err)
		}

		changes := datastore.RevisionChanges{
			Revision: rev,
			Metadata: parsedMetadata,
		}

		return s.sendChange(changes)
	}

	return nil
}

type (
	sendChangeFunc func(change datastore.RevisionChanges) error
	sendErrorFunc  func(err error)
)

func (cds *crdbDatastore) processChanges(ctx context.Context, changes pgx.Rows, sendError sendErrorFunc, sendChange sendChangeFunc, opts datastore.WatchOptions, streaming bool) {
	var tracked changeTracker[revisions.HLCRevision, revisions.HLCRevision]
	if streaming {
		tracked = &streamingChangeProvider{
			sendChange: sendChange,
			sendError:  sendError,
			content:    opts.Content,
		}
	} else {
		tracked = common.NewChanges(revisions.HLCKeyFunc, opts.Content, opts.MaximumBufferedChangesByteSize)
	}

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

			filtered, err := tracked.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev)
			if err != nil {
				sendError(err)
				return
			}

			for _, revChange := range filtered {
				revChange := revChange

				// TODO(jschorr): Change this to a new event type if/when we decide to report these
				// row GCs.

				// Filter out any DELETEs that occurred due to a relationship being expired and CRDB
				// performing GC of the row.
				// As CRDB does not (currently) provide a means for differentiating rows deleted by a user
				// and rows deleted by the system, we use a set of heuristics to determine when to filter:
				//
				// 1) Rows deleted by the TTL cleanup in CRDB will not have any metadata associated with the
				//    transaction.
				// 2) No other operations besides DELETE will be performed by the TTL cleanup.
				// 3) Only filter rows that had an expires_at that are before the current time.
				if _, ok := revChange.Metadata.AsMap()[spicedbTransactionKey]; !ok {
					if len(revChange.ChangedDefinitions) == 0 && len(revChange.DeletedNamespaces) == 0 &&
						len(revChange.DeletedCaveats) == 0 {
						hasNonTTLEvent := false
						for _, relChange := range revChange.RelationshipChanges {
							if relChange.Operation != tuple.UpdateOperationDelete {
								hasNonTTLEvent = true
								break
							}

							if relChange.Relationship.OptionalExpiration == nil {
								hasNonTTLEvent = true
								break
							}

							if relChange.Relationship.OptionalExpiration.After(time.Now()) {
								hasNonTTLEvent = true
								break
							}
						}

						if !hasNonTTLEvent {
							// Skip this event.
							continue
						}
					}
				}

				if err := sendChange(revChange); err != nil {
					sendError(err)
					return
				}
			}

			if opts.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints {
				if err := sendChange(datastore.RevisionChanges{
					Revision:     rev,
					IsCheckpoint: true,
				}); err != nil {
					sendError(err)
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
		case cds.schema.RelationshipTableName:
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

			var expiration *time.Time
			if details.After != nil {
				expiration = details.After.RelationshipExpiration
			}

			var integrity *core.RelationshipIntegrity
			if details.After != nil && details.After.IntegrityKeyID != nil && details.After.IntegrityHashAsHex != nil && details.After.TimestampAsString != nil {
				hexString := *details.After.IntegrityHashAsHex
				hashBytes, err := hex.DecodeString(hexString[2:]) // drop the \x
				if err != nil {
					sendError(fmt.Errorf("could not decode hash bytes: %w", err))
					return
				}

				timestampString := *details.After.TimestampAsString
				parsedTime, err := time.Parse("2006-01-02T15:04:05.999999999", timestampString)
				if err != nil {
					sendError(fmt.Errorf("could not parse timestamp: %w", err))
					return
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
				OptionalCaveat:     ctxCaveat,
				OptionalIntegrity:  integrity,
				OptionalExpiration: expiration,
			}

			rev, err := revisions.HLCRevisionFromString(details.Updated)
			if err != nil {
				sendError(fmt.Errorf("malformed update timestamp: %w", err))
				return
			}

			if details.After == nil {
				if err := tracked.AddRelationshipChange(ctx, rev, relationship, tuple.UpdateOperationDelete); err != nil {
					sendError(err)
					return
				}
			} else {
				if err := tracked.AddRelationshipChange(ctx, rev, relationship, tuple.UpdateOperationTouch); err != nil {
					sendError(err)
					return
				}
			}

		case schema.TableNamespace:
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

		case schema.TableCaveat:
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

		case schema.TableTransactionMetadata:
			if details.After != nil {
				rev, err := revisions.HLCRevisionFromString(details.Updated)
				if err != nil {
					sendError(fmt.Errorf("malformed update timestamp: %w", err))
					return
				}

				adjustedMetadata := details.After.Metadata
				delete(adjustedMetadata, spicedbTransactionKey)
				if err := tracked.SetRevisionMetadata(ctx, rev, adjustedMetadata); err != nil {
					sendError(err)
					return
				}
			}

		default:
			sendError(spiceerrors.MustBugf("unexpected table name in changefeed: %s", tableName))
			return
		}
	}

	if changes.Err() != nil {
		sendError(changes.Err())
		return
	}
}
