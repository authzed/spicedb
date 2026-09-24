package spanner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
	"github.com/puzpuzpuz/xsync/v4"
	"google.golang.org/api/option"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	CombinedChangeStreamName = "combined_change_stream"
)

// Copied from the spanner library: https://github.com/googleapis/google-cloud-go/blob/f03779538f949fb4ad93d5247d3c6b3e5b21091a/spanner/client.go#L67
// License: Apache License, Version 2.0, Copyright 2017 Google LLC
var validDBPattern = regexp.MustCompile("^projects/(?P<project>[^/]+)/instances/(?P<instance>[^/]+)/databases/(?P<database>[^/]+)$")

func parseDatabaseName(db string) (project, instance, database string, err error) {
	matches := validDBPattern.FindStringSubmatch(db)
	if len(matches) == 0 {
		return "", "", "", fmt.Errorf("failed to parse database name from %q according to pattern %q",
			db, validDBPattern.String())
	}
	return matches[1], matches[2], matches[3], nil
}

// checkpointTracker works out how far a Spanner change stream has been read.
//
// Spanner splits a change stream into partitions that are read at the same time
// and independently of one another, so how far one partition has been read says
// nothing about the others. A checkpoint promises the caller that it has seen
// everything up to that revision, so it can only be as recent as the *least*
// advanced partition currently being read. Each partition reports its progress
// three ways: the commit timestamp of a change it hands over, a heartbeat while
// it has no changes to hand over, and the start timestamp of the partitions
// that replace it when it is split or merged.
type checkpointTracker struct {
	lock sync.Mutex

	// readTo holds, for each partition still being read, the timestamp up to
	// which that partition's changes have already been handed to the caller.
	readTo map[string]time.Time // GUARDED_BY(lock)

	// lastCheckpoint is the timestamp of the most recent checkpoint handed to
	// the caller, so checkpoints never go backwards.
	lastCheckpoint time.Time // GUARDED_BY(lock)
}

func newCheckpointTracker() *checkpointTracker {
	return &checkpointTracker{readTo: make(map[string]time.Time)}
}

// readUpTo records that everything a partition has to say up to and including
// the given timestamp has already been handed to the caller.
func (ct *checkpointTracker) readUpTo(partitionToken string, timestamp time.Time) {
	ct.lock.Lock()
	defer ct.lock.Unlock()

	if existing, ok := ct.readTo[partitionToken]; !ok || timestamp.After(existing) {
		ct.readTo[partitionToken] = timestamp
	}
}

// replacedBy records that a partition has been split or merged and that the
// given partitions take over from it at the given timestamp. Spanner only ever
// sends a child partitions record as the very last record of a partition, so by
// the time this is called the replaced partition has nothing left to say and
// can stop holding the checkpoint back.
func (ct *checkpointTracker) replacedBy(partitionToken string, childTokens []string, startTimestamp time.Time) {
	ct.lock.Lock()
	defer ct.lock.Unlock()

	for _, childToken := range childTokens {
		// A merged partition is announced once by each of its parents; keep the
		// value already recorded if it is further along.
		if existing, ok := ct.readTo[childToken]; !ok || startTimestamp.After(existing) {
			ct.readTo[childToken] = startTimestamp
		}
	}

	delete(ct.readTo, partitionToken)
}

// nextCheckpoint returns the timestamp of the checkpoint to hand to the caller,
// and false if the stream has not been read any further since the last one.
func (ct *checkpointTracker) nextCheckpoint() (time.Time, bool) {
	ct.lock.Lock()
	defer ct.lock.Unlock()

	var readEverywhereTo time.Time
	for _, timestamp := range ct.readTo {
		if readEverywhereTo.IsZero() || timestamp.Before(readEverywhereTo) {
			readEverywhereTo = timestamp
		}
	}

	if readEverywhereTo.IsZero() || !readEverywhereTo.After(ct.lastCheckpoint) {
		return time.Time{}, false
	}

	ct.lastCheckpoint = readEverywhereTo
	return readEverywhereTo, true
}

func (sd *spannerDatastore) Watch(ctx context.Context, afterRevision datastore.Revision, opts datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	watchBufferLength := opts.WatchBufferLength
	if watchBufferLength == 0 {
		watchBufferLength = sd.watchBufferLength
	}

	updates := make(chan datastore.RevisionChanges, watchBufferLength)
	errs := make(chan error, 2) // we may try to send >1 error

	if opts.EmissionStrategy == datastore.EmitImmediatelyStrategy {
		close(updates)
		errs <- errors.New("emit immediately strategy is unsupported in Spanner")
		return updates, errs
	}

	go sd.watch(ctx, afterRevision, opts, updates, errs)

	return updates, errs
}

func (sd *spannerDatastore) watch(
	ctx context.Context,
	afterRevisionRaw datastore.Revision,
	opts datastore.WatchOptions,
	updates chan datastore.RevisionChanges,
	errs chan error,
) {
	defer close(updates)
	defer close(errs)

	// NOTE: 100ms is the minimum allowed.
	heartbeatInterval := max(opts.CheckpointInterval, 100*time.Millisecond)

	sendError := func(err error) {
		if errors.Is(ctx.Err(), context.Canceled) || common.IsCancellationError(err) {
			errs <- datastore.NewWatchCanceledErr()
			return
		}

		if common.IsResettableError(err) {
			errs <- datastore.NewWatchTemporaryErr(err)
			return
		}

		errs <- err
	}

	if !sd.watchEnabled {
		sendError(datastore.NewWatchDisabledErr("watch disabled in this datastore"))
		return
	}

	watchBufferWriteTimeout := opts.WatchBufferWriteTimeout
	if watchBufferWriteTimeout <= 0 {
		watchBufferWriteTimeout = sd.watchBufferWriteTimeout
	}

	sendChange := func(change datastore.RevisionChanges) bool {
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

	project, instance, database, err := parseDatabaseName(sd.database)
	if err != nil {
		sendError(err)
		return
	}

	afterRevision, ok := afterRevisionRaw.(revisions.TimestampRevision)
	if !ok {
		sendError(datastore.NewInvalidRevisionErr(afterRevisionRaw, datastore.CouldNotDetermineRevision))
		return
	}

	reader, err := changestreams.NewReaderWithConfig(
		ctx,
		project,
		instance,
		database,
		CombinedChangeStreamName,
		changestreams.Config{
			StartTimestamp:    afterRevision.Time().Add(1 * time.Nanosecond), // records with commit_timestamp greater than or equal to start_timestamp will be returned
			HeartbeatInterval: heartbeatInterval,
			SpannerClientOptions: []option.ClientOption{
				option.WithCredentialsFile(sd.config.credentialsFilePath), //nolint:staticcheck  // The preferred approach is using Application Default Credentials
			},
			SpannerClientConfig: spanner.ClientConfig{
				QueryOptions: spanner.QueryOptions{
					Priority: sppb.RequestOptions_PRIORITY_LOW,
				},
				ApplyOptions: []spanner.ApplyOption{
					spanner.Priority(sppb.RequestOptions_PRIORITY_LOW),
				},
			},
		})
	if err != nil {
		sendError(err)
		return
	}
	defer reader.Close()

	metadataForTransactionTag := xsync.NewMap[string, common.TransactionMetadata]()

	addMetadataForTransactionTag := func(ctx context.Context, tracked *common.Changes[revisions.TimestampRevision, int64], revision revisions.TimestampRevision, transactionTag string) error {
		if metadata, ok := metadataForTransactionTag.Load(transactionTag); ok {
			return tracked.AddRevisionMetadata(ctx, revision, metadata)
		}

		// Otherwise, load the metadata from the transactions metadata table.
		transactionMetadata, err := sd.readTransactionMetadata(ctx, transactionTag)
		if err != nil {
			return err
		}

		metadataForTransactionTag.Store(transactionTag, transactionMetadata)
		return tracked.AddRevisionMetadata(ctx, revision, transactionMetadata)
	}

	// This is a concurrent-safe map for incomplete transactions (transactions where IsLastRecordInTransactionInPartition=false).
	// For example if you send a write with both DELETEs and TOUCHEs, we get *two* separate DataChangeRecords for them,
	// but we only want to send them as *one* group.
	txnBuffer := xsync.NewMap[string, *common.Changes[revisions.TimestampRevision, int64]]()

	watchBufferSize := opts.MaximumBufferedChangesByteSize
	if watchBufferSize == 0 {
		watchBufferSize = sd.watchChangeBufferMaximumSize
	}

	wantsCheckpoints := opts.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints
	tracker := newCheckpointTracker()

	// Sending checkpoints is done under its own lock so that two partitions
	// finishing at the same time cannot put their checkpoints on the channel out
	// of order, which would look to the caller like the stream went backwards.
	var checkpointLock sync.Mutex
	sendCheckpoint := func() bool {
		checkpointLock.Lock()
		defer checkpointLock.Unlock()

		checkpoint, ok := tracker.nextCheckpoint()
		if !ok {
			return true
		}

		return sendChange(datastore.RevisionChanges{
			Revision:     revisions.NewForTime(checkpoint),
			IsCheckpoint: true,
		})
	}

	// NOTE: the callback below might be called concurrently across partitions.
	err = reader.Read(ctx, func(result *changestreams.ReadResult) error {
		// See: https://cloud.google.com/spanner/docs/change-streams/details
		for _, record := range result.ChangeRecords {
			for _, dcr := range record.DataChangeRecords {
				txnID := dcr.ServerTransactionID
				changeRevision := revisions.NewForTime(dcr.CommitTimestamp)
				modType := dcr.ModType // options are INSERT, UPDATE, DELETE

				// Get or create tracked changes for this transaction.
				tracked, _ := txnBuffer.LoadOrStore(txnID, common.NewChanges(revisions.TimestampIDKeyFunc, opts.Content, watchBufferSize))

				// See: https://cloud.google.com/spanner/docs/ttl
				// > TTL supports auditing its deletions through change streams. Change
				// > streams data records that track TTL changes to a database have the
				// > transaction_tag field set to RowDeletionPolicy and the
				// > is_system_transaction field set to true.
				// TODO could we not replace this with a filter on the change stream? https://docs.cloud.google.com/spanner/docs/change-streams/manage#filter-ttl-deletes
				if modType == "DELETE" && dcr.TransactionTag == "RowDeletionPolicy" && dcr.IsSystemTransaction {
					// Skip deletions that are performed by TTL policy.
					// TODO(jschorr): once we decide to emit events for GCed expired rels, change to emit those
					// events instead.
					continue
				}

				// NOTE when testing against the Spanner emulator, and until https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/280 is solved,
				// uncomment this line to test that transaction metadata is sent as part of the Watch response correctly.
				// dcr.TransactionTag = "some-value"
				if len(dcr.TransactionTag) > 0 {
					if err := addMetadataForTransactionTag(ctx, tracked, changeRevision, dcr.TransactionTag); err != nil {
						return err
					}
				}

				for _, mod := range dcr.Mods {
					primaryKeyColumnValues, ok := mod.Keys.Value.(map[string]any)
					if !ok {
						return spiceerrors.MustBugf("error converting keys map")
					}

					switch modType {
					case "DELETE":
						switch dcr.TableName {
						case tableRelationship:
							relationship := relationshipFromPrimaryKey(primaryKeyColumnValues)

							oldValues, ok := mod.OldValues.Value.(map[string]any)
							if !ok {
								return spiceerrors.MustBugf("error converting old values map")
							}

							relationship.OptionalCaveat, err = contextualizedCaveatFromValues(oldValues)
							if err != nil {
								return err
							}

							err := tracked.AddRelationshipChange(ctx, changeRevision, relationship, tuple.UpdateOperationDelete)
							if err != nil {
								return err
							}

						case tableNamespace:
							namespaceNameValue, ok := primaryKeyColumnValues[colNamespaceName]
							if !ok {
								return spiceerrors.MustBugf("missing namespace name value")
							}

							namespaceName, ok := namespaceNameValue.(string)
							if !ok {
								return spiceerrors.MustBugf("error converting namespace name: %v", primaryKeyColumnValues[colNamespaceName])
							}

							err := tracked.AddDeletedNamespace(ctx, changeRevision, namespaceName)
							if err != nil {
								return err
							}

						case tableCaveat:
							caveatNameValue, ok := primaryKeyColumnValues[colName]
							if !ok {
								return spiceerrors.MustBugf("missing caveat name")
							}

							caveatName, ok := caveatNameValue.(string)
							if !ok {
								return spiceerrors.MustBugf("error converting caveat name: %v", primaryKeyColumnValues[colName])
							}

							err := tracked.AddDeletedCaveat(ctx, changeRevision, caveatName)
							if err != nil {
								return err
							}

						default:
							return spiceerrors.MustBugf("unknown table name %s in delete of change stream", dcr.TableName)
						}

					case "INSERT":
						fallthrough

					case "UPDATE":
						newValues, ok := mod.NewValues.Value.(map[string]any)
						if !ok {
							return spiceerrors.MustBugf("error new values keys map")
						}

						switch dcr.TableName {
						case tableRelationship:
							relationship := relationshipFromPrimaryKey(primaryKeyColumnValues)

							oldValues, ok := mod.OldValues.Value.(map[string]any)
							if !ok {
								return spiceerrors.MustBugf("error converting old values map")
							}

							// NOTE: Spanner's change stream will return a record for a TOUCH operation that does not
							// change anything. Therefore, we check  to see if the caveat name or context has changed
							// between the old and new values, and only raise the event in that case. This works for
							// caveat context because Spanner will return either `nil` or a string value of the JSON.
							newValues, ok := mod.NewValues.Value.(map[string]any)
							if !ok {
								return spiceerrors.MustBugf("error converting new values map")
							}

							if oldValues[colCaveatName] == newValues[colCaveatName] && oldValues[colCaveatContext] == newValues[colCaveatContext] {
								continue
							}

							relationship.OptionalCaveat, err = contextualizedCaveatFromValues(newValues)
							if err != nil {
								return err
							}

							err := tracked.AddRelationshipChange(ctx, changeRevision, relationship, tuple.UpdateOperationTouch)
							if err != nil {
								return err
							}

						case tableNamespace:
							namespaceConfigValue, ok := newValues[colNamespaceConfig]
							if !ok {
								return spiceerrors.MustBugf("missing namespace config value")
							}

							ns := &core.NamespaceDefinition{}
							if err := unmarshalSchemaDefinition(ns, namespaceConfigValue); err != nil {
								return err
							}

							err := tracked.AddChangedDefinition(ctx, changeRevision, ns)
							if err != nil {
								return err
							}

						case tableCaveat:
							caveatDefValue, ok := newValues[colCaveatDefinition]
							if !ok {
								return spiceerrors.MustBugf("missing caveat definition value")
							}

							caveat := &core.CaveatDefinition{}
							if err := unmarshalSchemaDefinition(caveat, caveatDefValue); err != nil {
								return err
							}

							err := tracked.AddChangedDefinition(ctx, changeRevision, caveat)
							if err != nil {
								return err
							}

						default:
							return spiceerrors.MustBugf("unknown table name %s in delete of change stream", dcr.TableName)
						}

					default:
						return spiceerrors.MustBugf("unknown modtype in spanner change stream record")
					}
				}

				// Only send changes when we've received the last record for this transaction in this partition.
				if dcr.IsLastRecordInTransactionInPartition {
					// Remove from buffer since we have all the records for this transaction.
					txnBuffer.Delete(txnID)

					if !tracked.IsEmpty() {
						changes := tracked.AsRevisionChanges(revisions.TimestampIDKeyLessThanFunc)
						for revChange, err := range changes {
							if err != nil {
								return err
							}

							if !sendChange(revChange) {
								return datastore.NewWatchDisconnectedErr()
							}
						}
					}

					// Everything this partition had at this commit timestamp is now on
					// its way to the caller, so the partition has been read this far.
					// Records still waiting on the rest of their transaction are
					// deliberately not counted yet.
					tracker.readUpTo(result.PartitionToken, dcr.CommitTimestamp)
				}
			}

			// A heartbeat says the partition has no changes to report at or before
			// its timestamp, which is the only thing that moves a quiet partition -
			// and therefore a quiet database - forward.
			for _, hbr := range record.HeartbeatRecords {
				tracker.readUpTo(result.PartitionToken, hbr.Timestamp)
			}

			// A child partitions record is the last thing a partition sends before
			// Spanner splits or merges it. The reader follows the new partitions on
			// its own; all we do here is move the bookkeeping over to them.
			for _, cpr := range record.ChildPartitionsRecords {
				childTokens := make([]string, 0, len(cpr.ChildPartitions))
				for _, childPartition := range cpr.ChildPartitions {
					childTokens = append(childTokens, childPartition.Token)
				}

				tracker.replacedBy(result.PartitionToken, childTokens, cpr.StartTimestamp)
			}
		}

		if wantsCheckpoints {
			if !sendCheckpoint() {
				return datastore.NewWatchDisconnectedErr()
			}
		}

		return nil
	})
	if err != nil {
		sendError(err)
		return
	}
}

type unmarshallable interface {
	UnmarshalVT([]byte) error
}

func unmarshalSchemaDefinition(def unmarshallable, configValue any) error {
	base64SerializedConfig, ok := configValue.(string)
	if !ok {
		return spiceerrors.MustBugf("error converting config value")
	}

	serializedConfig, err := base64.StdEncoding.DecodeString(base64SerializedConfig)
	if err != nil {
		return fmt.Errorf(errUnableToReadConfig, err)
	}

	if err := def.UnmarshalVT(serializedConfig); err != nil {
		return fmt.Errorf(errUnableToReadConfig, err)
	}

	return nil
}

func relationshipFromPrimaryKey(primaryKeyColumnValues map[string]any) tuple.Relationship {
	return tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: primaryKeyColumnValues[colNamespace].(string),
				ObjectID:   primaryKeyColumnValues[colObjectID].(string),
				Relation:   primaryKeyColumnValues[colRelation].(string),
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: primaryKeyColumnValues[colUsersetNamespace].(string),
				ObjectID:   primaryKeyColumnValues[colUsersetObjectID].(string),
				Relation:   primaryKeyColumnValues[colUsersetRelation].(string),
			},
		},
	}
}

func contextualizedCaveatFromValues(values map[string]any) (*core.ContextualizedCaveat, error) {
	name := values[colCaveatName].(string)
	if name != "" {
		contextString := values[colCaveatContext]

		// NOTE: spanner returns the JSON field as a string here.
		var context map[string]any
		if contextString != nil {
			if err := json.Unmarshal([]byte(contextString.(string)), &context); err != nil {
				return nil, err
			}
		}

		return common.ContextualizedCaveatFrom(name, context)
	}
	return nil, nil
}
