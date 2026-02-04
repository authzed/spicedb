package spanner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
	"github.com/prometheus/client_golang/prometheus"
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

var retryHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "datastore",
	Name:      "spanner_watch_retries",
	Help:      "watch retry distribution",
	Buckets:   []float64{0, 1, 2, 5, 10, 20, 50},
})

func init() {
	prometheus.MustRegister(retryHistogram)
}

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

func (sd *spannerDatastore) DefaultsWatchOptions() datastore.WatchOptions {
	return datastore.WatchOptions{
		CheckpointInterval:      100 * time.Millisecond,
		WatchBufferLength:       defaultWatchBufferLength,
		WatchBufferWriteTimeout: defaultWatchBufferWriteTimeout,
		// Spanner does not use WatchConnectTimeout
		// Spanner does not support EmitImmediatelyStrategy
	}
}

func (sd *spannerDatastore) Watch(ctx context.Context, afterRevision datastore.Revision, opts datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	updates := make(chan datastore.RevisionChanges, opts.WatchBufferLength)
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

	sendChange := func(change datastore.RevisionChanges) bool {
		select {
		case updates <- change:
			return true

		default:
			// If we cannot immediately write, setup the timer and try again.
		}

		timer := time.NewTimer(opts.WatchBufferWriteTimeout)
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
			HeartbeatInterval: opts.CheckpointInterval,
			SpannerClientOptions: []option.ClientOption{
				option.WithCredentialsFile(sd.config.credentialsFilePath),
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

	// NOTE: the callback below might be called concurrently across partitions.
	err = reader.Read(ctx, func(result *changestreams.ReadResult) error {
		// See: https://cloud.google.com/spanner/docs/change-streams/details
		for _, record := range result.ChangeRecords {
			for _, dcr := range record.DataChangeRecords {
				txnID := dcr.ServerTransactionID
				changeRevision := revisions.NewForTime(dcr.CommitTimestamp)
				modType := dcr.ModType // options are INSERT, UPDATE, DELETE

				// Get or create tracked changes for this transaction.
				tracked, _ := txnBuffer.LoadOrStore(txnID, common.NewChanges(revisions.TimestampIDKeyFunc, opts.Content, opts.MaximumBufferedChangesByteSize))

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
						changes, err := tracked.AsRevisionChanges(revisions.TimestampIDKeyLessThanFunc)
						if err != nil {
							return err
						}

						for _, revChange := range changes {
							if !sendChange(revChange) {
								return datastore.NewWatchDisconnectedErr()
							}
						}
					}
				}

				// When there are data changes written to the partition,
				// data_change_record.commit_timestamp can be used instead of heartbeat_record.timestamp to tell
				// that the reader is making forward progress in reading the partition.
				if opts.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints {
					if !sendChange(datastore.RevisionChanges{
						Revision:     revisions.NewForTime(dcr.CommitTimestamp),
						IsCheckpoint: true,
					}) {
						return datastore.NewWatchDisconnectedErr()
					}
				}
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
