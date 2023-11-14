package spanner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/option"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	RelationTupleChangeStreamName = "relation_tuple_stream"
	SchemaChangeStreamName        = "schema_change_stream"
)

var retryHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "spicedb",
	Subsystem: "datastore",
	Name:      "spanner_schema_watch_retries",
	Help:      "schema watch retry distribution",
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

func (sd spannerDatastore) Watch(ctx context.Context, afterRevisionRaw datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, 10)
	errs := make(chan error, 1)

	go func() {
		project, instance, database, err := parseDatabaseName(sd.database)
		if err != nil {
			errs <- err
			return
		}

		afterRevision := afterRevisionRaw.(revision.Decimal)
		reader, err := changestreams.NewReaderWithConfig(
			ctx,
			project,
			instance,
			database,
			RelationTupleChangeStreamName,
			changestreams.Config{
				StartTimestamp:    timestampFromRevision(afterRevision),
				HeartbeatInterval: 30 * time.Second,
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
			errs <- err
			return
		}
		defer reader.Close()
		err = reader.Read(ctx, func(result *changestreams.ReadResult) error {
			tracked := common.NewChanges(revision.DecimalKeyFunc)

			// See: https://cloud.google.com/spanner/docs/change-streams/details
			for _, record := range result.ChangeRecords {
				for _, dcr := range record.DataChangeRecords {
					changeRevision := revisionFromTimestamp(dcr.CommitTimestamp)
					modType := dcr.ModType // options are INSERT, UPDATE, DELETE

					for _, mod := range dcr.Mods {
						primaryKeyColumnValues, ok := mod.Keys.Value.(map[string]any)
						if !ok {
							return spiceerrors.MustBugf("error converting keys map")
						}

						relationTuple := &core.RelationTuple{
							ResourceAndRelation: &core.ObjectAndRelation{
								Namespace: primaryKeyColumnValues[colNamespace].(string),
								ObjectId:  primaryKeyColumnValues[colObjectID].(string),
								Relation:  primaryKeyColumnValues[colRelation].(string),
							},
							Subject: &core.ObjectAndRelation{
								Namespace: primaryKeyColumnValues[colUsersetNamespace].(string),
								ObjectId:  primaryKeyColumnValues[colUsersetObjectID].(string),
								Relation:  primaryKeyColumnValues[colUsersetRelation].(string),
							},
						}

						oldValues, ok := mod.OldValues.Value.(map[string]any)
						if !ok {
							return spiceerrors.MustBugf("error converting old values map")
						}

						switch modType {
						case "DELETE":
							relationTuple.Caveat, err = contextualizedCaveatFromValues(oldValues)
							if err != nil {
								return err
							}

							tracked.AddChange(ctx, changeRevision, relationTuple, core.RelationTupleUpdate_DELETE)

						case "INSERT":
							fallthrough

						case "UPDATE":
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

							relationTuple.Caveat, err = contextualizedCaveatFromValues(newValues)
							if err != nil {
								return err
							}

							tracked.AddChange(ctx, changeRevision, relationTuple, core.RelationTupleUpdate_TOUCH)

						default:
							return spiceerrors.MustBugf("unknown modtype in spanner change stream record")
						}
					}
				}
			}

			changesToWrite := tracked.AsRevisionChanges(revision.DecimalKeyLessThanFunc)
			for _, changeToWrite := range changesToWrite {
				changeToWrite := changeToWrite
				select {
				case updates <- &changeToWrite:
					// Nothing to do here, we've already written to the channel.
				default:
					return datastore.NewWatchDisconnectedErr()
				}
			}

			return nil
		})
		if err != nil {
			errs <- err
			return
		}
	}()
	return updates, errs
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

const maxSchemaWatchRetryCount = 15

func (sd spannerDatastore) WatchSchema(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.SchemaState, <-chan error) {
	updates := make(chan *datastore.SchemaState, 10)
	errs := make(chan error, 1)

	lastRevision := afterRevision
	retryCount := -1
	go func() {
		running := true
		for running {
			retryCount++
			retryHistogram.Observe(float64(retryCount))
			if retryCount >= maxSchemaWatchRetryCount {
				errs <- fmt.Errorf("schema watch terminated after %d retries", retryCount)
				return
			}

			log.Debug().Str("revision", lastRevision.String()).Int("retry-count", retryCount).Msg("starting schema watch connection")

			sd.watchSchemaWithoutRetry(ctx, lastRevision, func(update *datastore.SchemaState, err error) {
				if err != nil {
					// TODO(jschorr): If this is a terminal error, just quit instead of retrying.
					running = true

					// Sleep a bit for retrying.
					pgxcommon.SleepOnErr(ctx, err, uint8(retryCount))
					return
				}

				if update.Revision.GreaterThan(lastRevision) {
					lastRevision = update.Revision
				}

				updates <- update
				retryCount = 0
			})
		}
	}()

	return updates, errs
}

func (sd spannerDatastore) watchSchemaWithoutRetry(ctx context.Context, afterRevisionRaw datastore.Revision, processUpdate func(update *datastore.SchemaState, err error)) {
	project, instance, database, err := parseDatabaseName(sd.database)
	if err != nil {
		processUpdate(nil, err)
		return
	}

	afterRevision := afterRevisionRaw.(revision.Decimal)
	reader, err := changestreams.NewReaderWithConfig(
		ctx,
		project,
		instance,
		database,
		SchemaChangeStreamName,
		changestreams.Config{
			StartTimestamp:    timestampFromRevision(afterRevision),
			HeartbeatInterval: sd.config.schemaWatchHeartbeat,
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
		processUpdate(nil, err)
		return
	}
	defer reader.Close()

	err = reader.Read(ctx, func(result *changestreams.ReadResult) error {
		// See: https://cloud.google.com/spanner/docs/change-streams/details
		for _, record := range result.ChangeRecords {
			for _, dcr := range record.DataChangeRecords {
				changeRevision := revisionFromTimestamp(dcr.CommitTimestamp)
				modType := dcr.ModType // options are INSERT, UPDATE, DELETE

				for _, mod := range dcr.Mods {
					primaryKeyColumnValues, ok := mod.Keys.Value.(map[string]any)
					if !ok {
						return spiceerrors.MustBugf("error converting keys map")
					}

					switch modType {
					case "DELETE":
						switch dcr.TableName {
						case tableNamespace:
							namespaceNameValue, ok := primaryKeyColumnValues[colNamespaceName]
							if !ok {
								return spiceerrors.MustBugf("missing namespace name value")
							}

							namespaceName, ok := namespaceNameValue.(string)
							if !ok {
								return spiceerrors.MustBugf("error converting namespace name: %v", primaryKeyColumnValues[colNamespaceName])
							}

							processUpdate(&datastore.SchemaState{
								Revision:          changeRevision,
								DeletedNamespaces: []string{namespaceName},
								IsCheckpoint:      false,
							}, nil)

						case tableCaveat:
							caveatNameValue, ok := primaryKeyColumnValues[colNamespaceName]
							if !ok {
								return spiceerrors.MustBugf("missing caveat name")
							}

							caveatName, ok := caveatNameValue.(string)
							if !ok {
								return spiceerrors.MustBugf("error converting caveat name: %v", primaryKeyColumnValues[colName])
							}

							processUpdate(&datastore.SchemaState{
								Revision:       changeRevision,
								DeletedCaveats: []string{caveatName},
								IsCheckpoint:   false,
							}, nil)

						default:
							return spiceerrors.MustBugf("unknown table name %s in delete of schema change stream", dcr.TableName)
						}

					case "INSERT":
						fallthrough

					case "UPDATE":
						newValues, ok := mod.NewValues.Value.(map[string]any)
						if !ok {
							return spiceerrors.MustBugf("error new values keys map")
						}

						switch dcr.TableName {
						case tableNamespace:
							namespaceConfigValue, ok := newValues[colNamespaceConfig]
							if !ok {
								return spiceerrors.MustBugf("missing namespace config value")
							}

							base64SerializedConfig, ok := namespaceConfigValue.(string)
							if !ok {
								return spiceerrors.MustBugf("error converting namespace config value")
							}

							serializedConfig, err := base64.StdEncoding.DecodeString(base64SerializedConfig)
							if err != nil {
								return fmt.Errorf(errUnableToReadConfig, err)
							}

							ns := &core.NamespaceDefinition{}
							if err := ns.UnmarshalVT(serializedConfig); err != nil {
								return fmt.Errorf(errUnableToReadConfig, err)
							}

							processUpdate(&datastore.SchemaState{
								Revision:           changeRevision,
								ChangedDefinitions: []datastore.SchemaDefinition{ns},
								IsCheckpoint:       false,
							}, nil)

						case tableCaveat:
							caveatDefValue, ok := newValues[colCaveatDefinition]
							if !ok {
								return spiceerrors.MustBugf("missing caveat definition value")
							}

							base64SerializedConfig, ok := caveatDefValue.(string)
							if !ok {
								return spiceerrors.MustBugf("error converting caveat definition value")
							}

							serializedConfig, err := base64.StdEncoding.DecodeString(base64SerializedConfig)
							if err != nil {
								return fmt.Errorf(errUnableToReadConfig, err)
							}

							caveat := &core.CaveatDefinition{}
							if err := caveat.UnmarshalVT(serializedConfig); err != nil {
								return fmt.Errorf(errUnableToReadConfig, err)
							}

							processUpdate(&datastore.SchemaState{
								Revision:           changeRevision,
								ChangedDefinitions: []datastore.SchemaDefinition{caveat},
								IsCheckpoint:       false,
							}, nil)

						default:
							return spiceerrors.MustBugf("unknown table name %s in delete of schema change stream", dcr.TableName)
						}

					default:
						return spiceerrors.MustBugf("unknown modtype in spanner schema change stream record")
					}
				}
			}

			for _, hbr := range record.HeartbeatRecords {
				processUpdate(&datastore.SchemaState{
					Revision:     revisionFromTimestamp(hbr.Timestamp),
					IsCheckpoint: true,
				}, nil)
			}
		}
		return nil
	})

	if err != nil {
		processUpdate(nil, err)
		return
	}
}
