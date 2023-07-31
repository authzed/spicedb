package spanner

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
	"google.golang.org/api/option"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	RelationTupleChangeStreamName = "relation_tuple_stream"
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
						primaryKeyColumnValues := mod.Keys.Value.(map[string]any)
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

						oldValues := mod.OldValues.Value.(map[string]any)

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
							newValues := mod.NewValues.Value.(map[string]any)
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
