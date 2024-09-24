package spanner

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/otel/trace"

	"github.com/ccoveille/go-safecast"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var querySomeRandomRelationships = fmt.Sprintf(`SELECT %s FROM %s LIMIT 10`,
	strings.Join([]string{
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
	}, ", "),
	tableRelationship)

const defaultEstimatedBytesPerRelationships = 20 // determined by looking at some sample clusters

func (sd *spannerDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	var uniqueID string
	if err := sd.client.Single().Read(
		context.Background(),
		tableMetadata,
		spanner.AllKeys(),
		[]string{colUniqueID},
	).Do(func(r *spanner.Row) error {
		return r.Columns(&uniqueID)
	}); err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to read unique ID: %w", err)
	}

	iter := sd.client.Single().Read(
		ctx,
		tableNamespace,
		spanner.AllKeys(),
		[]string{colNamespaceConfig, colNamespaceTS},
	)
	defer iter.Stop()

	allNamespaces, err := readAllNamespaces(iter, trace.SpanFromContext(ctx))
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to read namespaces: %w", err)
	}

	// If there is not yet a cached estimated bytes per relationship, read a few relationships and then
	// compute the average bytes per relationship.
	sd.cachedEstimatedBytesPerRelationshipLock.RLock()
	estimatedBytesPerRelationship := sd.cachedEstimatedBytesPerRelationship
	sd.cachedEstimatedBytesPerRelationshipLock.RUnlock()

	if estimatedBytesPerRelationship == 0 {
		riter := sd.client.Single().Query(ctx, spanner.Statement{SQL: querySomeRandomRelationships})
		defer riter.Stop()

		totalByteCount := 0
		totalRelationships := 0

		if err := riter.Do(func(row *spanner.Row) error {
			nextTuple := &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{},
				Subject:             &core.ObjectAndRelation{},
			}
			err := row.Columns(
				&nextTuple.ResourceAndRelation.Namespace,
				&nextTuple.ResourceAndRelation.ObjectId,
				&nextTuple.ResourceAndRelation.Relation,
				&nextTuple.Subject.Namespace,
				&nextTuple.Subject.ObjectId,
				&nextTuple.Subject.Relation,
			)
			if err != nil {
				return err
			}

			relationshipByteCount := len(nextTuple.ResourceAndRelation.Namespace) + len(nextTuple.ResourceAndRelation.ObjectId) +
				len(nextTuple.ResourceAndRelation.Relation) + len(nextTuple.Subject.Namespace) + len(nextTuple.Subject.ObjectId) +
				len(nextTuple.Subject.Relation)

			totalRelationships++
			totalByteCount += relationshipByteCount

			return nil
		}); err != nil {
			return datastore.Stats{}, err
		}

		if totalRelationships == 0 {
			return datastore.Stats{
				UniqueID:                   uniqueID,
				ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(allNamespaces),
				EstimatedRelationshipCount: 0,
			}, nil
		}

		estimatedBytesPerRelationship, err = safecast.ToUint64(totalByteCount / totalRelationships)
		if err != nil {
			return datastore.Stats{}, spiceerrors.MustBugf("could not cast estimated bytes to uint64: %v", err)
		}
		if estimatedBytesPerRelationship > 0 {
			sd.cachedEstimatedBytesPerRelationshipLock.Lock()
			sd.cachedEstimatedBytesPerRelationship = estimatedBytesPerRelationship
			sd.cachedEstimatedBytesPerRelationshipLock.Unlock()
		}
	}

	if estimatedBytesPerRelationship == 0 {
		estimatedBytesPerRelationship = defaultEstimatedBytesPerRelationships // Use a default
	}

	// Reference: https://cloud.google.com/spanner/docs/introspection/table-sizes-statistics
	queryRelationshipByteEstimate := fmt.Sprintf(`SELECT used_bytes FROM %s WHERE
		interval_end = (
			SELECT MAX(interval_end)
			FROM %s
		)
		AND table_name = '%s'`, sd.tableSizesStatsTable, sd.tableSizesStatsTable, tableRelationship)

	var byteEstimate spanner.NullInt64
	if err := sd.client.Single().Query(ctx, spanner.Statement{SQL: queryRelationshipByteEstimate}).Do(func(r *spanner.Row) error {
		return r.Columns(&byteEstimate)
	}); err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to read tuples byte count: %w", err)
	}

	// If the byte estimate is NULL, try to fallback to just selecting the single row. This is necessary for certain
	// versions of the emulator.
	if byteEstimate.IsNull() {
		lookupSingleEstimate := fmt.Sprintf(`SELECT used_bytes FROM %s WHERE table_name = '%s'`, sd.tableSizesStatsTable, tableRelationship)
		if err := sd.client.Single().Query(ctx, spanner.Statement{SQL: lookupSingleEstimate}).Do(func(r *spanner.Row) error {
			return r.Columns(&byteEstimate)
		}); err != nil {
			return datastore.Stats{}, fmt.Errorf("unable to fallback read tuples byte count: %w", err)
		}
	}

	uintByteEstimate, err := safecast.ToUint64(byteEstimate.Int64)
	if err != nil {
		return datastore.Stats{}, spiceerrors.MustBugf("unable to cast byteEstimate to uint64: %v", err)
	}

	return datastore.Stats{
		UniqueID:                   uniqueID,
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(allNamespaces),
		EstimatedRelationshipCount: uintByteEstimate / estimatedBytesPerRelationship,
	}, nil
}
