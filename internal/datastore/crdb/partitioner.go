package crdb

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

const queryShowRanges = "SELECT start_key FROM [SHOW RANGES FROM TABLE %s] ORDER BY start_key"

// Compile-time assertion that crdbDatastore implements BulkExportPartitioner.
var _ datastore.BulkExportPartitioner = (*crdbDatastore)(nil)

// PlanPartitions splits the relationship table into non-overlapping key ranges
// for parallel bulk export. It uses CRDB's SHOW RANGES to align partitions
// with physical data distribution.
//
// CRDB automatically splits ranges when they exceed range_max_bytes (default
// 512MB), regardless of the number of nodes. A table with tens of billions of
// rows will have hundreds or thousands of ranges even on a single node. For
// very small tables (< 512MB) that haven't split yet, a single partition is
// returned.
func (cds *crdbDatastore) PlanPartitions(ctx context.Context, revision datastore.Revision, desiredCount uint32) ([]datastore.PartitionRange, error) {
	if desiredCount <= 1 {
		return []datastore.PartitionRange{{LowerBound: nil, UpperBound: nil}}, nil
	}

	boundaries, err := cds.rangeBoundaries(ctx)
	if err != nil {
		log.Warn().Err(err).Uint32("desired_partitions", desiredCount).
			Msg("SHOW RANGES failed, returning single partition")
		return []datastore.PartitionRange{{LowerBound: nil, UpperBound: nil}}, nil
	}

	if len(boundaries) == 0 {
		log.Info().Uint32("desired_partitions", desiredCount).
			Str("table", cds.schema.RelationshipTableName).
			Msg("no parseable range boundaries found (table may be < 512MB), returning single partition")
		return []datastore.PartitionRange{{LowerBound: nil, UpperBound: nil}}, nil
	}

	partitions := groupBoundaries(boundaries, desiredCount)
	log.Info().
		Uint32("desired_partitions", desiredCount).
		Int("parseable_boundaries", len(boundaries)).
		Int("actual_partitions", len(partitions)).
		Str("table", cds.schema.RelationshipTableName).
		Msg("planned partitioned export from SHOW RANGES")

	return partitions, nil
}

// groupBoundaries takes N split-point boundaries and a desired partition count K,
// and returns up to K non-overlapping PartitionRange values.
// N boundaries divide the key space into N+1 regions.
// Boundaries must be sorted in ascending key order.
func groupBoundaries(boundaries []options.Cursor, desiredCount uint32) []datastore.PartitionRange {
	if desiredCount <= 1 || len(boundaries) == 0 {
		return []datastore.PartitionRange{{LowerBound: nil, UpperBound: nil}}
	}

	K := int(desiredCount)
	N := len(boundaries)
	if K > N+1 {
		K = N + 1
	}

	partitions := make([]datastore.PartitionRange, K)
	for i := range partitions {
		// Select K-1 evenly-spaced boundaries from the N available.
		if i > 0 {
			idx := (i * N) / K
			partitions[i].LowerBound = boundaries[idx]
		}
		if i < K-1 {
			idx := ((i + 1) * N) / K
			partitions[i].UpperBound = boundaries[idx]
		}
	}

	return partitions
}

// rangeBoundaries returns the start keys of CRDB ranges for the relationship
// table, parsed into Cursor values. Each cursor represents a PK tuple.
func (cds *crdbDatastore) rangeBoundaries(ctx context.Context) ([]options.Cursor, error) {
	query := fmt.Sprintf(queryShowRanges, cds.schema.RelationshipTableName)

	var boundaries []options.Cursor
	var totalRows, skippedRows int
	if err := cds.readPool.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			totalRows++
			var startKey string
			if err := rows.Scan(&startKey); err != nil {
				return fmt.Errorf("unable to scan range start_key: %w", err)
			}

			if startKey == "" {
				skippedRows++
				continue
			}

			cursor, err := parseRangeStartKey(startKey)
			if err != nil {
				skippedRows++
				log.Debug().Err(err).Str("start_key", startKey).Msg("skipping unparseable range boundary")
				continue
			}

			boundaries = append(boundaries, cursor)
		}
		return nil
	}, query); err != nil {
		return nil, fmt.Errorf("range boundaries query failed: %w", err)
	}

	log.Debug().
		Int("total_ranges", totalRows).
		Int("parseable_boundaries", len(boundaries)).
		Int("skipped", skippedRows).
		Msg("SHOW RANGES results")

	return boundaries, nil
}

// parseRangeStartKey parses a CRDB range start key like:
//
//	/Table/53/1/"namespace"/"object_id"/"relation"/"userset_ns"/"userset_oid"/"userset_rel"
//
// or the abbreviated form:
//
//	…/1/"namespace"/"object_id"/"relation"/"userset_ns"/"userset_oid"/"userset_rel"
//
// into an options.Cursor.
func parseRangeStartKey(key string) (options.Cursor, error) {
	// Split on "/" and find quoted string values — the PK columns.
	parts := strings.Split(key, "/")

	var values []string
	for _, p := range parts {
		if len(p) >= 2 && p[0] == '"' && p[len(p)-1] == '"' {
			values = append(values, p[1:len(p)-1])
		}
	}

	if len(values) < 6 {
		return nil, fmt.Errorf("expected at least 6 PK columns in range key, got %d", len(values))
	}

	// Reject boundaries with empty PK fields — they can't form valid cursors.
	if slices.Contains(values[:6], "") {
		return nil, fmt.Errorf("range key contains empty PK column")
	}

	return options.ToCursor(
		tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: values[0],
					ObjectID:   values[1],
					Relation:   values[2],
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: values[3],
					ObjectID:   values[4],
					Relation:   values[5],
				},
			},
		},
	), nil
}
