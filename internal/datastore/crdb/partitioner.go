package crdb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// queryShowRanges queries ranges from the primary index only. Using
// SHOW RANGES FROM TABLE would include secondary index ranges whose start keys
// have columns in a different order than the PK, producing partition bounds
// that overlap when compared in PK tuple order.
const queryShowRanges = "SELECT start_key FROM [SHOW RANGES FROM INDEX %s@primary] ORDER BY start_key"

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
		return SinglePartitionRange, nil
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
// PK values may contain "/" characters (e.g., "dev_v1/my_organization"), so we
// extract quoted strings by scanning for matching quote pairs rather than
// splitting on "/".
//
// This is safe because none of SpiceDB's allowed PK characters include the
// double-quote character that would break quote-pair scanning:
//   - namespace/relation: [a-z0-9_/]
//   - object_id: [a-zA-Z0-9/_|\-=+]
//   - userset_relation: [a-z0-9_.] (includes "..." ellipsis)
func parseRangeStartKey(key string) (options.Cursor, error) {
	values := extractQuotedValues(key)

	if len(values) < 6 {
		return nil, fmt.Errorf("expected at least 6 PK columns in range key, got %d", len(values))
	}

	// Reject boundaries with empty PK fields — they can't form valid cursors.
	if slices.Contains(values[:6], "") {
		return nil, errors.New("range key contains empty PK column")
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

// extractQuotedValues scans a string for quoted substrings (e.g., "foo")
// and returns their unquoted contents. This handles values that contain "/"
// characters, which would be broken by a naive strings.Split("/") approach.
func extractQuotedValues(s string) []string {
	values := make([]string, 0, 6)
	for {
		open := strings.Index(s, `"`)
		if open == -1 {
			break
		}
		s = s[open+1:]
		end := strings.Index(s, `"`)
		if end == -1 {
			break
		}
		values = append(values, s[:end])
		s = s[end+1:]
	}
	return values
}
