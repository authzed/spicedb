package crdb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// queryShowRanges queries ranges from the primary index only, including
// each range's size in bytes. Using SHOW RANGES FROM TABLE would include
// secondary index ranges whose start keys have columns in a different order
// than the PK, producing partition bounds that overlap when compared in PK
// tuple order. WITH DETAILS exposes range_size, which we use to balance
// partitions by data volume rather than by range count.
const queryShowRanges = "SELECT start_key, range_size FROM [SHOW RANGES FROM INDEX %s@primary WITH DETAILS] ORDER BY start_key"

var (
	// SinglePartitionRange is a single partition covering the entire table,
	// used as a fallback when partitioning is not possible or not requested.
	SinglePartitionRange = []datastore.PartitionRange{{LowerBound: nil, UpperBound: nil}}

	// Compile-time assertion that crdbDatastore implements BulkExportPartitioner.
	_ datastore.BulkExportPartitioner = (*crdbDatastore)(nil)
)

// rangeInfo describes a single CRDB range whose start_key parses to a valid
// PK cursor (a usable split candidate), along with its size in bytes.
type rangeInfo struct {
	cursor options.Cursor
	size   int64
}

// PlanPartitions splits the relationship table into non-overlapping key ranges
// for parallel bulk export. It uses CRDB's SHOW RANGES to align partitions
// with physical data distribution, and balances partitions by total range size
// so that workers process roughly equal amounts of data.
//
// CRDB automatically splits ranges when they exceed range_max_bytes (default
// 512MB), regardless of the number of nodes. A table with tens of billions of
// rows will have hundreds or thousands of ranges even on a single node. For
// very small tables (< 512MB) that haven't split yet, a single partition is
// returned.
func (cds *crdbDatastore) PlanPartitions(ctx context.Context, desiredCount uint32) ([]datastore.PartitionRange, error) {
	if desiredCount <= 1 {
		return SinglePartitionRange, nil
	}

	ranges, err := cds.rangeInfos(ctx)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Uint32("desired_partitions", desiredCount).
			Msg("SHOW RANGES failed, returning single partition")
		return SinglePartitionRange, nil
	}

	if len(ranges) == 0 {
		log.Ctx(ctx).Info().Uint32("desired_partitions", desiredCount).
			Str("table", cds.schema.RelationshipTableName).
			Msg("no parseable range boundaries found (table may be < 512MB), returning single partition")
		return SinglePartitionRange, nil
	}

	partitions := groupRanges(ctx, ranges, desiredCount)
	log.Ctx(ctx).Info().
		Uint32("desired_partitions", desiredCount).
		Int("parseable_ranges", len(ranges)).
		Int("actual_partitions", len(partitions)).
		Str("table", cds.schema.RelationshipTableName).
		Msg("planned partitioned export from SHOW RANGES")

	return partitions, nil
}

// groupRanges takes a sorted list of ranges (each with a parseable cursor
// usable as a split point and a size) and a desired partition count K. It
// returns up to K non-overlapping PartitionRange values, choosing boundaries
// so that the cumulative size of each partition is as close as possible to
// totalSize/K. If fewer than K-1 ranges are available, fewer than K
// partitions are returned.
//
// Ranges must be sorted in ascending key order. Each range's cursor is the
// lower bound of that range (and, equivalently, the upper bound of the
// preceding range); a split at ranges[i].cursor closes off the partition
// containing ranges[0..i-1] and starts a new one.
func groupRanges(ctx context.Context, ranges []rangeInfo, desiredCount uint32) []datastore.PartitionRange {
	if desiredCount <= 1 || len(ranges) == 0 {
		return SinglePartitionRange
	}

	K := int(desiredCount)
	var totalSize int64
	for _, r := range ranges {
		totalSize += r.size
	}

	// If every range reports size 0 (e.g., the table is brand new and CRDB
	// hasn't accounted for it yet), fall back to range-count balancing by
	// treating each range as one unit. This is logged at info level because
	// it materially changes how splits are chosen — partitions will be
	// balanced by range count, not by data volume.
	useUnitSize := totalSize == 0
	if useUnitSize {
		log.Ctx(ctx).Info().
			Int("ranges", len(ranges)).
			Msg("range_size unavailable for all ranges; falling back to range-count balancing")
		totalSize = int64(len(ranges))
	}

	target := totalSize / int64(K)
	if target <= 0 {
		target = 1
	}

	// Greedy pack: walk ranges in order, deciding at each cursor whether to
	// close off the current partition by splitting there. Each ranges[i].cursor
	// is a usable split point — including ranges[0].cursor, which separates
	// the unparseable prefix region (whose start_key we can't parse, but which
	// holds real data) from the rest. We seed the accumulator with an estimate
	// of the prefix region's size so it becomes a candidate split point: one
	// unit in unit-size mode, or the average range size in size-aware mode.
	// Without this seed the algorithm would always merge the prefix into the
	// first partition and produce at most len(ranges) partitions instead of
	// len(ranges)+1.
	splits := make([]options.Cursor, 0, K-1)
	var acc int64
	if useUnitSize {
		acc = 1
	} else if len(ranges) > 0 {
		acc = totalSize / int64(len(ranges))
	}
	for i := 0; i < len(ranges) && len(splits) < K-1; i++ {
		if acc >= target {
			splits = append(splits, ranges[i].cursor)
			acc = 0
		}
		if useUnitSize {
			acc++
		} else {
			acc += ranges[i].size
		}
	}

	partitions := make([]datastore.PartitionRange, len(splits)+1)
	for i := range partitions {
		if i > 0 {
			partitions[i].LowerBound = splits[i-1]
		}
		if i < len(splits) {
			partitions[i].UpperBound = splits[i]
		}
	}
	return partitions
}

// rangeInfos returns CRDB primary-index ranges for the relationship table in
// ascending key order, with each range annotated by its size in bytes. Rows
// whose start_key cannot be parsed as a PK tuple — most commonly the first
// range, whose key is just the table prefix — are skipped: they cannot serve
// as split points, and their size contribution (bounded by range_max_bytes,
// ~512MB) is negligible on the large tables this feature targets.
func (cds *crdbDatastore) rangeInfos(ctx context.Context) ([]rangeInfo, error) {
	query := fmt.Sprintf(queryShowRanges, cds.schema.RelationshipTableName)

	var ranges []rangeInfo
	var totalRows, skippedRows int
	if err := cds.readPool.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			totalRows++
			var (
				startKey string
				size     int64
			)

			if err := rows.Scan(&startKey, &size); err != nil {
				return fmt.Errorf("unable to scan range row: %w", err)
			}

			cursor, err := parseRangeStartKey(startKey)
			if err != nil {
				skippedRows++
				log.Ctx(ctx).Debug().Err(err).Str("start_key", startKey).Msg("skipping range with unparseable start_key")
				continue
			}

			ranges = append(ranges, rangeInfo{cursor: cursor, size: size})
		}
		return nil
	}, query); err != nil {
		return nil, fmt.Errorf("range query failed: %w", err)
	}

	log.Ctx(ctx).Debug().
		Int("total_ranges", totalRows).
		Int("parseable_boundaries", len(ranges)).
		Int("skipped", skippedRows).
		Msg("SHOW RANGES results")

	return ranges, nil
}

// parseRangeStartKey parses a CRDB range start key like:
//
//	/Table/53/1/"namespace"/"object_id"/"relation"/"userset_ns"/"userset_oid"/"userset_rel"
//
// or the abbreviated form:
//
//	…/1/"namespace"/"object_id"/"relation"/"userset_ns"/"userset_oid"/"userset_rel"
//
// CRDB's pretty printer uses Go's strconv.Quote format for string values in
// range keys. We use strconv.Unquote to decode them, matching CRDB's own
// parsing logic.
// Reference: https://github.com/cockroachdb/cockroach/blob/master/pkg/keys/printer.go
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
// and returns their unquoted contents using strconv.Unquote, which handles
// all Go/CRDB string literal escape sequences (e.g., \", \\, \n, \t, \xNN,
// \uNNNN).
func extractQuotedValues(s string) []string {
	values := make([]string, 0, 6)
	for {
		// Find opening quote.
		start := strings.Index(s, `"`)
		if start == -1 {
			break
		}

		// Find closing unescaped quote.
		end := findClosingQuote(s[start+1:])
		if end == -1 {
			break
		}

		// Extract the full quoted string including both quotes and unquote it.
		quoted := s[start : start+1+end+1]
		unquoted, err := strconv.Unquote(quoted)
		if err != nil {
			// Malformed — skip this segment and continue.
			s = s[start+1+end+1:]
			continue
		}

		values = append(values, unquoted)
		s = s[start+1+end+1:]
	}
	return values
}

// findClosingQuote finds the index of the closing unescaped `"` in s,
// where s starts just after the opening quote.
func findClosingQuote(s string) int {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++ // skip escaped character
		case '"':
			return i
		}
	}
	return -1
}
