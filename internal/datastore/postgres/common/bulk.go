package common

import (
	"context"
	"strconv"
	"strings"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// maxParametersPerStatement is the maximum number of bind parameters that the
// PostgreSQL extended query protocol permits in a single statement.
const maxParametersPerStatement = 65535

// numBaseColumns is the number of non-integrity columns written for each
// relationship. When more columns than this are requested, the additional
// columns carry the relationship integrity values.
const numBaseColumns = 9

// appendRelationshipValues appends the column values for a single relationship
// to args, in the same column order used by BulkLoad. When withIntegrity is
// true, the relationship integrity values are appended as well.
func appendRelationshipValues(args []any, rel *tuple.Relationship, withIntegrity bool) ([]any, error) {
	var caveatName string
	var caveatContext map[string]any
	if rel.OptionalCaveat != nil {
		caveatName = rel.OptionalCaveat.CaveatName
		caveatContext = rel.OptionalCaveat.Context.AsMap()
	}

	args = append(args,
		rel.Resource.ObjectType,
		rel.Resource.ObjectID,
		rel.Resource.Relation,
		rel.Subject.ObjectType,
		rel.Subject.ObjectID,
		rel.Subject.Relation,
		caveatName,
		caveatContext, // PGX serializes map[string]any to JSONB columns.
		rel.OptionalExpiration,
	)

	if withIntegrity {
		if rel.OptionalIntegrity == nil {
			return nil, spiceerrors.MustBugf("expected relationship integrity for bulk load")
		}

		args = append(args,
			rel.OptionalIntegrity.KeyId,
			rel.OptionalIntegrity.Hash,
			rel.OptionalIntegrity.HashedAt.AsTime(),
		)
	}

	return args, nil
}

// BulkLoad writes all of the relationships produced by iter into tupleTableName
// using batched INSERT statements with `ON CONFLICT DO NOTHING`. The conflict
// clause gives the load TOUCH-like semantics: relationships that already exist
// are silently skipped rather than causing the load to fail, which makes
// re-importing the same data idempotent. The returned count reflects the number
// of relationships that were actually inserted (i.e. excludes ones skipped
// because they already existed).
func BulkLoad(
	ctx context.Context,
	tx pgx.Tx,
	tupleTableName string,
	colNames []string,
	iter datastore.BulkWriteRelationshipSource,
) (uint64, error) {
	numCols := len(colNames)
	if numCols == 0 {
		return 0, spiceerrors.MustBugf("no columns provided to bulk load")
	}
	withIntegrity := numCols > numBaseColumns

	// The static prefix shared by every batch: `INSERT INTO <table> (cols) VALUES `.
	prefix := "INSERT INTO " + tupleTableName + " (" + strings.Join(colNames, ", ") + ") VALUES "

	// Cap the number of rows per statement so the bind-parameter count never
	// exceeds the wire-protocol limit.
	maxRowsPerBatch := maxParametersPerStatement / numCols

	var totalInserted uint64
	args := make([]any, 0, maxRowsPerBatch*numCols)
	var sb strings.Builder

	flush := func() error {
		rows := len(args) / numCols
		if rows == 0 {
			return nil
		}

		sb.Reset()
		sb.WriteString(prefix)
		param := 1
		for row := 0; row < rows; row++ {
			if row > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('(')
			for col := 0; col < numCols; col++ {
				if col > 0 {
					sb.WriteByte(',')
				}
				sb.WriteByte('$')
				sb.WriteString(strconv.Itoa(param))
				param++
			}
			sb.WriteByte(')')
		}
		sb.WriteString(" ON CONFLICT DO NOTHING")

		tag, err := tx.Exec(ctx, sb.String(), args...)
		if err != nil {
			return err
		}

		inserted, err := safecast.Convert[uint64](tag.RowsAffected())
		if err != nil {
			return spiceerrors.MustBugf("number inserted was negative: %v", err)
		}
		totalInserted += inserted

		args = args[:0]
		return nil
	}

	rel, err := iter.Next(ctx)
	for ; err == nil && rel != nil; rel, err = iter.Next(ctx) {
		args, err = appendRelationshipValues(args, rel, withIntegrity)
		if err != nil {
			return 0, err
		}

		if len(args)/numCols >= maxRowsPerBatch {
			if flushErr := flush(); flushErr != nil {
				return 0, flushErr
			}
		}
	}
	if err != nil {
		return 0, err
	}

	if flushErr := flush(); flushErr != nil {
		return 0, flushErr
	}

	return totalInserted, nil
}
