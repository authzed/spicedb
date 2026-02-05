package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

// pgSchemaHashWatcher implements SingleStoreSchemaHashWatcher for Postgres using polling.
// It queries only the schema_revision table (not the full schema table) for fast hash lookups.
type pgSchemaHashWatcher struct {
	query pgxcommon.DBFuncQuerier
}

// newPGSchemaHashWatcher creates a new schema hash watcher for Postgres.
func newPGSchemaHashWatcher(query pgxcommon.DBFuncQuerier) *pgSchemaHashWatcher {
	return &pgSchemaHashWatcher{query: query}
}

// WatchSchemaHash polls the schema_revision table for changes to the schema hash.
func (w *pgSchemaHashWatcher) WatchSchemaHash(ctx context.Context, refreshInterval time.Duration, callback datastore.SchemaWatchCallback) error {
	// Read initial hash from schema_revision table
	lastHash, err := w.readSchemaHash(ctx)
	if err != nil && !errors.Is(err, datastore.ErrSchemaNotFound) {
		return fmt.Errorf("failed to read initial schema hash: %w", err)
	}

	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Read hash with revision for the callback
			ch, currentRevision, err := w.readSchemaHashWithRevision(ctx)
			if err != nil {
				if errors.Is(err, datastore.ErrSchemaNotFound) {
					if lastHash != "" {
						lastHash = ""
						if err := callback("", nil); err != nil {
							return fmt.Errorf("callback error: %w", err)
						}
					}
					continue
				}
				return fmt.Errorf("failed to read schema hash with revision: %w", err)
			}

			// Always call callback to update checkpoint, even if hash hasn't changed
			if err := callback(ch, currentRevision); err != nil {
				return fmt.Errorf("callback error: %w", err)
			}

			lastHash = ch
		}
	}
}

// readSchemaHash reads just the schema hash from the schema_revision table.
func (w *pgSchemaHashWatcher) readSchemaHash(ctx context.Context) (string, error) {
	sql, args, err := psql.Select("hash").
		From("schema_revision").
		Where(sq.Eq{
			"name":        "current",
			"deleted_xid": liveDeletedTxnID,
		}).
		ToSql()
	if err != nil {
		return "", fmt.Errorf("failed to build query: %w", err)
	}

	var hashBytes []byte

	err = w.query.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&hashBytes)
	}, sql, args...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", datastore.ErrSchemaNotFound
		}
		return "", fmt.Errorf("failed to query schema hash: %w", err)
	}

	return string(hashBytes), nil
}

// readSchemaHashWithRevision reads the schema hash and revision from the schema_revision table.
func (w *pgSchemaHashWatcher) readSchemaHashWithRevision(ctx context.Context) (string, datastore.Revision, error) {
	sql, args, err := psql.Select("hash", "created_xid").
		From("schema_revision").
		Where(sq.Eq{
			"name":        "current",
			"deleted_xid": liveDeletedTxnID,
		}).
		ToSql()
	if err != nil {
		return "", nil, fmt.Errorf("failed to build query: %w", err)
	}

	var hashBytes []byte
	var xid uint64

	err = w.query.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&hashBytes, &xid)
	}, sql, args...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil, datastore.ErrSchemaNotFound
		}
		return "", nil, fmt.Errorf("failed to query schema hash: %w", err)
	}

	revision := revisions.NewForTransactionID(xid)
	return string(hashBytes), revision, nil
}

var _ datastore.SingleStoreSchemaHashWatcher = (*pgSchemaHashWatcher)(nil)
