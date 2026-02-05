package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

// mysqlSchemaHashWatcher implements SingleStoreSchemaHashWatcher for MySQL using polling.
// It queries only the schema_revision table (not the full schema table) for fast hash lookups.
type mysqlSchemaHashWatcher struct {
	db                      *sql.DB
	schemaRevisionTableName string
}

// newMySQLSchemaHashWatcher creates a new schema hash watcher for MySQL.
func newMySQLSchemaHashWatcher(db *sql.DB, schemaRevisionTableName string) *mysqlSchemaHashWatcher {
	return &mysqlSchemaHashWatcher{
		db:                      db,
		schemaRevisionTableName: schemaRevisionTableName,
	}
}

// WatchSchemaHash polls the schema_revision table for changes to the schema hash.
func (w *mysqlSchemaHashWatcher) WatchSchemaHash(ctx context.Context, refreshInterval time.Duration, callback datastore.SchemaWatchCallback) error {
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
			currentHash, currentRevision, err := w.readSchemaHashWithRevision(ctx)
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
			if err := callback(currentHash, currentRevision); err != nil {
				return fmt.Errorf("callback error: %w", err)
			}

			lastHash = currentHash
		}
	}
}

// readSchemaHash reads just the schema hash from the schema_revision table.
func (w *mysqlSchemaHashWatcher) readSchemaHash(ctx context.Context) (string, error) {
	query, args, err := sb.Select("hash").
		From(w.schemaRevisionTableName).
		Where(sq.Eq{
			"name":                "current",
			"deleted_transaction": liveDeletedTxnID,
		}).
		ToSql()
	if err != nil {
		return "", fmt.Errorf("failed to build query: %w", err)
	}

	var hashBytes []byte

	err = w.db.QueryRowContext(ctx, query, args...).Scan(&hashBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", datastore.ErrSchemaNotFound
		}
		return "", fmt.Errorf("failed to query schema hash: %w", err)
	}

	return string(hashBytes), nil
}

// readSchemaHashWithRevision reads the schema hash and revision from the schema_revision table.
func (w *mysqlSchemaHashWatcher) readSchemaHashWithRevision(ctx context.Context) (string, datastore.Revision, error) {
	query, args, err := sb.Select("hash", "created_transaction").
		From(w.schemaRevisionTableName).
		Where(sq.Eq{
			"name":                "current",
			"deleted_transaction": liveDeletedTxnID,
		}).
		ToSql()
	if err != nil {
		return "", nil, fmt.Errorf("failed to build query: %w", err)
	}

	var hashBytes []byte
	var txnID uint64

	err = w.db.QueryRowContext(ctx, query, args...).Scan(&hashBytes, &txnID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, datastore.ErrSchemaNotFound
		}
		return "", nil, fmt.Errorf("failed to query schema hash: %w", err)
	}

	revision := revisions.NewForTransactionID(txnID)
	return string(hashBytes), revision, nil
}

var _ datastore.SingleStoreSchemaHashWatcher = (*mysqlSchemaHashWatcher)(nil)
