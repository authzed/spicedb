package crdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	"github.com/authzed/spicedb/pkg/datastore"
)

const (
	// Checkpoint frequency for schema hash watcher (as fast as CRDB will allow)
	schemaCheckpointFrequency = "0s"

	// Schema changefeed queries for different CRDB versions
	querySchemaChangefeed       = "CREATE CHANGEFEED FOR schema_revision WITH updated, cursor = '%s', resolved = '%s', min_checkpoint_frequency = '" + schemaCheckpointFrequency + "'"
	querySchemaChangefeedPreV25 = "EXPERIMENTAL CHANGEFEED FOR schema_revision WITH updated, cursor = '%s', resolved = '%s', min_checkpoint_frequency = '" + schemaCheckpointFrequency + "'"
	querySchemaChangefeedPreV22 = "EXPERIMENTAL CHANGEFEED FOR schema_revision WITH updated, cursor = '%s', resolved = '%s'"
)

// crdbSchemaHashWatcher implements SingleStoreSchemaHashWatcher for CRDB using CHANGEFEED.
// It watches the schema_revision table for changes to the schema hash.
type crdbSchemaHashWatcher struct {
	query                 *pool.RetryPool
	changefeedQueryFormat string
}

// newCRDBSchemaHashWatcher creates a new schema hash watcher for CRDB.
func newCRDBSchemaHashWatcher(query *pool.RetryPool, changefeedQueryFormat string) *crdbSchemaHashWatcher {
	return &crdbSchemaHashWatcher{
		query:                 query,
		changefeedQueryFormat: changefeedQueryFormat,
	}
}

type schemaRevisionChange struct {
	Resolved string
	Updated  string
	After    *struct {
		Name string `json:"name"`
		Hash []byte `json:"hash"`
	}
}

// WatchSchemaHash watches the schema_revision table using CHANGEFEED for changes to the schema hash.
func (w *crdbSchemaHashWatcher) WatchSchemaHash(ctx context.Context, refreshInterval time.Duration, callback datastore.SchemaWatchCallback) error {
	// Read initial hash
	lastHash, err := w.readSchemaHash(ctx)
	if err != nil && !errors.Is(err, datastore.ErrSchemaNotFound) {
		return fmt.Errorf("failed to read initial schema hash: %w", err)
	}

	// Get initial cursor (current time)
	cursor, _, err := readCRDBNow(ctx, w.query)
	if err != nil {
		return fmt.Errorf("failed to get initial cursor: %w", err)
	}

	// Create changefeed query using version-appropriate format
	changefeedQuery := fmt.Sprintf(w.changefeedQueryFormat, cursor, refreshInterval)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Execute changefeed query
		err := w.query.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
			for rows.Next() {
				var tableName string
				var key, value []byte

				if err := rows.Scan(&tableName, &key, &value); err != nil {
					return fmt.Errorf("failed to scan changefeed row: %w", err)
				}

				// Parse the change
				var change schemaRevisionChange
				if err := json.Unmarshal(value, &change); err != nil {
					return fmt.Errorf("failed to unmarshal change: %w", err)
				}

				// If this is a resolved timestamp (checkpoint), invoke callback with same hash
				// to indicate the world has moved forward
				if change.Resolved != "" {
					currentRevision, _, err := readCRDBNow(ctx, w.query)
					if err != nil {
						return fmt.Errorf("failed to get current revision for checkpoint: %w", err)
					}

					// Call with the same hash to update the checkpoint
					if lastHash != "" {
						if err := callback(lastHash, currentRevision); err != nil {
							return fmt.Errorf("callback error on checkpoint: %w", err)
						}
					}
					continue
				}

				// If we have an update with data
				if change.After != nil && change.After.Name == "current" {
					newHash := string(change.After.Hash)
					if newHash != lastHash {
						// Get current revision
						currentRevision, _, err := readCRDBNow(ctx, w.query)
						if err != nil {
							return fmt.Errorf("failed to get current revision: %w", err)
						}

						lastHash = newHash
						if err := callback(newHash, currentRevision); err != nil {
							return fmt.Errorf("callback error: %w", err)
						}
					}
				}
			}
			return rows.Err()
		}, changefeedQuery)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			// If changefeed fails, wait and retry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(refreshInterval):
				// Get new cursor and retry
				cursor, _, err = readCRDBNow(ctx, w.query)
				if err != nil {
					return fmt.Errorf("failed to get cursor for retry: %w", err)
				}
				continue
			}
		}
	}
}

// readSchemaHash reads just the schema hash from the schema_revision table.
func (w *crdbSchemaHashWatcher) readSchemaHash(ctx context.Context) (string, error) {
	var hashBytes []byte

	err := w.query.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&hashBytes)
	}, "SELECT hash FROM schema_revision WHERE name = 'current'")
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", datastore.ErrSchemaNotFound
		}
		return "", fmt.Errorf("failed to query schema hash: %w", err)
	}

	return string(hashBytes), nil
}

var _ datastore.SingleStoreSchemaHashWatcher = (*crdbSchemaHashWatcher)(nil)
