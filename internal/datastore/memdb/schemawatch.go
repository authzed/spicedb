package memdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
)

// memdbSchemaHashWatcher implements SingleStoreSchemaHashWatcher for memdb using polling.
// It queries only the in-memory schema revision table (not the full schema) for fast hash lookups.
type memdbSchemaHashWatcher struct {
	db *memdbDatastore
}

// newMemdbSchemaHashWatcher creates a new schema hash watcher for memdb.
func newMemdbSchemaHashWatcher(db *memdbDatastore) *memdbSchemaHashWatcher {
	return &memdbSchemaHashWatcher{db: db}
}

// WatchSchemaHash polls the in-memory schema revision table for changes to the schema hash.
func (w *memdbSchemaHashWatcher) WatchSchemaHash(ctx context.Context, refreshInterval time.Duration, callback datastore.SchemaWatchCallback) error {
	// Read initial hash from schema revision table
	lastHash, err := w.readSchemaHash()
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
			// Read current hash from schema revision table
			currentHash, err := w.readSchemaHash()
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
				return fmt.Errorf("failed to read schema hash: %w", err)
			}

			if currentHash != lastHash {
				lastHash = currentHash
				if err := callback(currentHash, nil); err != nil {
					return fmt.Errorf("callback error: %w", err)
				}
			}
		}
	}
}

// readSchemaHash reads the schema hash from the in-memory schema revision table.
func (w *memdbSchemaHashWatcher) readSchemaHash() (string, error) {
	w.db.RLock()
	defer w.db.RUnlock()

	tx := w.db.db.Txn(false)
	defer tx.Abort()

	raw, err := tx.First(tableSchemaRevision, indexID, "current")
	if err != nil {
		return "", fmt.Errorf("failed to query schema hash: %w", err)
	}

	if raw == nil {
		return "", datastore.ErrSchemaNotFound
	}

	revisionData, ok := raw.(*schemaRevisionData)
	if !ok {
		return "", errors.New("invalid schema revision data type")
	}

	return string(revisionData.hash), nil
}

var _ datastore.SingleStoreSchemaHashWatcher = (*memdbSchemaHashWatcher)(nil)
