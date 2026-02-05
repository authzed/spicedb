package spanner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

// spannerSchemaHashWatcher implements SingleStoreSchemaHashWatcher for Spanner using polling.
// It queries only the schema_revision table (not the full schema table) for fast hash lookups.
type spannerSchemaHashWatcher struct {
	client *spanner.Client
}

// newSpannerSchemaHashWatcher creates a new schema hash watcher for Spanner.
func newSpannerSchemaHashWatcher(client *spanner.Client) *spannerSchemaHashWatcher {
	return &spannerSchemaHashWatcher{client: client}
}

// WatchSchemaHash polls the schema_revision table for changes to the schema hash.
func (w *spannerSchemaHashWatcher) WatchSchemaHash(ctx context.Context, refreshInterval time.Duration, callback datastore.SchemaWatchCallback) error {
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
			// Read current hash from schema_revision table
			currentHash, err := w.readSchemaHash(ctx)
			if err != nil {
				if errors.Is(err, datastore.ErrSchemaNotFound) {
					if lastHash != "" {
						lastHash = ""
						// Only read revision when hash changes
						currentRevision := revisions.NewForTime(time.Now())
						if err := callback("", currentRevision); err != nil {
							return fmt.Errorf("callback error: %w", err)
						}
					}
					continue
				}
				return fmt.Errorf("failed to read schema hash: %w", err)
			}

			// Only read revision if hash has changed
			if currentHash != lastHash {
				// Read current revision (commit timestamp)
				currentRevision := revisions.NewForTime(time.Now())

				lastHash = currentHash
				if err := callback(currentHash, currentRevision); err != nil {
					return fmt.Errorf("callback error: %w", err)
				}
			}
		}
	}
}

// readSchemaHash reads the schema hash from the schema_revision table.
func (w *spannerSchemaHashWatcher) readSchemaHash(ctx context.Context) (string, error) {
	txn := w.client.Single()
	defer txn.Close()

	iter := txn.Query(ctx, spanner.Statement{
		SQL: "SELECT schema_hash FROM schema_revision WHERE name = @name",
		Params: map[string]any{
			"name": "current",
		},
	})
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		if errors.Is(err, iterator.Done) {
			return "", datastore.ErrSchemaNotFound
		}
		return "", fmt.Errorf("failed to query schema hash: %w", err)
	}

	var hashBytes []byte
	if err := row.Columns(&hashBytes); err != nil {
		return "", fmt.Errorf("failed to scan schema hash: %w", err)
	}

	return string(hashBytes), nil
}

var _ datastore.SingleStoreSchemaHashWatcher = (*spannerSchemaHashWatcher)(nil)
