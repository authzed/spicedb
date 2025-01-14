package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/postgres/common"
)

const createIndexTemplate = `
CREATE INDEX CONCURRENTLY 
	%s
	ON
	%s`

const dropIndexTemplate = `
	DROP INDEX CONCURRENTLY IF EXISTS 
	%s;
`

const timeoutMessage = "This typically indicates that your database global statement_timeout needs to be increased and/or spicedb migrate command needs --migration-timeout increased (1h by default)"

// createIndexConcurrently creates an index concurrently, dropping the existing index if it exists to ensure
// that indexes are not left in a partially constructed state.
// See: https://www.shayon.dev/post/2024/225/stop-relying-on-if-not-exists-for-concurrent-index-creation-in-postgresql/
func createIndexConcurrently(ctx context.Context, conn *pgx.Conn, indexName, creationClause string) error {
	dropIndexSQL := fmt.Sprintf(dropIndexTemplate, indexName)
	if _, err := conn.Exec(ctx, dropIndexSQL); err != nil {
		if common.IsQueryCanceledError(err) {
			return fmt.Errorf(
				"timed out while trying to drop index %s before recreating it: %w. %s",
				indexName,
				err,
				timeoutMessage,
			)
		}

		return fmt.Errorf("failed to drop index %s before creating it: %w", indexName, err)
	}

	createIndexSQL := fmt.Sprintf(createIndexTemplate, indexName, creationClause)
	if _, err := conn.Exec(ctx, createIndexSQL); err != nil {
		if common.IsQueryCanceledError(err) {
			return fmt.Errorf(
				"timed out while trying to create index %s: %w. %s",
				indexName,
				err,
				timeoutMessage,
			)
		}

		return fmt.Errorf("failed to create index %s: %w", indexName, err)
	}
	return nil
}
