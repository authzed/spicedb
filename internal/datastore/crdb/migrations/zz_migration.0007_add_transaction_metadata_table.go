package migrations

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
)

const (
	addTransactionMetadataTableQuery = `
		CREATE TABLE transaction_metadata (
			key UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			expires_at TIMESTAMPTZ,
			metadata JSONB
		) WITH (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@daily');
	`

	// See: https://www.cockroachlabs.com/docs/stable/changefeed-messages#prevent-changefeeds-from-emitting-row-level-ttl-deletes
	// for why we set ttl_disable_changefeed_replication = 'true'. This isn't stricly necessary as the Watch API will ignore the
	// deletions of these metadata rows, but no reason to even have it in the changefeed.
	// NOTE: This only applies on CRDB v24 and later.
	addTransactionMetadataTableQueryWithTTLIgnore = `
		CREATE TABLE transaction_metadata (
			key UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			expires_at TIMESTAMPTZ,
			metadata JSONB
		) WITH (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@daily', ttl_disable_changefeed_replication = 'true');
	`
)

func init() {
	err := CRDBMigrations.Register("add-transaction-metadata-table", "add-relationship-counters-table", addTransactionMetadataTable, noAtomicMigration)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

func addTransactionMetadataTable(ctx context.Context, conn *pgx.Conn) error {
	row := conn.QueryRow(ctx, "select version()")
	var version string
	if err := row.Scan(&version); err != nil {
		return err
	}

	if strings.Contains(version, "v24.") {
		if _, err := conn.Exec(ctx, addTransactionMetadataTableQueryWithTTLIgnore); err != nil {
			return err
		}
		return nil
	}

	// CRDB v23 and earlier.
	if _, err := conn.Exec(ctx, addTransactionMetadataTableQuery); err != nil {
		return err
	}
	return nil
}
