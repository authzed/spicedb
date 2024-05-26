package migrations

import (
	"context"
	"fmt"
	"regexp"

	"github.com/Masterminds/semver"
	"github.com/jackc/pgx/v5"
)

const (
	// ttl_expiration_expression support was added in CRDB v22.2, but the E2E tests
	// use v21.2.
	addTransactionMetadataTableQueryWithBasicTTL = `
		CREATE TABLE transaction_metadata (
			key UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			expires_at TIMESTAMPTZ,
			metadata JSONB
		) WITH (ttl_expire_after = '1d');
	`

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
	err := CRDBMigrations.Register("add-transaction-metadata-table", "add-integrity-relationtuple-table", addTransactionMetadataTable, noAtomicMigration)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

func addTransactionMetadataTable(ctx context.Context, conn *pgx.Conn) error {
	row := conn.QueryRow(ctx, "select version()")
	var fullVersionString string
	if err := row.Scan(&fullVersionString); err != nil {
		return err
	}

	re, err := regexp.Compile(semver.SemVerRegex)
	if err != nil {
		return fmt.Errorf("failed to compile regex: %w", err)
	}

	version := re.FindString(fullVersionString)
	v, err := semver.NewVersion(version)
	if err != nil {
		return fmt.Errorf("failed to parse version %q: %w", version, err)
	}

	if v.Major() < 22 {
		return fmt.Errorf("unsupported version %q", version)
	}

	// v22.1 doesn't support `ttl_expiration_expression`; it was added in v22.2.
	if v.Major() == 22 && v.Minor() == 1 {
		_, err := conn.Exec(ctx, addTransactionMetadataTableQueryWithBasicTTL)
		return err
	}

	// `ttl_disable_changefeed_replication`	was added in v24.
	if v.Major() < 24 {
		_, err := conn.Exec(ctx, addTransactionMetadataTableQuery)
		return err
	}

	// v24 and later
	_, err = conn.Exec(ctx, addTransactionMetadataTableQueryWithTTLIgnore)
	return err
}
