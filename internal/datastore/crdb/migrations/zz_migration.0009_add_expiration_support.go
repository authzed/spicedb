package migrations

import (
	"context"
	"fmt"
	"regexp"

	"github.com/Masterminds/semver"
	"github.com/jackc/pgx/v5"
)

const (
	addExpirationColumnToRelationTuple = `
		ALTER TABLE relation_tuple
		ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ DEFAULT NULL;

		ALTER TABLE relation_tuple_with_integrity
		ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ DEFAULT NULL;
	`

	// ttl_expiration_expression support was added in CRDB v22.2.
	addExpirationPolicy = `
		ALTER TABLE relation_tuple SET (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@daily');

		ALTER TABLE relation_tuple_with_integrity SET (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@daily');
	`
)

func init() {
	err := CRDBMigrations.Register("add-expiration-support", "add-transaction-metadata-table", addExpirationSupport, noAtomicMigration)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

func addExpirationSupport(ctx context.Context, conn *pgx.Conn) error {
	// Add the expires_at column to relation_tuple.
	_, err := conn.Exec(ctx, addExpirationColumnToRelationTuple)
	if err != nil {
		return err
	}

	// Add the TTL policy to relation_tuple, if supported.
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

	if v.Major() < 22 || (v.Major() == 22 && v.Minor() < 2) {
		return nil
	}

	_, err = conn.Exec(ctx, addExpirationPolicy)
	return err
}
