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

	// ttl_disable_changefeed_replication keeps the deletes performed by the
	// row-level TTL job out of the changefeed that backs the Watch API. It was
	// added in v24.1, so older clusters get the policy without it and have the
	// parameter set for them at startup instead, by
	// ensureTTLChangefeedReplicationDisabled - which is also what sets it on
	// databases that ran this migration before it set the parameter.
	addExpirationPolicyWithTTLIgnore = `
		ALTER TABLE relation_tuple SET (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@daily', ttl_disable_changefeed_replication = 'true');

		ALTER TABLE relation_tuple_with_integrity SET (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@daily', ttl_disable_changefeed_replication = 'true');
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

	re := regexp.MustCompile(semver.SemVerRegex)
	version := re.FindString(fullVersionString)
	v, err := semver.NewVersion(version)
	if err != nil {
		return fmt.Errorf("failed to parse version %q: %w", version, err)
	}

	if v.Major() < 22 || (v.Major() == 22 && v.Minor() < 2) {
		return nil
	}

	// v24.1 and later: set ttl_disable_changefeed_replication as part of the
	// policy. Doing it here rather than leaving it to the startup check means a
	// freshly migrated database already has it, and the check becomes a no-op.
	if v.Major() > 24 || (v.Major() == 24 && v.Minor() >= 1) {
		_, err = conn.Exec(ctx, addExpirationPolicyWithTTLIgnore)
		return err
	}

	_, err = conn.Exec(ctx, addExpirationPolicy)
	return err
}
