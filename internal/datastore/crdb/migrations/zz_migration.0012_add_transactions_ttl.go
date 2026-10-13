package migrations

import (
	"context"
	"fmt"
	"regexp"

	"github.com/Masterminds/semver"
	"github.com/jackc/pgx/v5"
)

const (
	// Overlay keys in the transactions table only need to exist long enough to
	// force concurrent writes to overlap (new-enemy protection). After 24h they
	// are unused; without TTL they accumulate forever, especially with the
	// `request` overlap strategy where clients can introduce arbitrary keys.
	//
	// ttl_expire_after refreshes crdb_internal_expiration on INSERT and UPDATE,
	// so touching a key (ON CONFLICT DO UPDATE SET timestamp = now()) resets
	// the TTL. See https://github.com/authzed/spicedb/issues/2507.
	transactionsTTLDuration = "24 hours"

	// ttl_expire_after was available when row-level TTL shipped in v22.1.
	addTransactionsTTLQueryBasic = `
		ALTER TABLE transactions SET (ttl_expire_after = '` + transactionsTTLDuration + `');
	`

	// ttl_job_cron was added alongside ttl_expiration_expression in v22.2.
	addTransactionsTTLQuery = `
		ALTER TABLE transactions SET (ttl_expire_after = '` + transactionsTTLDuration + `', ttl_job_cron = '@hourly');
	`

	// ttl_disable_changefeed_replication was added in v24. The Watch API does
	// not subscribe to this table, but operators running their own changefeeds
	// should not see TTL deletes as relationship-change noise.
	addTransactionsTTLQueryWithTTLIgnore = `
		ALTER TABLE transactions SET (ttl_expire_after = '` + transactionsTTLDuration + `', ttl_job_cron = '@hourly', ttl_disable_changefeed_replication = 'true');
	`
)

func init() {
	err := CRDBMigrations.Register("add-transactions-ttl", "populate-schema-tables", addTransactionsTTL, noAtomicMigration)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

func addTransactionsTTL(ctx context.Context, conn *pgx.Conn) error {
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

	sql, err := transactionsTTLAlterSQL(v)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, sql)
	return err
}

// transactionsTTLAlterSQL returns the ALTER TABLE statement that enables
// row-level TTL on the overlay-key transactions table for the given CRDB version.
func transactionsTTLAlterSQL(v *semver.Version) (string, error) {
	if v.Major() < 22 {
		return "", fmt.Errorf("unsupported version %q", v)
	}

	// v22.1 doesn't support ttl_job_cron; it was added in v22.2.
	if v.Major() == 22 && v.Minor() == 1 {
		return addTransactionsTTLQueryBasic, nil
	}

	// ttl_disable_changefeed_replication was added in v24.
	if v.Major() < 24 {
		return addTransactionsTTLQuery, nil
	}

	return addTransactionsTTLQueryWithTTLIgnore, nil
}
