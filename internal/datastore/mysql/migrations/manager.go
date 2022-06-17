package migrations

import (
	"database/sql"
	"fmt"
	"regexp"

	"github.com/authzed/spicedb/pkg/migrate"
)

const migrationNamePattern = `^[_a-zA-Z]*$`

var (
	noNonatomicMigration migrate.MigrationFunc[Wrapper]
	noTxMigration        migrate.TxMigrationFunc[TxWrapper] // nolint: deadcode, unused, varcheck

	migrationNameRe = regexp.MustCompile(migrationNamePattern)

	// Manager is the singleton migration manager instance for MySQL
	Manager = migrate.NewManager[*MySQLDriver, Wrapper, TxWrapper]()
)

// Wrapper makes it possible to forward the table schema needed for MySQL MigrationFunc to run
type Wrapper struct {
	db     *sql.DB
	tables *tables
}

// TxWrapper makes it possible to forward the table schema to a transactional migration func.
type TxWrapper struct {
	tx     *sql.Tx
	tables *tables
}

func mustRegisterMigration(version, replaces string, up migrate.MigrationFunc[Wrapper], upTx migrate.TxMigrationFunc[TxWrapper]) {
	if err := registerMigration(version, replaces, up, upTx); err != nil {
		panic("failed to register migration  " + err.Error())
	}
}

func registerMigration(version, replaces string, up migrate.MigrationFunc[Wrapper], upTx migrate.TxMigrationFunc[TxWrapper]) error {
	// validate migration names to ensure they are compatible with mysql column names
	for _, v := range []string{version, replaces} {
		if match := migrationNameRe.MatchString(version); !match {
			return fmt.Errorf("migration from '%s' to '%s': '%s' is an invalid mysql migration version, expected to match pattern '%s'",
				replaces, version, v, migrationNamePattern,
			)
		}
	}

	// register the migration
	return Manager.Register(version, replaces, up, upTx)
}
