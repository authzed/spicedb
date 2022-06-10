package migrations

import (
	"database/sql"
	"fmt"
	"regexp"

	"github.com/authzed/spicedb/pkg/migrate"
)

const migrationNamePattern = `^[_a-zA-Z]*$`

var (
	migrationNameRe = regexp.MustCompile(migrationNamePattern)

	// Manager is the singleton migration manager instance for MySQL
	Manager = migrate.NewManager[*MySQLDriver, mysqlTx]()
)

type mySQLMigrationFunc = migrate.MigrationFunc[mysqlTx]

type mysqlTx struct {
	tx     *sql.Tx
	tables *tables
}

func mustRegisterMigration(version, replaces string, up mySQLMigrationFunc) {
	if err := registerMigration(version, replaces, up); err != nil {
		panic("failed to register migration  " + err.Error())
	}
}

func registerMigration(version, replaces string, up mySQLMigrationFunc) error {
	// validate migration names to ensure they are compatible with mysql column names
	for _, v := range []string{version, replaces} {
		if match := migrationNameRe.MatchString(version); !match {
			return fmt.Errorf("migration from '%s' to '%s': '%s' is an invalid mysql migration version, expected to match pattern '%s'",
				replaces, version, v, migrationNamePattern,
			)
		}
	}

	// register the migration
	return Manager.Register(version, replaces, up)
}
