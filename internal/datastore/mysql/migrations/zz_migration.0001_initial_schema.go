package migrations

import "fmt"

func createMysqlMigrationVersion(driver *MySQLDriver) string {
	// we need the additional primary key column because github.com/github/gh-ost requires a shared, not-null
	// key between the _to_ and _from_ table schemas to perform a schema migration.
	// -- https://github.com/github/gh-ost/blob/master/doc/shared-key.md
	return fmt.Sprintf("CREATE TABLE %s "+
		`( id int(11) NOT NULL PRIMARY KEY,
		_meta_version_ VARCHAR(255) NOT NULL
		);`,
		driver.migrationVersion())
}

func init() {
	mustRegisterMigration("initial", "",
		newExecutor(
			createMysqlMigrationVersion,
		).migrate,
	)
}
