package migrations

import "fmt"

func createMysqlMigrationVersion(mysql *MysqlDriver) string {
	return fmt.Sprintf("CREATE TABLE %s (_meta_version_ VARCHAR(255) NOT NULL PRIMARY KEY);",
		mysql.mysqlMigrationVersionTable())
}

func init() {
	mustRegisterMigration("initial", "",
		newExecutor(
			createMysqlMigrationVersion,
		).migrate,
	)
}
