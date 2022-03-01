package migrations

import "fmt"

func createMysqlMigrationVersion(mysql *MysqlDriver) string {
	return fmt.Sprintf("CREATE TABLE %smysql_migration_version (version_num VARCHAR(255) NOT NULL PRIMARY KEY);",
		mysql.tablePrefix)
}

func insertEmptyVersion(mysql *MysqlDriver) string {
	return fmt.Sprintf("INSERT INTO %smysql_migration_version (version_num) VALUES ('');",
		mysql.tablePrefix)
}

func init() {
	err := Manager.Register("initial", "",
		newExecutor(
			createMysqlMigrationVersion,
			insertEmptyVersion,
		).migrate,
	)
	if err != nil {
		panic("failed to register migration  " + err.Error())
	}
}
