package migrations

import "fmt"

func createMysqlMigrationVersion(mysql *MysqlDriver) string {
	return fmt.Sprintf("CREATE TABLE %s (version_num VARCHAR(255) NOT NULL PRIMARY KEY);",
		mysql.mysqlMigrationVersionTable())
}

func insertEmptyVersion(mysql *MysqlDriver) string {
	return fmt.Sprintf("INSERT INTO %s (version_num) VALUES ('');",
		mysql.mysqlMigrationVersionTable())
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
