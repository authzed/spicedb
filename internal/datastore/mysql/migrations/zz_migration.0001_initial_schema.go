package migrations

const createMysqlMigrationVersion = `CREATE TABLE mysql_migration_version (
	version_num VARCHAR(255) NOT NULL PRIMARY KEY
);`
const insertEmptyVersion = `INSERT INTO mysql_migration_version (version_num) VALUES ('');`

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
