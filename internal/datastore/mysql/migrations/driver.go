package migrations

import (
	"database/sql"
	"errors"
	"fmt"

	sqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

const (
	errUnableToInstantiate       = "unable to instantiate MysqlDriver: %w"
	mysqlMissingTableErrorNumber = 1146
)

type MysqlDriver struct {
	db *sqlx.DB
}

// NewMysqlDriver creates a new driver with active connections to the database specified.
func NewMysqlDriver(url string) (*MysqlDriver, error) {
	dbConfig, err := sqlDriver.ParseDSN(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	db, err := sqlx.Connect("mysql", dbConfig.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}
	return &MysqlDriver{db}, nil
}

// Version returns the version of the schema to which the connected database
// has been migrated.
func (mysql *MysqlDriver) Version() (string, error) {
	var loaded string

	if err := mysql.db.QueryRowx("SELECT version_num FROM mysql_migration_version").Scan(&loaded); err != nil {
		var mysqlError *sqlDriver.MySQLError
		if errors.As(err, &mysqlError) && mysqlError.Number == mysqlMissingTableErrorNumber {
			return "", nil
		}
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("unable to load mysql migration revision: %w", err)
	}

	return loaded, nil
}

// WriteVersion overwrites the value stored to track the version of the
// database schema.
func (mysql *MysqlDriver) WriteVersion(version, replaced string) error {
	updateSQL := "UPDATE mysql_migration_version SET version_num=? WHERE version_num=?;"

	result, err := mysql.db.Exec(updateSQL, version, replaced)
	if err != nil {
		return fmt.Errorf("unable to update version row: %w", err)
	}

	updatedCount, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("unable to compute number of rows affected: %w", err)
	}

	if updatedCount != 1 {
		return fmt.Errorf("writing version update affected %d rows, should be 1", updatedCount)
	}

	return nil
}

func (mysql *MysqlDriver) Dispose() {
	mysql.db.Close()
}
