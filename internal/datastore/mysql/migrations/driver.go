package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"

	sqlDriver "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
)

const (
	errUnableToInstantiate       = "unable to instantiate MysqlDriver: %w"
	mysqlMissingTableErrorNumber = 1146
)

var sb = sq.StatementBuilder.PlaceholderFormat(sq.Question)

type MysqlDriver struct {
	db          *sql.DB
	tablePrefix string
}

// https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html
// URI: [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...

/* scheme: The transport protocol to use. Use mysqlx for X Protocol connections and mysql for classic
   MySQL protocol connections. If no protocol is specified, the server attempts to guess the protocol.
   Connectors that support DNS SRV can use the mysqlx+srv scheme (see Connections Using DNS SRV Records). */
// schema: The default database for the connection. If no database is specified, the connection has no default database.

// NewMysqlDriver creates a new driver with active connections to the database specified.
func NewMysqlDriver(url string, tablePrefix string) (*MysqlDriver, error) {
	// TODO: we're currently using a DSN here, not a URI
	dbConfig, err := sqlDriver.ParseDSN(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	db, err := sql.Open("mysql", dbConfig.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}
	err = sqlDriver.SetLogger(&log.Logger)
	if err != nil {
		return nil, fmt.Errorf("unable to set logging to mysql driver: %w", err)
	}
	return &MysqlDriver{db, tablePrefix}, nil
}

// Version returns the version of the schema to which the connected database
// has been migrated.
func (mysql *MysqlDriver) Version() (string, error) {
	var loaded string

	query, args, err := sb.Select("version_num").From(mysql.mysqlMigrationVersionTable()).ToSql()
	if err != nil {
		return "", fmt.Errorf("unable to load mysql migration revision: %w", err)
	}

	if err := mysql.db.QueryRow(query, args...).Scan(&loaded); err != nil {
		var mysqlError *sqlDriver.MySQLError
		if errors.As(err, &mysqlError) && mysqlError.Number == mysqlMissingTableErrorNumber {
			return "", nil
		}
		return "", fmt.Errorf("unable to load mysql migration revision: %w", err)
	}

	return loaded, nil
}

// WriteVersion overwrites the value stored to track the version of the
// database schema.
func (mysql *MysqlDriver) WriteVersion(version, replaced string) error {
	updateSQL, args, err := sb.Update(mysql.mysqlMigrationVersionTable()).Set("version_num", version).Where("version_num = ?", replaced).ToSql()
	if err != nil {
		return fmt.Errorf("unable to update version row: %w", err)
	}

	result, err := mysql.db.Exec(updateSQL, args...)
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
	defer common.LogOnError(context.Background(), mysql.db.Close)
}

func (mysql *MysqlDriver) mysqlMigrationVersionTable() string {
	return fmt.Sprintf("%s%s", mysql.tablePrefix, "mysql_migration_version")
}

func (mysql *MysqlDriver) tableTransaction() string {
	return fmt.Sprintf("%s%s", mysql.tablePrefix, common.TableTransactionDefault)
}

func (mysql *MysqlDriver) tableTuple() string {
	return fmt.Sprintf("%s%s", mysql.tablePrefix, common.TableTupleDefault)
}

func (mysql *MysqlDriver) tableNamespace() string {
	return fmt.Sprintf("%s%s", mysql.tablePrefix, common.TableNamespaceDefault)
}
