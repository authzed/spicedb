package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"

	sqlDriver "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
)

const (
	errUnableToInstantiate       = "unable to instantiate MysqlDriver: %w"
	mysqlMissingTableErrorNumber = 1146

	migrationVersionColumnPrefix = "_meta_version_"
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

func revisionToColumnName(revision string) string {
	return migrationVersionColumnPrefix + revision
}

func columnNameToRevision(columnName string) string {
	return strings.TrimPrefix(columnName, migrationVersionColumnPrefix)
}

// Version returns the version of the schema to which the connected database
// has been migrated.
func (mysql *MysqlDriver) Version() (string, error) {
	query, args, err := sb.Select("*").From(mysql.mysqlMigrationVersionTable()).ToSql()
	if err != nil {
		return "", fmt.Errorf("unable to load mysql migration revision: %w", err)
	}

	rows, err := mysql.db.Query(query, args...)
	if err != nil {
		var mysqlError *sqlDriver.MySQLError
		if errors.As(err, &mysqlError) && mysqlError.Number == mysqlMissingTableErrorNumber {
			return "", nil
		}
		return "", fmt.Errorf("unable to load mysql migration revision: %w", err)
	}
	if rows.Err() != nil {
		return "", fmt.Errorf("unable to load mysql migration revision: %w", err)
	}
	cols, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("failed to get columns: %w", err)
	}

	return columnNameToRevision(cols[0]), nil
}

// WriteVersion overwrites the _meta_version_ column name which encodes the version
// of the database schema.
func (mysql *MysqlDriver) WriteVersion(version, replaced string) error {
	stmt := fmt.Sprintf("ALTER TABLE %s CHANGE %s %s VARCHAR(255) NOT NULL",
		mysql.mysqlMigrationVersionTable(),
		revisionToColumnName(replaced),
		revisionToColumnName(version),
	)
	if _, err := mysql.db.Exec(stmt); err != nil {
		return fmt.Errorf("unable to version: %w", err)
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
