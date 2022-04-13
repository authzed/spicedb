package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	sqlDriver "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
)

const (
	errUnableToInstantiate       = "unable to instantiate MySQLDriver: %w"
	mysqlMissingTableErrorNumber = 1146

	migrationVersionColumnPrefix = "_meta_version_"
)

var sb = sq.StatementBuilder.PlaceholderFormat(sq.Question)

type MySQLDriver struct {
	db *sql.DB
	*tables
}

// NewMySQLDriverFromDSN creates a new migration driver with a connection pool to the database DSN specified.
//
// URI: [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...
// See // https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html
func NewMySQLDriverFromDSN(url string, tablePrefix string) (*MySQLDriver, error) {
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
	return NewMySQLDriverFromDB(db, tablePrefix), nil
}

// NewMySQLDriverFromDB creates a new migration driver with a connection pool specified upfront.
func NewMySQLDriverFromDB(db *sql.DB, tablePrefix string) *MySQLDriver {
	return &MySQLDriver{db, newTables(tablePrefix)}
}

func revisionToColumnName(revision string) string {
	return migrationVersionColumnPrefix + revision
}

func columnNameToRevision(columnName string) (string, bool) {
	if !strings.HasPrefix(columnName, migrationVersionColumnPrefix) {
		return "", false
	}
	return strings.TrimPrefix(columnName, migrationVersionColumnPrefix), true
}

// Version returns the version of the schema to which the connected database
// has been migrated.
func (driver *MySQLDriver) Version() (string, error) {
	query, args, err := sb.Select("*").From(driver.migrationVersion()).ToSql()
	if err != nil {
		return "", fmt.Errorf("unable to load driver migration revision: %w", err)
	}

	rows, err := driver.db.Query(query, args...)
	if err != nil {
		var mysqlError *sqlDriver.MySQLError
		if errors.As(err, &mysqlError) && mysqlError.Number == mysqlMissingTableErrorNumber {
			return "", nil
		}
		return "", fmt.Errorf("unable to load driver migration revision: %w", err)
	}
	defer LogOnError(context.Background(), rows.Close)
	if rows.Err() != nil {
		return "", fmt.Errorf("unable to load driver migration revision: %w", err)
	}
	cols, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("failed to get columns: %w", err)
	}

	for _, col := range cols {
		if revision, ok := columnNameToRevision(col); ok {
			return revision, nil
		}
	}
	return "", errors.New("no migration version detected")
}

// WriteVersion overwrites the _meta_version_ column name which encodes the version
// of the database schema.
func (driver *MySQLDriver) WriteVersion(version, replaced string) error {
	stmt := fmt.Sprintf("ALTER TABLE %s CHANGE %s %s VARCHAR(255) NOT NULL",
		driver.migrationVersion(),
		revisionToColumnName(replaced),
		revisionToColumnName(version),
	)
	if _, err := driver.db.Exec(stmt); err != nil {
		return fmt.Errorf("unable to version: %w", err)
	}

	return nil
}

func (driver *MySQLDriver) Close() error {
	return driver.db.Close()
}

// LogOnError executes the function and logs the error.
// Useful to avoid silently ignoring errors in defer statements
func LogOnError(ctx context.Context, f func() error) {
	if err := f(); err != nil {
		log.Ctx(ctx).Error().Err(err)
	}
}
