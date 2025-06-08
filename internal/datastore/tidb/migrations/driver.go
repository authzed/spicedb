package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	sqlDriver "github.com/go-sql-driver/mysql"

	"github.com/authzed/spicedb/internal/datastore/common"
	tidbCommon "github.com/authzed/spicedb/internal/datastore/tidb/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

const (
	errUnableToInstantiate      = "unable to instantiate TiDBDriver: %w"
	tidbMissingTableErrorNumber = 1146 // TiDB uses the same error number for missing table

	migrationVersionColumnPrefix = "_meta_version_"
)

var sb = sq.StatementBuilder.PlaceholderFormat(sq.Question)

// TiDBDriver is an implementation of migrate.Driver for TiDB
type TiDBDriver struct {
	db *sql.DB
	*tables
}

// NewTiDBDriverFromDSN creates a new migration driver with a connection pool to the database DSN specified.
//
// URI: [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...
// See https://docs.pingcap.com/tidb/stable/dev-guide-connecting-to-tidb
func NewTiDBDriverFromDSN(url string, tablePrefix string, credentialsProvider datastore.CredentialsProvider) (*TiDBDriver, error) {
	dbConfig, err := sqlDriver.ParseDSN(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	err = tidbCommon.MaybeAddCredentialsProviderHook(dbConfig, credentialsProvider)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	// Call NewConnector with the existing parsed configuration to preserve the BeforeConnect added by the CredentialsProvider
	connector, err := sqlDriver.NewConnector(dbConfig)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	db := sql.OpenDB(connector)
	return NewTiDBDriverFromDB(db, tablePrefix), nil
}

// NewTiDBDriverFromDB creates a new migration driver with a connection pool specified upfront.
func NewTiDBDriverFromDB(db *sql.DB, tablePrefix string) *TiDBDriver {
	return &TiDBDriver{db, newTables(tablePrefix)}
}

// revisionToColumnName generates the column name that will denote a given migration revision
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
func (driver *TiDBDriver) Version(ctx context.Context) (string, error) {
	query, args, err := sb.Select("*").From(driver.migrationVersion()).ToSql()
	if err != nil {
		return "", fmt.Errorf("unable to generate query for revision: %w", err)
	}

	rows, err := driver.db.QueryContext(ctx, query, args...)
	if err != nil {
		var tidbError *sqlDriver.MySQLError
		if errors.As(err, &tidbError) && tidbError.Number == tidbMissingTableErrorNumber {
			return "", nil
		}
		return "", fmt.Errorf("unable to query revision: %w", err)
	}
	defer common.LogOnError(ctx, rows.Close)
	if rows.Err() != nil {
		return "", fmt.Errorf("unable to load revision row: %w", rows.Err())
	}
	cols, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("failed to get columns from revision row: %w", err)
	}

	for _, col := range cols {
		if revision, ok := columnNameToRevision(col); ok {
			return revision, nil
		}
	}
	return "", errors.New("no migration version detected")
}

func (driver *TiDBDriver) Conn() Wrapper {
	return Wrapper{db: driver.db, tables: driver.tables}
}

func (driver *TiDBDriver) RunTx(ctx context.Context, f migrate.TxMigrationFunc[TxWrapper]) error {
	return BeginTxFunc(
		ctx,
		driver.db,
		&sql.TxOptions{Isolation: sql.LevelSerializable},
		func(tx *sql.Tx) error {
			return f(ctx, TxWrapper{tx, driver.tables})
		},
	)
}

// BeginTxFunc is a polyfill for database/sql which implements a closure style transaction lifecycle.
// The underlying transaction is aborted if the supplied function returns an error.
// The underlying transaction is committed if the supplied function returns nil.
func BeginTxFunc(ctx context.Context, db *sql.DB, txOptions *sql.TxOptions, f func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, txOptions)
	if err != nil {
		return err
	}

	if err := f(tx); err != nil {
		rerr := tx.Rollback()
		if rerr != nil {
			return errors.Join(err, rerr)
		}

		return err
	}

	return tx.Commit()
}

// WriteVersion overwrites the _meta_version_ column name which encodes the version
// of the database schema.
func (driver *TiDBDriver) WriteVersion(ctx context.Context, txWrapper TxWrapper, version, replaced string) error {
	stmt := fmt.Sprintf("ALTER TABLE %s CHANGE %s %s VARCHAR(255) NOT NULL",
		driver.migrationVersion(),
		revisionToColumnName(replaced),
		revisionToColumnName(version),
	)
	if _, err := txWrapper.tx.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("unable to write version: %w", err)
	}

	return nil
}

func (driver *TiDBDriver) Close(_ context.Context) error {
	return driver.db.Close()
}

var _ migrate.Driver[Wrapper, TxWrapper] = &TiDBDriver{}
