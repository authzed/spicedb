package migrations

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/migrate"
)

const (
	errUnableToInstantiate = "unable to instantiate CRDBDriver: %w"

	postgresMissingTableErrorCode = "42P01"

	queryLoadVersion  = "SELECT version_num from schema_version"
	queryWriteVersion = "UPDATE schema_version SET version_num=$1 WHERE version_num=$2"
)

// CRDBDriver implements a schema migration facility for use in SpiceDB's CRDB
// datastore.
type CRDBDriver struct {
	db *pgx.Conn
}

// NewCRDBDriver creates a new driver with active connections to the database
// specified.
func NewCRDBDriver(url string) (*CRDBDriver, error) {
	connConfig, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}
	pgxcommon.ConfigurePGXLogger(connConfig)
	pgxcommon.ConfigureOTELTracer(connConfig, false)

	db, err := pgx.ConnectConfig(context.Background(), connConfig)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	return &CRDBDriver{db}, nil
}

// Version returns the version of the schema to which the connected database
// has been migrated.
func (apd *CRDBDriver) Version(ctx context.Context) (string, error) {
	var loaded string

	if err := apd.db.QueryRow(ctx, queryLoadVersion).Scan(&loaded); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == postgresMissingTableErrorCode {
			return "", nil
		}
		return "", fmt.Errorf("unable to load alembic revision: %w", err)
	}

	return loaded, nil
}

// Conn returns the underlying pgx.Conn instance for this driver
func (apd *CRDBDriver) Conn() *pgx.Conn {
	return apd.db
}

func (apd *CRDBDriver) RunTx(ctx context.Context, f migrate.TxMigrationFunc[pgx.Tx]) error {
	return pgx.BeginFunc(ctx, apd.db, func(tx pgx.Tx) error {
		return f(ctx, tx)
	})
}

// Close disposes the driver.
func (apd *CRDBDriver) Close(ctx context.Context) error {
	return apd.db.Close(ctx)
}

func (apd *CRDBDriver) WriteVersion(ctx context.Context, tx pgx.Tx, version, replaced string) error {
	result, err := tx.Exec(ctx, queryWriteVersion, version, replaced)
	if err != nil {
		return fmt.Errorf("unable to update version row: %w", err)
	}

	updatedCount := result.RowsAffected()
	if updatedCount != 1 {
		return fmt.Errorf("writing version update affected %d rows, should be 1", updatedCount)
	}

	return nil
}

var _ migrate.Driver[*pgx.Conn, pgx.Tx] = &CRDBDriver{}
