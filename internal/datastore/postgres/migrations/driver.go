package migrations

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
)

const errUnableToInstantiate = "unable to instantiate AlembicPostgresDriver: %w"

const postgresMissingTableErrorCode = "42P01"

// AlembicPostgresDriver implements a schema migration facility for use in
// SpiceDB's Postgres datastore.
//
// It is compatible with the popular Python library, Alembic
type AlembicPostgresDriver struct {
	db *pgx.Conn
}

// NewAlembicPostgresDriver creates a new driver with active connections to the database specified.
func NewAlembicPostgresDriver(url string) (*AlembicPostgresDriver, error) {
	connectStr, err := pq.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	db, err := pgx.Connect(context.Background(), connectStr)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	return &AlembicPostgresDriver{db}, nil
}

// Conn returns the underlying pgx.Conn instance for this driver
func (apd *AlembicPostgresDriver) Conn() *pgx.Conn {
	return apd.db
}

// Version returns the version of the schema to which the connected database
// has been migrated.
func (apd *AlembicPostgresDriver) Version(ctx context.Context) (string, error) {
	var loaded string

	if err := apd.db.QueryRow(ctx, "SELECT version_num from alembic_version").Scan(&loaded); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == postgresMissingTableErrorCode {
			return "", nil
		}
		return "", fmt.Errorf("unable to load alembic revision: %w", err)
	}

	return loaded, nil
}

// Close disposes the driver.
func (apd *AlembicPostgresDriver) Close(ctx context.Context) error {
	return apd.db.Close(ctx)
}

func writeVersion(ctx context.Context, tx pgx.Tx, version, replaced string) error {
	result, err := tx.Exec(
		ctx,
		"UPDATE alembic_version SET version_num=$1 WHERE version_num=$2",
		version,
		replaced,
	)
	if err != nil {
		return fmt.Errorf("unable to update version row: %w", err)
	}

	updatedCount := result.RowsAffected()
	if updatedCount != 1 {
		return fmt.Errorf("writing version update affected %d rows, should be 1", updatedCount)
	}

	return nil
}

func commitWithMigrationVersion(ctx context.Context, conn *pgx.Conn, version, replaced string, f func(tx pgx.Tx) error) error {
	return conn.BeginFunc(ctx, func(tx pgx.Tx) error {
		err := f(tx)
		if err != nil {
			return err
		}
		return writeVersion(ctx, tx, version, replaced)
	})
}
