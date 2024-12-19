package migrations

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"

	log "github.com/authzed/spicedb/internal/logging"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

const postgresMissingTableErrorCode = "42P01"

var tracer = otel.Tracer("spicedb/internal/datastore/common")

// AlembicPostgresDriver implements a schema migration facility for use in
// SpiceDB's Postgres datastore.
//
// It is compatible with the popular Python library, Alembic
type AlembicPostgresDriver struct {
	db *pgx.Conn
}

// NewAlembicPostgresDriver creates a new driver with active connections to the database specified.
func NewAlembicPostgresDriver(ctx context.Context, url string, credentialsProvider datastore.CredentialsProvider, includeQueryParametersInTraces bool) (*AlembicPostgresDriver, error) {
	ctx, span := tracer.Start(ctx, "NewAlembicPostgresDriver")
	defer span.End()

	connConfig, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	pgxcommon.ConfigurePGXLogger(connConfig)
	pgxcommon.ConfigureOTELTracer(connConfig, includeQueryParametersInTraces)

	if credentialsProvider != nil {
		log.Ctx(ctx).Debug().Str("name", credentialsProvider.Name()).Msg("using credentials provider")
		connConfig.User, connConfig.Password, err = credentialsProvider.Get(ctx, fmt.Sprintf("%s:%d", connConfig.Host, connConfig.Port), connConfig.User)
		if err != nil {
			return nil, err
		}
	}

	db, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	return &AlembicPostgresDriver{db}, nil
}

// Conn returns the underlying pgx.Conn instance for this driver
func (apd *AlembicPostgresDriver) Conn() *pgx.Conn {
	return apd.db
}

func (apd *AlembicPostgresDriver) RunTx(ctx context.Context, f migrate.TxMigrationFunc[pgx.Tx]) error {
	return pgx.BeginFunc(ctx, apd.db, func(tx pgx.Tx) error {
		return f(ctx, tx)
	})
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

func (apd *AlembicPostgresDriver) WriteVersion(ctx context.Context, tx pgx.Tx, version, replaced string) error {
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

var _ migrate.Driver[*pgx.Conn, pgx.Tx] = &AlembicPostgresDriver{}
