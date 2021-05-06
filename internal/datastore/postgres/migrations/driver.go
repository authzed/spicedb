package migrations

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

const errUnableToInstantiate = "unable to instantiate AlembicPostgresDriver: %w"

const postgresMissingTableErrorCode = "42P01"

type AlembicPostgresDriver struct {
	db *sqlx.DB
}

func NewAlembicPostgresDriver(url string) (*AlembicPostgresDriver, error) {
	connectStr, err := pq.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	db, err := sqlx.Connect("postgres", connectStr)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	return &AlembicPostgresDriver{db}, nil
}

func (apd *AlembicPostgresDriver) Version() (string, error) {
	var loaded string

	if err := apd.db.QueryRowx("SELECT version_num from alembic_version").Scan(&loaded); err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == postgresMissingTableErrorCode {
			return "", nil
		}
		return "", fmt.Errorf("unable to load alembic revision: %w", err)
	}

	return loaded, nil
}

func (apd *AlembicPostgresDriver) WriteVersion(version, replaced string) error {
	result, err := apd.db.Exec("UPDATE alembic_version SET version_num=$1 WHERE version_num=$2", version, replaced)
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
