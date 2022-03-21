package migration

import (
	"fmt"

	"github.com/rs/zerolog/log"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	spannermigrations "github.com/authzed/spicedb/internal/datastore/spanner/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
)

//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . Config
type Config struct {
	DatastoreURI       string
	DatastoreEngine    string
	SpannerCredentials string
}

func (c *Config) Complete() (Migrator, error) {
	m := migrator{}

	switch c.DatastoreEngine {
	case "cockroachdb":
		log.Info().Msg("migrating cockroachdb datastore")

		var err error
		m.driver, err = crdbmigrations.NewCRDBDriver(c.DatastoreURI)
		if err != nil {
			return nil, fmt.Errorf("unable to create migration driver: %w", err)
		}
		m.manager = crdbmigrations.CRDBMigrations
	case "postgres":
		log.Info().Msg("migrating postgres datastore")

		var err error
		m.driver, err = migrations.NewAlembicPostgresDriver(c.DatastoreURI)
		if err != nil {
			return nil, fmt.Errorf("unable to create migration driver: %w", err)
		}
		m.manager = migrations.DatabaseMigrations
	case "spanner":
		log.Info().Msg("migrating spanner datastore")

		if c.SpannerCredentials == "" {
			return nil, fmt.Errorf("no credentials for spanner")
		}
		var err error
		m.driver, err = spannermigrations.NewSpannerDriver(c.DatastoreURI, c.SpannerCredentials)
		if err != nil {
			return nil, fmt.Errorf("unable to create migration driver: %w", err)
		}
		m.manager = spannermigrations.SpannerMigrations
	default:
		return nil, fmt.Errorf("cannot migrate datastore engine type: %s", c.DatastoreEngine)
	}
	return &m, nil
}

type Migrator interface {
	Migrate(revision string) error
	AtHead() (bool, error)
	MigrateHead() error
	HeadRevision() (string, error)
}

type migrator struct {
	driver  migrate.Driver
	manager *migrate.Manager
}

var _ Migrator = &migrator{}

func (m *migrator) Migrate(revision string) error {
	log.Info().Str("targetRevision", revision).Msg("running migrations")
	if err := m.manager.Run(m.driver, revision, migrate.LiveRun); err != nil {
		return fmt.Errorf("unable to complete requested migrations: %w", err)
	}

	if err := m.driver.Close(); err != nil {
		return fmt.Errorf("unable to close migration driver: %w", err)
	}
	return nil
}

func (m *migrator) AtHead() (bool, error) {
	version, err := m.driver.Version()
	if err != nil {
		return false, err
	}
	head, err := m.manager.HeadRevision()
	if err != nil {
		return false, err
	}
	return version == head, nil
}

func (m *migrator) HeadRevision() (string, error) {
	return m.manager.HeadRevision()
}

func (m *migrator) MigrateHead() error {
	return m.Migrate(migrate.Head)
}
