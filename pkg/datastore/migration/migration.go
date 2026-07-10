// Package migration provides a registry of the datastore engines that can run
// schema migrations. It is shared by the `migrate` and `head` commands and by
// the generic datastore test suite. Engines register themselves via
// RegisterMigratableEngine from an init function; importing an engine package
// makes it migratable.
package migration

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

// Config holds configuration for running database migrations.
type Config struct {
	DatastoreEngine         string
	DatastoreURI            string
	CredentialsProviderName string
	SpannerCredentialsFile  string
	SpannerEmulatorHost     string
	MySQLTablePrefix        string
	Timeout                 time.Duration
	BatchSize               uint64

	// ExtraConfig holds the string value of every flag registered on the
	// command being run, keyed by flag name. Engines defined outside this
	// repository register their own flags on the migrate command and read them
	// from here, since this package has no dedicated field for them.
	ExtraConfig map[string]string
}

// CredentialsProvider returns the configured datastore credentials provider,
// or nil if none is configured.
func (cfg *Config) CredentialsProvider(ctx context.Context) (datastore.CredentialsProvider, error) {
	if cfg.CredentialsProviderName == "" {
		return nil, nil
	}
	return datastore.NewCredentialsProvider(ctx, cfg.CredentialsProviderName)
}

type migratableEngine struct {
	// verifiableMigrationName is the earliest migration at which the current
	// code can open the datastore and read and write data. If the current code
	// starts depending on a later migration, this must be advanced to it.
	verifiableMigrationName string

	headRevision   func() (string, error)
	migrationNames func() ([]string, error)
	version        func(ctx context.Context, cfg *Config) (string, error)
	migrate        func(ctx context.Context, cfg *Config, revision string) error
}

// migratableEngines holds the registered migratable engines, keyed by engine
// name. Engines register themselves via RegisterMigratableEngine from an init
// function; importing an engine package makes it migratable.
var migratableEngines = map[string]migratableEngine{}

// RegisterMigratableEngine makes a datastore engine migratable. It is
// typically called from an init function of the package defining the engine.
// verifiableMigrationName is the earliest migration at which the engine's
// current datastore code can open the datastore and read and write data.
func RegisterMigratableEngine[D migrate.Driver[C, T], C any, T any](
	engineName string,
	manager *migrate.Manager[D, C, T],
	newDriver func(ctx context.Context, cfg *Config) (D, error),
	verifiableMigrationName string,
) {
	migratableEngines[engineName] = newMigratableEngine(manager, newDriver, verifiableMigrationName)
}

// UnregisterMigratableEngineForTesting removes an engine from the registry.
// Tests that register a fake engine must call it so the engine does not leak
// into the registry observed by other tests in the same binary.
func UnregisterMigratableEngineForTesting(engineName string) {
	delete(migratableEngines, engineName)
}

// newMigratableEngine pairs a datastore engine's migration manager with the constructor for its migration driver
func newMigratableEngine[D migrate.Driver[C, T], C any, T any](
	manager *migrate.Manager[D, C, T],
	newDriver func(ctx context.Context, cfg *Config) (D, error),
	verifiableMigrationName string,
) migratableEngine {
	return migratableEngine{
		verifiableMigrationName: verifiableMigrationName,
		headRevision:            manager.HeadRevision,
		migrationNames:          manager.MigrationNames,
		version: func(ctx context.Context, cfg *Config) (string, error) {
			driver, err := newDriver(ctx, cfg)
			if err != nil {
				return "", fmt.Errorf("unable to create migration driver for %s: %w", cfg.DatastoreEngine, err)
			}
			defer func() { _ = driver.Close(ctx) }()
			return driver.Version(ctx)
		},
		migrate: func(ctx context.Context, cfg *Config, revision string) error {
			driver, err := newDriver(ctx, cfg)
			if err != nil {
				return fmt.Errorf("unable to create migration driver for %s: %w", cfg.DatastoreEngine, err)
			}
			log.Ctx(ctx).Info().Str("targetRevision", revision).Msg("running migrations")
			ctxWithBatch := context.WithValue(ctx, migrate.BackfillBatchSize, cfg.BatchSize)
			ctx, cancel := context.WithTimeout(ctxWithBatch, cfg.Timeout)
			defer cancel()
			if err := manager.Run(ctx, driver, revision, migrate.LiveRun); err != nil {
				return fmt.Errorf("unable to migrate to `%s` revision: %w", revision, err)
			}

			if err := driver.Close(ctx); err != nil {
				return fmt.Errorf("unable to close migration driver: %w", err)
			}
			return nil
		},
	}
}

// Run runs the migrations for the configured datastore engine up to the given
// revision.
func Run(ctx context.Context, cfg *Config, revision string) error {
	if revision == "" {
		return errors.New("missing required revision")
	}

	e, ok := migratableEngines[cfg.DatastoreEngine]
	if !ok {
		return fmt.Errorf("cannot migrate datastore engine type: %s", cfg.DatastoreEngine)
	}

	log.Ctx(ctx).Info().Str("engine", cfg.DatastoreEngine).Msg("migrating datastore")
	return e.migrate(ctx, cfg, revision)
}

// HeadRevision returns the latest migration revision for the given engine.
func HeadRevision(engine string) (string, error) {
	e, ok := migratableEngines[engine]
	if !ok {
		return "", fmt.Errorf("cannot migrate datastore engine type: %s", engine)
	}
	return e.headRevision()
}

// VerifiableMigrationName returns the earliest migration at which the given
// engine's current datastore code can open the datastore and read and write data.
func VerifiableMigrationName(engine string) (string, error) {
	e, ok := migratableEngines[engine]
	if !ok {
		return "", fmt.Errorf("cannot migrate datastore engine type: %s", engine)
	}
	return e.verifiableMigrationName, nil
}

// MigrationNames returns the names of every migration supported by the given
// engine, ordered from oldest to newest (head).
func MigrationNames(engine string) ([]string, error) {
	e, ok := migratableEngines[engine]
	if !ok {
		return nil, fmt.Errorf("cannot migrate datastore engine type: %s", engine)
	}
	return e.migrationNames()
}

// Version returns the migration revision the configured datastore is currently at.
func Version(ctx context.Context, cfg *Config) (string, error) {
	e, ok := migratableEngines[cfg.DatastoreEngine]
	if !ok {
		return "", fmt.Errorf("cannot migrate datastore engine type: %s", cfg.DatastoreEngine)
	}
	return e.version(ctx, cfg)
}

// Engines returns the names of all registered migratable engines, sorted.
func Engines() []string {
	engines := make([]string, 0, len(migratableEngines))
	for engine := range migratableEngines {
		engines = append(engines, engine)
	}
	slices.Sort(engines)
	return engines
}
