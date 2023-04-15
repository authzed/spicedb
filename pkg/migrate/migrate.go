package migrate

import (
	"context"
	"fmt"
	"strings"

	log "github.com/authzed/spicedb/internal/logging"
)

const (
	Head = "head"
	None = ""
)

type RunType bool

var (
	DryRun  RunType = true
	LiveRun RunType = false
)

// Driver represents the common interface for enabling the orchestration of migrations
// for a specific type of datastore. The driver is parameterized with a type representing
// a connection handler that will be forwarded by the Manager to the MigrationFunc to execute.
type Driver[C any, T any] interface {
	// Version returns the current version of the schema in the backing datastore.
	// If the datastore is brand new, version should return the empty string without
	// an error.
	Version(ctx context.Context) (string, error)

	// WriteVersion stores the migration version being run
	WriteVersion(ctx context.Context, tx T, version string, replaced string) error

	// Conn returns the drivers underlying connection handler to be used by one or more MigrationFunc
	Conn() C

	// RunTx returns a transaction for to be used by one or more TxMigrationFunc
	RunTx(context.Context, TxMigrationFunc[T]) error

	// Close frees up any resources in use by the driver.
	Close(ctx context.Context) error
}

// MigrationFunc is a function that executes in the context of a specific database connection handler.
type MigrationFunc[C any] func(ctx context.Context, conn C) error

// TxMigrationFunc is a function that executes in the context of a specific database transaction.
type TxMigrationFunc[T any] func(ctx context.Context, tx T) error

type migration[C any, T any] struct {
	version  string
	replaces string
	up       MigrationFunc[C]
	upTx     TxMigrationFunc[T]
}

// Manager is used to manage a self-contained set of migrations. Standard usage
// would be to instantiate one at the package level for a particular application
// and then statically register migrations to the single instantiation in init
// functions.
// The manager is parameterized using the Driver interface along the concrete type of
// a database connection handler. This makes it possible for MigrationFunc to run without
// having to abstract each connection handler behind a common interface.
type Manager[D Driver[C, T], C any, T any] struct {
	migrations map[string]migration[C, T]
}

// NewManager creates a new empty instance of a migration manager.
func NewManager[D Driver[C, T], C any, T any]() *Manager[D, C, T] {
	return &Manager[D, C, T]{migrations: make(map[string]migration[C, T])}
}

// Register is used to associate a single migration with the migration engine.
// The up parameter should be a function that performs the actual upgrade logic
// and which takes a pointer to a concrete implementation of the Driver
// interface as its only parameters, which will be passed directly from the Run
// method into the upgrade function. If not extra fields or data are required
// the function can alternatively take a Driver interface param.
func (m *Manager[D, C, T]) Register(version, replaces string, up MigrationFunc[C], upTx TxMigrationFunc[T]) error {
	if strings.ToLower(version) == Head {
		return fmt.Errorf("unable to register version called head")
	}

	if _, ok := m.migrations[version]; ok {
		return fmt.Errorf("revision already exists: %s", version)
	}

	m.migrations[version] = migration[C, T]{
		version:  version,
		replaces: replaces,
		up:       up,
		upTx:     upTx,
	}

	return nil
}

// Run will actually perform the necessary migrations to bring the backing datastore
// from its current revision to the specified revision.
func (m *Manager[D, C, T]) Run(ctx context.Context, driver D, throughRevision string, dryRun RunType) error {
	requestedRevision := throughRevision
	starting, err := driver.Version(ctx)
	if err != nil {
		return fmt.Errorf("unable to get current revision: %w", err)
	}

	if strings.ToLower(throughRevision) == Head {
		throughRevision, err = m.HeadRevision()
		if err != nil {
			return fmt.Errorf("unable to compute head revision: %w", err)
		}
	}

	toRun, err := collectMigrationsInRange(starting, throughRevision, m.migrations)
	if err != nil {
		return fmt.Errorf("unable to compute migration list: %w", err)
	}
	if len(toRun) == 0 {
		log.Ctx(ctx).Info().Str("targetRevision", requestedRevision).Msg("server already at requested revision")
	}

	if !dryRun {
		for _, migrationToRun := range toRun {
			// Double check that the current version reported is the one we expect
			currentVersion, err := driver.Version(ctx)
			if err != nil {
				return fmt.Errorf("unable to load version from driver: %w", err)
			}

			if migrationToRun.replaces != currentVersion {
				return fmt.Errorf("migration attempting to run out of order: %s != %s", currentVersion, migrationToRun.replaces)
			}

			log.Ctx(ctx).Info().Str("from", migrationToRun.replaces).Str("to", migrationToRun.version).Msg("migrating")
			if migrationToRun.up != nil {
				if err = migrationToRun.up(ctx, driver.Conn()); err != nil {
					return fmt.Errorf("error executing migration function: %w", err)
				}
			}

			migrationToRun := migrationToRun
			if err := driver.RunTx(ctx, func(ctx context.Context, tx T) error {
				if migrationToRun.upTx != nil {
					if err := migrationToRun.upTx(ctx, tx); err != nil {
						return err
					}
				}
				return driver.WriteVersion(ctx, tx, migrationToRun.version, migrationToRun.replaces)
			}); err != nil {
				return fmt.Errorf("error executing migration `%s`: %w", migrationToRun.version, err)
			}

			currentVersion, err = driver.Version(ctx)
			if err != nil {
				return fmt.Errorf("unable to load version from driver: %w", err)
			}
			if migrationToRun.version != currentVersion {
				return fmt.Errorf("the migration function succeeded, but the driver did not report the expected version: %s", migrationToRun.version)
			}
		}
	}

	return nil
}

func (m *Manager[D, C, T]) HeadRevision() (string, error) {
	candidates := make(map[string]struct{}, len(m.migrations))
	for candidate := range m.migrations {
		candidates[candidate] = struct{}{}
	}

	for _, eliminateReplaces := range m.migrations {
		delete(candidates, eliminateReplaces.replaces)
	}

	allHeads := make([]string, 0, len(candidates))
	for headRevision := range candidates {
		allHeads = append(allHeads, headRevision)
	}

	if len(allHeads) != 1 {
		return "", fmt.Errorf("multiple or zero head revisions found: %v", allHeads)
	}

	return allHeads[0], nil
}

func (m *Manager[D, C, T]) IsHeadCompatible(revision string) (bool, error) {
	headRevision, err := m.HeadRevision()
	if err != nil {
		return false, err
	}
	headMigration := m.migrations[headRevision]
	return revision == headMigration.version || revision == headMigration.replaces, nil
}

func collectMigrationsInRange[C any, T any](starting, through string, all map[string]migration[C, T]) ([]migration[C, T], error) {
	var found []migration[C, T]

	lookingForRevision := through
	for lookingForRevision != starting {
		foundMigration, ok := all[lookingForRevision]
		if !ok {
			return []migration[C, T]{}, fmt.Errorf("unable to find migration for revision: %s", lookingForRevision)
		}

		found = append([]migration[C, T]{foundMigration}, found...)
		lookingForRevision = foundMigration.replaces
	}

	return found, nil
}
