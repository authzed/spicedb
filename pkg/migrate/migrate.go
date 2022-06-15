package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
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

// Driver represents the common interface for enabling the orchestreation of migrations
// for a specific type of datastore. The driver is parameterized with a type representing
// a connection handler that will be forwarded by the Manager to the MigrationFunc to execute.
type Driver[T any] interface {
	// Version returns the current version of the schema in the backing datastore.
	// If the datastore is brand new, version should return the empty string without
	// an error.
	Version(ctx context.Context) (string, error)

	// Conn returns the drivers underlying connection handler to be used by one or more MigrationFunc
	Conn() T

	// Close frees up any resources in use by the driver.
	Close(ctx context.Context) error
}

// MigrationFunc is a function that executes in the context of a specific database connection handler.
// The version string represents the version of this migration to run, and replaced
// represents the previous version that is being superseded.
type MigrationFunc[T any] func(ctx context.Context, conn T, version, replaced string) error

type migration[T any] struct {
	version  string
	replaces string
	up       MigrationFunc[T]
}

// Manager is used to manage a self-contained set of migrations. Standard usage
// would be to instantiate one at the package level for a particular application
// and then statically register migrations to the single instantiation in init
// functions.
// The manager is parameterized using the Driver interface along the concrete type of
// a database connection handler. This makes it possible for MigrationFunc to run without
// having to abstract each connection handler behind a common interface.
type Manager[D Driver[T], T any] struct {
	migrations map[string]migration[T]
}

// NewManager creates a new empty instance of a migration manager.
func NewManager[D Driver[T], T any]() *Manager[D, T] {
	return &Manager[D, T]{migrations: make(map[string]migration[T])}
}

// Register is used to associate a single migration with the migration engine.
// The up parameter should be a function that performs the actual upgrade logic
// and which takes a pointer to a concrete implementation of the Driver
// interface as its only parameters, which will be passed directly from the Run
// method into the upgrade function. If not extra fields or data are required
// the function can alternatively take a Driver interface param.
func (m *Manager[D, T]) Register(version, replaces string, up MigrationFunc[T]) error {
	if strings.ToLower(version) == Head {
		return fmt.Errorf("unable to register version called head")
	}

	if _, ok := m.migrations[version]; ok {
		return fmt.Errorf("revision already exists: %s", version)
	}

	m.migrations[version] = migration[T]{
		version:  version,
		replaces: replaces,
		up:       up,
	}

	return nil
}

// Run will actually perform the necessary migrations to bring the backing datastore
// from its current revision to the specified revision.
func (m *Manager[D, T]) Run(ctx context.Context, driver D, throughRevision string, dryRun RunType) error {
	starting, err := driver.Version(ctx)
	if err != nil {
		return fmt.Errorf("unable to compute target revision: %w", err)
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

			log.Info().Str("from", migrationToRun.replaces).Str("to", migrationToRun.version).Msg("migrating")
			err = migrationToRun.up(ctx, driver.Conn(), migrationToRun.version, migrationToRun.replaces)
			if err != nil {
				return fmt.Errorf("error executing migration function: %w", err)
			}
		}
	}

	return nil
}

func (m *Manager[D, T]) HeadRevision() (string, error) {
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

func (m *Manager[D, T]) IsHeadCompatible(revision string) (bool, error) {
	headRevision, err := m.HeadRevision()
	if err != nil {
		return false, err
	}
	headMigration := m.migrations[headRevision]
	return revision == headMigration.version || revision == headMigration.replaces, nil
}

func collectMigrationsInRange[T any](starting, through string, all map[string]migration[T]) ([]migration[T], error) {
	var found []migration[T]

	lookingForRevision := through
	for lookingForRevision != starting {
		foundMigration, ok := all[lookingForRevision]
		if !ok {
			return []migration[T]{}, fmt.Errorf("unable to find migration for revision: %s", lookingForRevision)
		}

		found = append([]migration[T]{foundMigration}, found...)
		lookingForRevision = foundMigration.replaces
	}

	return found, nil
}
