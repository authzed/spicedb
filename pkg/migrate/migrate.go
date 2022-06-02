package migrate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

// Driver represents the common interface for reading and writing the revision
// data from a migrateable backing datastore.
type Driver interface {
	// Version returns the current version of the schema in the backing datastore.
	// If the datastore is brand new, version should return the empty string without
	// an error.
	Version(ctx context.Context) (string, error)

	// WriteVersion records the newly migrated version to the backing datastore.
	WriteVersion(ctx context.Context, version, replaced string) error

	// Close frees up any resources in use by the driver.
	Close() error
}

type migration struct {
	version  string
	replaces string
	up       interface{}
}

// Manager is used to manage a self-contained set of migrations. Standard usage
// would be to instantiate one at the package level for a particular application
// and then statically register migrations to the single instantiation in init
// functions.
type Manager struct {
	migrations map[string]migration
}

// NewManager creates a new empty instance of a migration manager.
func NewManager() *Manager {
	return &Manager{migrations: make(map[string]migration)}
}

// Register is used to associate a single migration with the migration engine.
// The up parameter should be a function that performs the actual upgrade logic
// and which takes a pointer to a concrete implementation of the Driver
// interface as its only parameters, which will be passed direcly from the Run
// method into the upgrade function. If not extra fields or data are required
// the function can alternatively take a Driver interface param.
func (m *Manager) Register(version, replaces string, up interface{}) error {
	if strings.ToLower(version) == Head {
		return fmt.Errorf("unable to register version called head")
	}

	if _, ok := m.migrations[version]; ok {
		return fmt.Errorf("revision already exists: %s", version)
	}

	m.migrations[version] = migration{
		version:  version,
		replaces: replaces,
		up:       up,
	}

	return nil
}

// Run will actually perform the necessary migrations to bring the backing datastore
// from its current revision to the specified revision.
func (m *Manager) Run(ctx context.Context, driver Driver, throughRevision string, dryRun RunType) error {
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

	for _, oneMigration := range toRun {
		// Check that the migration is actually a function that accepts one parameter, the type of
		// which is a valid coercion of driver instance.
		if err := checkTypes(driver, oneMigration.up); err != nil {
			return fmt.Errorf("unable to validate up migration: %w", err)
		}

		log.Info().Str("from", oneMigration.replaces).Str("to", oneMigration.version).Msg("planned migration")
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

			in := []reflect.Value{reflect.ValueOf(driver)}
			upFunction := reflect.ValueOf(migrationToRun.up)

			errArg := upFunction.Call(in)[0]
			if !errArg.IsNil() {
				return fmt.Errorf("error running migration up function: %v", errArg)
			}

			if err := driver.WriteVersion(ctx, migrationToRun.version, migrationToRun.replaces); err != nil {
				return fmt.Errorf("error writing migration version to driver: %w", err)
			}
		}
	}

	return nil
}

func (m *Manager) HeadRevision() (string, error) {
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

func (m *Manager) IsHeadCompatible(revision string) (bool, error) {
	headRevision, err := m.HeadRevision()
	if err != nil {
		return false, err
	}
	headMigration := m.migrations[headRevision]
	return revision == headMigration.version || revision == headMigration.replaces, nil
}

func collectMigrationsInRange(starting, through string, all map[string]migration) ([]migration, error) {
	var found []migration

	lookingForRevision := through
	for lookingForRevision != starting {
		foundMigration, ok := all[lookingForRevision]
		if !ok {
			return []migration{}, fmt.Errorf("unable to find migration for revision: %s", lookingForRevision)
		}

		found = append([]migration{foundMigration}, found...)
		lookingForRevision = foundMigration.replaces
	}

	return found, nil
}

func checkTypes(driver Driver, shouldBeFunction interface{}) error {
	driverType := reflect.TypeOf(driver)
	if driverType == nil {
		return errors.New("driver is nil pointer")
	}

	funcType := reflect.TypeOf(shouldBeFunction)
	if funcType == nil {
		return errors.New("function pointer is nil")
	}

	if funcType.NumIn() != 1 {
		return errors.New("function must take one and only one parameter")
	}

	onlyParam := funcType.In(0)

	if !driverType.AssignableTo(onlyParam) {
		return errors.New("driver object must be assignable to function parameter")
	}

	return nil
}
