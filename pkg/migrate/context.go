package migrate

import (
	"context"

	"github.com/authzed/spicedb/pkg/datalayer/schemamode"
)

// MigrationVariable contains constants that can be used as context keys that might
// be relevant in a number of different migration scenarios.
type MigrationVariable int

const (
	// BackfillBatchSize represents the number of items that should be backfilled in a
	// single step of an incremental backfill, and should be of type uint64.
	BackfillBatchSize MigrationVariable = iota

	// SchemaMigrationMode identifies the authoritative schema format for backfills.
	SchemaMigrationMode

	// SchemaMigrationModeConfigured reports that the operator set a schema mode
	// or schema migration phase for the migration, and should be of type bool.
	SchemaMigrationModeConfigured
)

// ShouldRefreshSchema reports whether the refresh copies legacy storage into
// unified storage. An installation configured to read unified storage already
// treats it as current, so there is nothing to copy. One configured to read
// legacy storage always copies. Without a configured mode, the refresh copies
// only where that cannot lose unified-only changes; see SchemaModeConfigured.
func ShouldRefreshSchema(ctx context.Context) bool {
	mode, _ := ctx.Value(SchemaMigrationMode).(schemamode.SchemaMode)
	return !SchemaModeConfigured(ctx) || !mode.ReadsFromNew()
}

// SchemaModeConfigured reports whether the migration was told the schema mode
// the installation runs, explicitly or through a schema migration phase.
func SchemaModeConfigured(ctx context.Context) bool {
	configured, _ := ctx.Value(SchemaMigrationModeConfigured).(bool)
	return configured
}
