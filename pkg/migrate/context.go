package migrate

import (
	"context"

	"github.com/authzed/ctxkey"

	"github.com/authzed/spicedb/pkg/datalayer/schemamode"
)

// BackfillBatchSize is the context key for the number of items that should be
// backfilled in a single step of an incremental backfill.
var BackfillBatchSize = ctxkey.New[uint64]()

// SchemaMigrationMode is the context key for the schema mode the installation
// runs, which identifies the authoritative schema format for backfills.
var SchemaMigrationMode = ctxkey.New[schemamode.SchemaMode]()

// SchemaMigrationModeConfigured is the context key reporting that the operator
// set a schema mode or schema migration phase for the migration.
var SchemaMigrationModeConfigured = ctxkey.New[bool]()

// ShouldRefreshSchema reports whether the refresh copies legacy storage into
// unified storage. An installation configured to read unified storage already
// treats it as current, so there is nothing to copy. Any other installation
// reads legacy storage and always copies. That includes one configured with no
// mode at all: before unified storage became the default, every other mode had
// to be set explicitly, so an unconfigured installation ran legacy-only.
func ShouldRefreshSchema(ctx context.Context) bool {
	mode, _ := SchemaMigrationMode.Value(ctx)
	return !SchemaModeConfigured(ctx) || !mode.ReadsFromNew()
}

// SchemaModeConfigured reports whether the migration was told the schema mode
// the installation runs, explicitly or through a schema migration phase.
func SchemaModeConfigured(ctx context.Context) bool {
	configured, _ := SchemaMigrationModeConfigured.Value(ctx)
	return configured
}
