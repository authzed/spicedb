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
)

// ShouldRefreshSchema preserves unified-only deployments whose legacy copy may be stale.
func ShouldRefreshSchema(ctx context.Context) bool {
	mode, _ := ctx.Value(SchemaMigrationMode).(schemamode.SchemaMode)
	return mode != schemamode.SchemaModeReadNewWriteNew
}
