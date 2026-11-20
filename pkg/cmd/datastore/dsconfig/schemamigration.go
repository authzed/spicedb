package dsconfig

import "github.com/authzed/spicedb/pkg/datalayer/schemamode"

// ApplySchemaMigrationPhase consumes shared schema phases before engine-specific
// validation, and permits the preparation deployment to run before the refresh.
func (c *Config) ApplySchemaMigrationPhase() {
	mode, ok := schemamode.ModeForMigrationPhase(c.MigrationPhase)
	if !ok {
		return
	}
	if mode == schemamode.SchemaModeReadLegacyWriteBoth {
		var previous string
		switch c.Engine {
		case PostgresEngine, CockroachEngine, SpannerEngine:
			previous = "populate-schema-tables"
		case MySQLEngine:
			previous = "identifiers_case_sensitive"
		}
		if previous != "" {
			c.AllowedMigrations = append(c.AllowedMigrations, previous)
		}
	}
	if c.BootstrapSchemaMode != schemamode.SchemaModeReadNewWriteNew {
		// The PostgreSQL refresh relies on SSI to invalidate writer snapshots that
		// predate the first unified schema. Restore the configured isolation after
		// the operator clears the schema phase.
		if c.Engine == PostgresEngine {
			c.RelaxedIsolationLevel = false
		}
		if c.BootstrapSchemaMode != schemamode.SchemaModeReadLegacyWriteBoth {
			c.BootstrapSchemaMode = mode
		}
	}
	c.MigrationPhase = ""
}
