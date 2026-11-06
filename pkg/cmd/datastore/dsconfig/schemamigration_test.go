package dsconfig

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datalayer/schemamode"
)

func TestApplySchemaMigrationPhase(t *testing.T) {
	for _, engine := range []string{PostgresEngine, CockroachEngine, MySQLEngine, SpannerEngine, MemoryEngine} {
		t.Run(engine, func(t *testing.T) {
			cfg := DefaultDatastoreConfig()
			cfg.Engine = engine
			cfg.RelaxedIsolationLevel = true
			cfg.MigrationPhase = "schema-write-both-read-old"
			cfg.AllowedMigrations = []string{"explicit"}
			cfg.ApplySchemaMigrationPhase()
			require.Empty(t, cfg.MigrationPhase)
			if engine == PostgresEngine {
				require.False(t, cfg.RelaxedIsolationLevel)
			}
			require.Equal(t, schemamode.SchemaModeReadLegacyWriteBoth, cfg.BootstrapSchemaMode)
			require.Contains(t, cfg.AllowedMigrations, "explicit")
			switch engine {
			case MySQLEngine:
				require.Contains(t, cfg.AllowedMigrations, "identifiers_case_sensitive")
			case MemoryEngine:
				require.Len(t, cfg.AllowedMigrations, 1)
			default:
				require.Contains(t, cfg.AllowedMigrations, "populate-schema-tables")
			}
		})
	}
	t.Run("preserve unified only", func(t *testing.T) {
		cfg := DefaultDatastoreConfig()
		cfg.Engine = PostgresEngine
		cfg.RelaxedIsolationLevel = true
		cfg.MigrationPhase = "schema-write-both-read-old"
		cfg.BootstrapSchemaMode = schemamode.SchemaModeReadNewWriteNew
		cfg.ApplySchemaMigrationPhase()
		require.Equal(t, schemamode.SchemaModeReadNewWriteNew, cfg.BootstrapSchemaMode)
		require.True(t, cfg.RelaxedIsolationLevel)
	})
	t.Run("promotion requires head", func(t *testing.T) {
		cfg := DefaultDatastoreConfig()
		cfg.MigrationPhase = "schema-write-both-read-new"
		cfg.ApplySchemaMigrationPhase()
		require.Empty(t, cfg.MigrationPhase)
		require.Empty(t, cfg.AllowedMigrations)
		require.Equal(t, schemamode.SchemaModeReadNewWriteBoth, cfg.BootstrapSchemaMode)
	})
	t.Run("historical phase is forwarded", func(t *testing.T) {
		cfg := DefaultDatastoreConfig()
		cfg.MigrationPhase = "write-both-read-old"
		cfg.ApplySchemaMigrationPhase()
		require.Equal(t, "write-both-read-old", cfg.MigrationPhase)
	})
}
