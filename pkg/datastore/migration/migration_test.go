package migration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunRejectsInvalidSchemaConfigurationBeforeConnecting(t *testing.T) {
	for _, cfg := range []Config{
		{SchemaMode: "unknown-schema-mode"},
		{MigrationPhase: "schema-unknown"},
		{SchemaMode: "read-legacy-write-legacy", MigrationPhase: "schema-write-both-read-old"},
	} {
		// No engine is registered: schema configuration must be validated before
		// choosing or connecting to an engine and running any migrations.
		err := Run(t.Context(), &cfg, "head")
		require.ErrorContains(t, err, "schema")
	}
}
