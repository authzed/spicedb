package schemamode

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveMigrationPhase(t *testing.T) {
	for _, tc := range []struct {
		name, mode, phase string
		want              SchemaMode
		wantError         bool
	}{
		{"default", "", "", SchemaModeReadNewWriteBoth, false},
		{"prepare", "read-new-write-both", "schema-write-both-read-old", SchemaModeReadLegacyWriteBoth, false},
		{"promote", "", "schema-write-both-read-new", SchemaModeReadNewWriteBoth, false},
		{"unified only prepare", "read-new-write-new", "schema-write-both-read-old", SchemaModeReadNewWriteNew, false},
		{"unified only promote", "read-new-write-new", "schema-write-both-read-new", SchemaModeReadNewWriteNew, false},
		{"fallback", "read-legacy-write-legacy", "", SchemaModeReadLegacyWriteLegacy, false},
		{"fallback during promotion", "read-legacy-write-both", "schema-write-both-read-new", SchemaModeReadLegacyWriteBoth, false},
		{"conflicting override", "read-legacy-write-legacy", "schema-write-both-read-old", 0, true},
		{"unknown schema phase", "", "schema-invalid", 0, true},
		{"historical datastore phase", "", "write-both-read-old", SchemaModeReadNewWriteBoth, false},
		{"invalid mode", "invalid", "schema-write-both-read-old", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Resolve(tc.mode, tc.phase)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
