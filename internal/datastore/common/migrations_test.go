package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestMigrationReadyState(t *testing.T) {
	tests := []struct {
		name                        string
		additionalAllowedMigrations []string
		headMigration               string
		version                     string
		want                        datastore.ReadyState
	}{
		{
			name:          "matches head migration",
			headMigration: "initial",
			version:       "initial",
			want:          datastore.ReadyState{IsReady: true},
		},
		{
			name:                        "matches additional migration",
			headMigration:               "initial",
			additionalAllowedMigrations: []string{"additional"},
			version:                     "additional",
			want:                        datastore.ReadyState{IsReady: true},
		},
		{
			name:                        "matches one of several additional migrations",
			headMigration:               "initial",
			additionalAllowedMigrations: []string{"additional", "additional2", "additional3"},
			version:                     "additional2",
			want:                        datastore.ReadyState{IsReady: true},
		},
		{
			name:                        "matches head migration when additional migrations are allowed",
			headMigration:               "initial",
			additionalAllowedMigrations: []string{"additional"},
			version:                     "initial",
			want:                        datastore.ReadyState{IsReady: true},
		},
		{
			name:          "doesn't match head migration",
			headMigration: "initial",
			version:       "additional",
			want:          datastore.ReadyState{IsReady: false, Message: `datastore is not migrated: currently at revision "additional", but requires "initial". Please run "spicedb datastore migrate".`},
		},
		{
			name:                        "doesn't match head migration or additional migrations",
			headMigration:               "initial",
			additionalAllowedMigrations: []string{"additional", "additional2"},
			version:                     "plustwo",
			want:                        datastore.ReadyState{IsReady: false, Message: `datastore is not migrated: currently at revision "plustwo", but requires "initial" (additional allowed migrations: [additional additional2]). Please run "spicedb datastore migrate".`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mv := &MigrationValidator{
				additionalAllowedMigrations: tt.additionalAllowedMigrations,
				headMigration:               tt.headMigration,
			}
			require.Equal(t, tt.want, mv.MigrationReadyState(tt.version))
		})
	}
}
