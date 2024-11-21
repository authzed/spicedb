package common

import (
	"fmt"
	"slices"
	"strings"

	"github.com/authzed/spicedb/pkg/datastore"
)

type MigrationValidator struct {
	additionalAllowedMigrations []string
	headMigration               string
}

func NewMigrationValidator(headMigration string, additionalAllowedMigrations []string) *MigrationValidator {
	return &MigrationValidator{
		additionalAllowedMigrations: additionalAllowedMigrations,
		headMigration:               headMigration,
	}
}

// MigrationReadyState returns the readiness of the datastore for the given version.
func (mv *MigrationValidator) MigrationReadyState(version string) datastore.ReadyState {
	if version == mv.headMigration {
		return datastore.ReadyState{IsReady: true}
	}
	if slices.Contains(mv.additionalAllowedMigrations, version) {
		return datastore.ReadyState{IsReady: true}
	}
	var msgBuilder strings.Builder
	msgBuilder.WriteString(fmt.Sprintf("datastore is not migrated: currently at revision %q, but requires %q", version, mv.headMigration))

	if len(mv.additionalAllowedMigrations) > 0 {
		msgBuilder.WriteString(fmt.Sprintf(" (additional allowed migrations: %v)", mv.additionalAllowedMigrations))
	}
	msgBuilder.WriteString(". Please run \"spicedb datastore migrate\".")
	return datastore.ReadyState{
		Message: msgBuilder.String(),
		IsReady: false,
	}
}
