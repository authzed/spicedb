package schemamode

import (
	"fmt"
	"strings"
)

const (
	// MigrationPhaseWriteBothReadOld prepares an installation for unified reads.
	MigrationPhaseWriteBothReadOld = "schema-write-both-read-old"
	// MigrationPhaseWriteBothReadNew enables unified reads after the refresh migration.
	MigrationPhaseWriteBothReadNew = "schema-write-both-read-new"
)

// ModeForMigrationPhase identifies schema phases without consuming historical,
// datastore-specific migration phases.
func ModeForMigrationPhase(phase string) (SchemaMode, bool) {
	switch phase {
	case MigrationPhaseWriteBothReadOld:
		return SchemaModeReadLegacyWriteBoth, true
	case MigrationPhaseWriteBothReadNew:
		return SchemaModeReadNewWriteBoth, true
	default:
		return 0, false
	}
}

// Resolve selects a schema mode for server startup and migration jobs. The normal
// default defers to an active phase. An explicit unified-only deployment remains
// authoritative. Dual-writing legacy fallbacks are safe; legacy-only overrides
// are rejected during phased upgrades.
func Resolve(configured, phase string) (SchemaMode, error) {
	mode := SchemaModeReadNewWriteBoth
	if configured != "" {
		var err error
		mode, err = ParseSchemaMode(configured)
		if err != nil {
			return 0, err
		}
	}
	phasedMode, ok := ModeForMigrationPhase(phase)
	if !ok {
		if strings.HasPrefix(phase, "schema-") {
			return 0, fmt.Errorf("unknown schema migration phase: %s", phase)
		}
		return mode, nil
	}
	if mode == SchemaModeReadNewWriteNew || mode == SchemaModeReadLegacyWriteBoth {
		return mode, nil
	}
	if mode != SchemaModeReadNewWriteBoth && mode != phasedMode {
		return 0, fmt.Errorf("schema mode %q conflicts with migration phase %q", configured, phase)
	}
	return phasedMode, nil
}
