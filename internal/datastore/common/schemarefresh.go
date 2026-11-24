package common

import (
	"context"
	"errors"

	"github.com/authzed/spicedb/pkg/migrate"
)

// ErrSchemaRefreshModeRequired is returned when a refresh would replace a unified
// schema that differs from legacy storage, and the migration was not told which
// storage the installation reads.
var ErrSchemaRefreshModeRequired = errors.New("refusing to refresh unified schema storage: it differs from legacy storage, " +
	"and the migration was not told which one this installation reads. " +
	"Rerun the migration with --datastore-migration-phase=schema-write-both-read-old to copy legacy storage into unified storage, " +
	"or with --experimental-schema-mode=read-new-write-new to keep a unified-only schema")

// UnifiedSchemaState describes the stored unified schema as a refresh found it.
type UnifiedSchemaState struct {
	// Exists reports that a live unified schema is stored.
	Exists bool

	// Hash is the stored unified schema hash.
	Hash string
}

// CheckSchemaRefresh allows a refresh without a configured schema mode only when
// copying legacy storage cannot lose anything: unified storage is absent or
// already holds the same schema.
func CheckSchemaRefresh(ctx context.Context, state UnifiedSchemaState, legacyHash string) error {
	if migrate.SchemaModeConfigured(ctx) || !state.Exists || state.Hash == legacyHash {
		return nil
	}
	return ErrSchemaRefreshModeRequired
}
