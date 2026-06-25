package memdb

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const unifiedSchemaName = "unified_schema"

// ReadStoredSchema reads the unified stored schema from the in-memory database.
func (r *memdbReader) ReadStoredSchema(_ context.Context) (*datastore.ReadOnlyStoredSchema, error) {
	r.mustLock()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, err
	}

	raw, err := tx.First(tableSchema, indexID, unifiedSchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	if raw == nil {
		return nil, datastore.ErrSchemaNotFound
	}

	sd, ok := raw.(*schemaData)
	if !ok {
		return nil, fmt.Errorf("unexpected schema data type: %T", raw)
	}

	storedSchema := &core.StoredSchema{}
	if err := storedSchema.UnmarshalVT(sd.data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	// len(sd.data) is the exact serialized size, used as the rough schema-size base for cache cost.
	return datastore.NewReadOnlyStoredSchemaWithSize(storedSchema, len(sd.data)), nil
}

// assertSchemaHash verifies the stored schema hash matches expectedHash.
// Returns ErrSchemaHashPreconditionFailed if not found or mismatched.
func assertSchemaHash(_ context.Context, rwt *memdbReadWriteTx, expectedHash string) error {
	rwt.mustLock()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	raw, err := tx.First(tableSchemaRevision, indexID, "current")
	if err != nil {
		return fmt.Errorf("failed to read schema hash: %w", err)
	}

	if raw == nil {
		return datastore.ErrSchemaNotFound
	}

	srd, ok := raw.(*schemaRevisionData)
	if !ok {
		return fmt.Errorf("unexpected schema revision data type: %T", raw)
	}

	if len(srd.hash) == 0 || string(srd.hash) != expectedHash {
		return datastore.ErrSchemaHashPreconditionFailed
	}
	return nil
}

// WriteStoredSchema writes the unified stored schema to the in-memory database.
func (rwt *memdbReadWriteTx) WriteStoredSchema(_ context.Context, schema *core.StoredSchema) error {
	rwt.mustLock()
	defer rwt.Unlock()

	tx, err := rwt.txSource()
	if err != nil {
		return err
	}

	data, err := schema.MarshalVT()
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	if err := tx.Insert(tableSchema, &schemaData{
		name: unifiedSchemaName,
		data: data,
	}); err != nil {
		return fmt.Errorf("failed to write schema: %w", err)
	}

	// Always clear the existing schema revision entry so a stale hash is never
	// left behind, even when the new write does not supply a hash.
	if existing, err := tx.First(tableSchemaRevision, indexID, "current"); err == nil && existing != nil {
		if err := tx.Delete(tableSchemaRevision, existing); err != nil {
			return fmt.Errorf("failed to delete existing schema revision data: %w", err)
		}
	}

	// Write the new hash only when one is provided.
	v1 := schema.GetV1()
	if v1 != nil && v1.SchemaHash != "" {
		if err := tx.Insert(tableSchemaRevision, &schemaRevisionData{
			name: "current",
			hash: []byte(v1.SchemaHash),
		}); err != nil {
			return fmt.Errorf("failed to write schema revision data: %w", err)
		}
	}

	return nil
}
