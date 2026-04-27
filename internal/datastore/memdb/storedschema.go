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

	return datastore.NewReadOnlyStoredSchema(storedSchema), nil
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

	// Write the schema hash to the schema revision table for tracking per-snapshot.
	v1 := schema.GetV1()
	if v1 != nil {
		// Delete existing entry first, if any.
		if existing, err := tx.First(tableSchemaRevision, indexID, "current"); err == nil && existing != nil {
			if err := tx.Delete(tableSchemaRevision, existing); err != nil {
				return fmt.Errorf("failed to delete existing schema revision data: %w", err)
			}
		}

		if err := tx.Insert(tableSchemaRevision, &schemaRevisionData{
			name: "current",
			hash: []byte(v1.SchemaHash),
		}); err != nil {
			return fmt.Errorf("failed to write schema revision data: %w", err)
		}
	}

	return nil
}
