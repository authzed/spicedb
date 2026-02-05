package memdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaTableReadWrite(t *testing.T) {
	ds, err := NewMemdbDatastore(0, 0, 0)
	require.NoError(t, err)
	defer ds.Close()

	memdbDS := ds.(*memdbDatastore)

	// Lock for direct access
	memdbDS.Lock()
	defer memdbDS.Unlock()

	// Get a write transaction
	tx := memdbDS.db.Txn(true)
	defer tx.Abort()

	// Test writing schema data
	testSchema := &schemaData{
		name: "test-schema",
		data: []byte("test schema definition data"),
	}

	err = tx.Insert(tableSchema, testSchema)
	require.NoError(t, err, "failed to insert schema")

	// Test reading schema data
	found, err := tx.First(tableSchema, indexID, "test-schema")
	require.NoError(t, err)
	require.NotNil(t, found, "schema not found")

	readSchema := found.(*schemaData)
	require.Equal(t, testSchema.name, readSchema.name)
	require.Equal(t, testSchema.data, readSchema.data)

	// Commit the transaction
	tx.Commit()
}

func TestSchemaRevisionTableReadWrite(t *testing.T) {
	ds, err := NewMemdbDatastore(0, 0, 0)
	require.NoError(t, err)
	defer ds.Close()

	memdbDS := ds.(*memdbDatastore)

	// Lock for direct access
	memdbDS.Lock()
	defer memdbDS.Unlock()

	// Get a write transaction
	tx := memdbDS.db.Txn(true)
	defer tx.Abort()

	// Test writing schema revision data
	revisionData := &schemaRevisionData{
		name: "current",
		hash: []byte("schema-hash-12345"),
	}

	err = tx.Insert(tableSchemaRevision, revisionData)
	require.NoError(t, err, "failed to insert schema revision")

	// Test reading schema revision data
	found, err := tx.First(tableSchemaRevision, indexID, "current")
	require.NoError(t, err)
	require.NotNil(t, found, "schema revision not found")

	readRevision := found.(*schemaRevisionData)
	require.Equal(t, revisionData.name, readRevision.name)
	require.Equal(t, revisionData.hash, readRevision.hash)

	// Commit the transaction
	tx.Commit()
}

func TestSchemaTableUpdate(t *testing.T) {
	ds, err := NewMemdbDatastore(0, 0, 0)
	require.NoError(t, err)
	defer ds.Close()

	memdbDS := ds.(*memdbDatastore)

	// Lock for direct access
	memdbDS.Lock()
	defer memdbDS.Unlock()

	// Get a write transaction
	tx := memdbDS.db.Txn(true)
	defer tx.Abort()

	// Insert initial schema
	initialSchema := &schemaData{
		name: "test-schema-update",
		data: []byte("initial data"),
	}

	err = tx.Insert(tableSchema, initialSchema)
	require.NoError(t, err, "failed to insert initial schema")

	// Update the schema by deleting and inserting
	err = tx.Delete(tableSchema, initialSchema)
	require.NoError(t, err, "failed to delete old schema")

	updatedSchema := &schemaData{
		name: "test-schema-update",
		data: []byte("updated data that is much longer"),
	}

	err = tx.Insert(tableSchema, updatedSchema)
	require.NoError(t, err, "failed to insert updated schema")

	// Read back and verify
	found, err := tx.First(tableSchema, indexID, "test-schema-update")
	require.NoError(t, err)
	require.NotNil(t, found, "updated schema not found")

	readSchema := found.(*schemaData)
	require.Equal(t, updatedSchema.data, readSchema.data)

	// Commit the transaction
	tx.Commit()
}

func TestSchemaTableDelete(t *testing.T) {
	ds, err := NewMemdbDatastore(0, 0, 0)
	require.NoError(t, err)
	defer ds.Close()

	memdbDS := ds.(*memdbDatastore)

	// Lock for direct access
	memdbDS.Lock()
	defer memdbDS.Unlock()

	// Get a write transaction
	tx := memdbDS.db.Txn(true)
	defer tx.Abort()

	// Insert schema
	testSchema := &schemaData{
		name: "test-schema-delete",
		data: []byte("data to be deleted"),
	}

	err = tx.Insert(tableSchema, testSchema)
	require.NoError(t, err, "failed to insert schema")

	// Verify it exists
	found, err := tx.First(tableSchema, indexID, "test-schema-delete")
	require.NoError(t, err)
	require.NotNil(t, found, "schema not found before delete")

	// Delete the schema
	err = tx.Delete(tableSchema, testSchema)
	require.NoError(t, err, "failed to delete schema")

	// Verify it's gone
	found, err = tx.First(tableSchema, indexID, "test-schema-delete")
	require.NoError(t, err)
	require.Nil(t, found, "schema should be deleted")

	// Commit the transaction
	tx.Commit()
}

func TestSchemaTableMultipleSchemas(t *testing.T) {
	ds, err := NewMemdbDatastore(0, 0, 0)
	require.NoError(t, err)
	defer ds.Close()

	memdbDS := ds.(*memdbDatastore)

	// Lock for direct access
	memdbDS.Lock()
	defer memdbDS.Unlock()

	// Get a write transaction
	tx := memdbDS.db.Txn(true)
	defer tx.Abort()

	// Insert multiple schemas
	schemas := []*schemaData{
		{name: "schema1", data: []byte("data1")},
		{name: "schema2", data: []byte("data2")},
		{name: "schema3", data: []byte("data3")},
	}

	for _, s := range schemas {
		err = tx.Insert(tableSchema, s)
		require.NoError(t, err, "failed to insert schema %s", s.name)
	}

	// Read them all back
	for _, s := range schemas {
		found, err := tx.First(tableSchema, indexID, s.name)
		require.NoError(t, err)
		require.NotNil(t, found, "schema %s not found", s.name)

		readSchema := found.(*schemaData)
		require.Equal(t, s.name, readSchema.name)
		require.Equal(t, s.data, readSchema.data)
	}

	// Commit the transaction
	tx.Commit()
}
