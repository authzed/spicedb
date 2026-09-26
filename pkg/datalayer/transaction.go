package datalayer

import "github.com/authzed/spicedb/pkg/datastore"

// NewReadWriteTransaction wraps an already-open datastore transaction. Schema reads
// remain transaction-local; this wrapper neither retries nor publishes shared cache entries.
func NewReadWriteTransaction(tx datastore.ReadWriteTransaction, mode SchemaMode) ReadWriteTransaction {
	return &readWriteTransaction{rwt: tx, schemaMode: mode, cache: noopSchemaCache{}}
}
