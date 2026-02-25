package datalayer

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// LegacySchemaWriter provides access to legacy schema write operations.
//
// Deprecated: This is only for backwards-compatible additive-only schema changes
// and will be removed when the additive-only schema mode is removed.
type LegacySchemaWriter interface {
	LegacyWriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error
	LegacyWriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error
	LegacyDeleteCaveats(ctx context.Context, names []string) error
	LegacyDeleteNamespaces(ctx context.Context, nsNames []string, delOption datastore.DeleteNamespacesRelationshipsOption) error
}

// DataLayer is the interface for accessing data from the calling layer.
// It abstracts the underlying datastore, hiding Legacy* methods and
// providing clean access to schema, relationships, and metadata.
type DataLayer interface {
	SnapshotReader(datastore.Revision) RevisionedReader
	ReadWriteTx(context.Context, TxUserFunc, ...options.RWTOptionsOption) (datastore.Revision, error)
	OptimizedRevision(ctx context.Context) (datastore.Revision, error)
	HeadRevision(ctx context.Context) (datastore.Revision, error)
	CheckRevision(ctx context.Context, revision datastore.Revision) error
	RevisionFromString(serialized string) (datastore.Revision, error)
	Watch(ctx context.Context, afterRevision datastore.Revision, options datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error)
	ReadyState(ctx context.Context) (datastore.ReadyState, error)
	Features(ctx context.Context) (*datastore.Features, error)
	OfflineFeatures() (*datastore.Features, error)
	Statistics(ctx context.Context) (datastore.Stats, error)
	UniqueID(ctx context.Context) (string, error)
	MetricsID() (string, error)
	Close() error

	// TODO: additional methods to support bulk operations, such as:
	// BulkImport
	// BulkExport
	// DeleteAllData
}

// SchemaReader groups schema read methods, accessed via RevisionedReader.ReadSchema().
type SchemaReader interface {
	// SchemaText returns the full schema text.
	SchemaText() (string, error)

	// LookupTypeDefByName looks up a type definition by name.
	LookupTypeDefByName(ctx context.Context, name string) (datastore.RevisionedTypeDefinition, bool, error)

	// LookupCaveatDefByName looks up a caveat definition by name.
	LookupCaveatDefByName(ctx context.Context, name string) (datastore.RevisionedCaveat, bool, error)

	// ListAllTypeDefinitions lists all type definitions.
	ListAllTypeDefinitions(ctx context.Context) ([]datastore.RevisionedTypeDefinition, error)

	// ListAllCaveatDefinitions lists all caveat definitions.
	ListAllCaveatDefinitions(ctx context.Context) ([]datastore.RevisionedCaveat, error)

	// ListAllSchemaDefinitions lists all type and caveat definitions.
	ListAllSchemaDefinitions(ctx context.Context) (map[string]datastore.SchemaDefinition, error)

	// LookupSchemaDefinitionsByNames looks up type and caveat definitions by name.
	LookupSchemaDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.SchemaDefinition, error)

	// LookupTypeDefinitionsByNames looks up type definitions by name.
	LookupTypeDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.TypeDefinition, error)

	// LookupCaveatDefinitionsByNames looks up caveat definitions by name.
	LookupCaveatDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.CaveatDefinition, error)
}

// RevisionedReader reads data at a specific revision.
type RevisionedReader interface {
	// ReadSchema returns a SchemaReader for organized schema operations.
	ReadSchema() (SchemaReader, error)

	// QueryRelationships reads relationships, starting from the resource side.
	QueryRelationships(
		ctx context.Context,
		filter datastore.RelationshipsFilter,
		options ...options.QueryOptionsOption,
	) (datastore.RelationshipIterator, error)

	// ReverseQueryRelationships reads relationships, starting from the subject.
	ReverseQueryRelationships(
		ctx context.Context,
		subjectsFilter datastore.SubjectsFilter,
		options ...options.ReverseQueryOptionsOption,
	) (datastore.RelationshipIterator, error)

	// CountRelationships returns the count of relationships that match the provided counter.
	CountRelationships(ctx context.Context, name string) (int, error)

	// LookupCounters returns all registered counters.
	LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error)
}

// TxUserFunc is a type for the function that users supply when they invoke a read-write transaction.
type TxUserFunc func(context.Context, ReadWriteTransaction) error

// ReadWriteTransaction supports both reading and writing.
type ReadWriteTransaction interface {
	RevisionedReader

	// WriteRelationships takes a list of tuple mutations and applies them to the datastore.
	WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error

	// DeleteRelationships deletes relationships that match the provided filter.
	DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter,
		options ...options.DeleteOptionsOption,
	) (uint64, bool, error)

	// BulkLoad writes all relationships from the source in an optimized fashion.
	BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error)

	// WriteSchema writes the full set of schema definitions.
	WriteSchema(ctx context.Context, definitions []datastore.SchemaDefinition, schemaString string, caveatTypeSet *caveattypes.TypeSet) error

	// LegacySchemaWriter returns a legacy schema writer for backwards-compatible
	// additive-only schema operations.
	//
	// Deprecated: Will be removed when additive-only schema mode is removed.
	LegacySchemaWriter() LegacySchemaWriter

	// RegisterCounter registers a count with the provided filter.
	RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error

	// UnregisterCounter unregisters a counter.
	UnregisterCounter(ctx context.Context, name string) error

	// StoreCounterValue stores a count for the counter with the given name.
	StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error
}
