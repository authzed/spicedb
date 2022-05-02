package datastore

import (
	"context"
	"fmt"
	"sort"
	"strings"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var Engines = []string{}

// SortedEngineIDs returns the full set of engine IDs, sorted.
func SortedEngineIDs() []string {
	engines := append([]string{}, Engines...)
	sort.Strings(engines)
	return engines
}

// EngineOptions returns the full set of engine IDs, sorted and quoted into a string.
func EngineOptions() string {
	ids := SortedEngineIDs()
	quoted := make([]string, 0, len(ids))
	for _, id := range ids {
		quoted = append(quoted, fmt.Sprintf("%q", id))
	}
	return strings.Join(quoted, ", ")
}

// Ellipsis is a special relation that is assumed to be valid on the right
// hand side of a tuple.
const Ellipsis = "..."

// RevisionChanges represents the changes in a single transaction.
type RevisionChanges struct {
	Revision Revision
	Changes  []*core.RelationTupleUpdate
}

type Reader interface {
	// QueryTuples reads relationships starting from the resource side.
	QueryRelationships(
		ctx context.Context,
		filter *v1.RelationshipFilter,
		options ...options.QueryOptionsOption,
	) (RelationshipIterator, error)

	// ReverseQueryRelationships reads relationships starting from the subject.
	ReverseQueryRelationships(
		ctx context.Context,
		subjectFilter *v1.SubjectFilter,
		options ...options.ReverseQueryOptionsOption,
	) (RelationshipIterator, error)

	// ReadNamespace reads a namespace definition and version and returns it, and the revision at
	// which it was created or last written, if found.
	ReadNamespace(ctx context.Context, nsName string) (ns *core.NamespaceDefinition, lastWritten Revision, err error)

	// ListNamespaces lists all namespaces defined.
	ListNamespaces(ctx context.Context) ([]*core.NamespaceDefinition, error)

	// NamespaceCacheKey returns a string key for use in a NamespaceManager's cache
	NamespaceCacheKey(namespaceName string) (string, error)
}

type ReadWriteTransaction interface {
	Reader

	// WriteRelationships takes a list of tuple mutations and applies them to the datastore.
	WriteRelationships(mutations []*v1.RelationshipUpdate) error

	// DeleteRelationships deletes all Relationships that match the provided filter.
	DeleteRelationships(filter *v1.RelationshipFilter) error

	// WriteNamespaces takes proto namespace definitions and persists them.
	WriteNamespaces(newConfigs ...*core.NamespaceDefinition) error

	// DeleteNamespace deletes a namespace and any associated tuples.
	DeleteNamespace(nsName string) error
}

// TxUserFunc is a type for the function that users supply when they invoke a read-write transaction.
type TxUserFunc func(context.Context, ReadWriteTransaction) error

// Datastore represents tuple access for a single namespace.
type Datastore interface {
	// SnapshotRead creates a read-only handle that reads the datastore at the specified revision.
	// Any errors establishing the reader will be returned by subsequent calls.
	SnapshotReader(Revision) Reader

	// ReadWriteTx tarts a read/write transaction, which will be committed if no error is
	// returned and rolled back if an error is returned.
	ReadWriteTx(context.Context, TxUserFunc) (Revision, error)

	// OptimizedRevision gets a revision that will likely already be replicated
	// and will likely be shared amongst many queries.
	OptimizedRevision(ctx context.Context) (Revision, error)

	// HeadRevision gets a revision that is guaranteed to be at least as fresh as
	// right now.
	HeadRevision(ctx context.Context) (Revision, error)

	// CheckRevision checks the specified revision to make sure it's valid and
	// hasn't been garbage collected.
	CheckRevision(ctx context.Context, revision Revision) error

	// Watch notifies the caller about all changes to tuples.
	//
	// All events following afterRevision will be sent to the caller.
	Watch(ctx context.Context, afterRevision Revision) (<-chan *RevisionChanges, <-chan error)

	// IsReady returns whether the datastore is ready to accept data. Datastores that require
	// database schema creation will return false until the migrations have been run to create
	// the necessary tables.
	IsReady(ctx context.Context) (bool, error)

	// Statistics returns relevant values about the data contained in this cluster.
	Statistics(ctx context.Context) (Stats, error)

	// Close closes the data store.
	Close() error
}

// ObjectTypeStat represents statistics for a single object type (namespace).
type ObjectTypeStat struct {
	// NumRelations is the number of relations defined in a single object type.
	NumRelations uint32

	// NumPermissions is the number of permissions defined in a single object type.
	NumPermissions uint32
}

// Stats represents statistics for the entire datastore.
type Stats struct {
	// UniqueID is a unique string for a single datastore.
	UniqueID string

	// EstimatedRelationshipCount is a best-guess estimate of the number of relationships
	// in the datstore. Computing it should use a lightweight method such as reading
	// table statistics.
	EstimatedRelationshipCount uint64

	// ObjectTypeStatistics returns a slice element for each object type (namespace)
	// stored in the datastore.
	ObjectTypeStatistics []ObjectTypeStat
}

// RelationshipIterator is an iterator over matched tuples.
type RelationshipIterator interface {
	// Next returns the next tuple in the result set.
	Next() *core.RelationTuple

	// After receiving a nil response, the caller must check for an error.
	Err() error

	// Close cancels the query and closes any open connections.
	Close()
}

// Revision is a type alias to make changing the revision type a little bit
// easier if we need to do it in the future. Implementations should code
// directly against decimal.Decimal when creating or parsing.
type Revision = decimal.Decimal

// NoRevision is a zero type for the revision that will make changing the
// revision type in the future a bit easier if necessary. Implementations
// should use any time they want to signal an empty/error revision.
var NoRevision Revision
