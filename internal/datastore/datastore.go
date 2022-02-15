package datastore

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore/options"
)

// Ellipsis is a special relation that is assumed to be valid on the right
// hand side of a tuple.
const Ellipsis = "..."

// RevisionChanges represents the changes in a single transaction.
type RevisionChanges struct {
	Revision Revision
	Changes  []*core.RelationTupleUpdate
}

// Datastore represents tuple access for a single namespace.
type Datastore interface {
	GraphDatastore

	// WriteTuples takes a list of existing tuples that must exist, and a list of
	// tuple mutations and applies it to the datastore for the specified
	// namespace.
	WriteTuples(ctx context.Context, preconditions []*v1.Precondition, mutations []*v1.RelationshipUpdate) (Revision, error)

	// DeleteRelationships deletes all Relationships that match the provided
	// filter if all preconditions are met.
	DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (Revision, error)

	// OptimizedRevision gets a revision that will likely already be replicated
	// and will likely be shared amongst many queries.
	OptimizedRevision(ctx context.Context) (Revision, error)

	// HeadRevision gets a revision that is guaranteed to be at least as fresh as
	// right now.
	HeadRevision(ctx context.Context) (Revision, error)

	// Watch notifies the caller about all changes to tuples.
	//
	// All events following afterRevision will be sent to the caller.
	Watch(ctx context.Context, afterRevision Revision) (<-chan *RevisionChanges, <-chan error)

	// WriteNamespace takes a proto namespace definition and persists it,
	// returning the version of the namespace that was created.
	WriteNamespace(ctx context.Context, newConfig *core.NamespaceDefinition) (Revision, error)

	// ReadNamespace reads a namespace definition and version and returns it, and the revision at
	// which it was created or last written, if found.
	ReadNamespace(ctx context.Context, nsName string, revision Revision) (ns *core.NamespaceDefinition, lastWritten Revision, err error)

	// DeleteNamespace deletes a namespace and any associated tuples.
	DeleteNamespace(ctx context.Context, nsName string) (Revision, error)

	// ListNamespaces lists all namespaces defined.
	ListNamespaces(ctx context.Context, revision Revision) ([]*core.NamespaceDefinition, error)

	// IsReady returns whether the datastore is ready to accept data. Datastores that require
	// database schema creation will return false until the migrations have been run to create
	// the necessary tables.
	IsReady(ctx context.Context) (bool, error)

	// NamespaceCacheKey returns a string key for use in a NamespaceManager's cache
	NamespaceCacheKey(namespaceName string, revision Revision) (string, error)

	// Close closes the data store.
	Close() error
}

// GraphDatastore is a subset of the datastore interface that is passed to
// graph resolvers.
type GraphDatastore interface {
	// QueryTuples reads relationships starting from the resource side.
	QueryTuples(
		ctx context.Context,
		filter *v1.RelationshipFilter,
		revision Revision,
		options ...options.QueryOptionsOption,
	) (TupleIterator, error)

	// ReverseQueryRelationships reads relationships starting from the subject.
	ReverseQueryTuples(
		ctx context.Context,
		subjectFilter *v1.SubjectFilter,
		revision Revision,
		options ...options.ReverseQueryOptionsOption,
	) (TupleIterator, error)

	// CheckRevision checks the specified revision to make sure it's valid and
	// hasn't been garbage collected.
	CheckRevision(ctx context.Context, revision Revision) error
}

// TupleIterator is an iterator over matched tuples.
type TupleIterator interface {
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
