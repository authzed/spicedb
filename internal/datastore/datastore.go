package datastore

import (
	"context"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
)

// Ellipsis is a special relation that is assumed to be valid on the right
// hand side of a tuple.
const Ellipsis = "..."

// RevisionChanges represents the changes in a single transaction.
type RevisionChanges struct {
	Revision Revision
	Changes  []*v0.RelationTupleUpdate
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

	// Revision gets the currently replicated revision for this datastore.
	Revision(ctx context.Context) (Revision, error)

	// SyncRevision gets a revision that is guaranteed to be at least as fresh as
	// right now.
	SyncRevision(ctx context.Context) (Revision, error)

	// Watch notifies the caller about all changes to tuples.
	//
	// All events following afterRevision will be sent to the caller.
	Watch(ctx context.Context, afterRevision Revision) (<-chan *RevisionChanges, <-chan error)

	// WriteNamespace takes a proto namespace definition and persists it,
	// returning the version of the namespace that was created.
	WriteNamespace(ctx context.Context, newConfig *v0.NamespaceDefinition) (Revision, error)

	// ReadNamespace reads a namespace definition and version and returns it if
	// found.
	ReadNamespace(ctx context.Context, nsName string) (*v0.NamespaceDefinition, Revision, error)

	// DeleteNamespace deletes a namespace and any associated tuples.
	DeleteNamespace(ctx context.Context, nsName string) (Revision, error)

	// ListNamespaces lists all namespaces defined.
	ListNamespaces(ctx context.Context) ([]*v0.NamespaceDefinition, error)

	// IsReady returns whether the datastore is ready to accept data. Datastores that require
	// database schema creation will return false until the migrations have been run to create
	// the necessary tables.
	IsReady(ctx context.Context) (bool, error)
}

// GraphDatastore is a subset of the datastore interface that is passed to
// graph resolvers.
type GraphDatastore interface {
	// QueryTuples creates a builder for reading tuples from the datastore.
	QueryTuples(resourceType, optionalResourceID, optionalRelation string, revision Revision) TupleQuery

	// ReverseQueryTuplesFromSubject creates a builder for reading tuples from
	// subject onward from the datastore.
	ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision Revision) ReverseTupleQuery

	// ReverseQueryTuplesFromSubjectRelation creates a builder for reading tuples
	// from a subject relation onward from the datastore.
	ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision Revision) ReverseTupleQuery

	// ReverseQueryTuplesFromSubjectNamespace creates a builder for reading
	// tuples from a subject namespace onward from the datastore.
	ReverseQueryTuplesFromSubjectNamespace(subjectNamespace string, revision Revision) ReverseTupleQuery

	// CheckRevision checks the specified revision to make sure it's valid and
	// hasn't been garbage collected.
	CheckRevision(ctx context.Context, revision Revision) error
}

// CommonTupleQuery is the common interface shared between TupleQuery and
// ReverseTupleQuery.
type CommonTupleQuery interface {
	// Execute runs the tuple query and returns a result iterator.
	Execute(ctx context.Context) (TupleIterator, error)

	// Limit sets a limit on the query.
	Limit(limit uint64) CommonTupleQuery
}

// ReverseTupleQuery is a builder for constructing reverse tuple queries.
type ReverseTupleQuery interface {
	CommonTupleQuery

	// WithObjectRelation filters to tuples with the given object relation on the
	// left hand side.
	WithObjectRelation(namespace string, relation string) ReverseTupleQuery
}

// TupleQuery is a builder for constructing tuple queries.
type TupleQuery interface {
	CommonTupleQuery

	// WithUserset adds a userset filter to the query.
	WithUsersetFilter(*v1.SubjectFilter) TupleQuery

	// WithUsersets adds multiple userset filters to the query.
	WithUsersets(usersets []*v0.ObjectAndRelation) TupleQuery
}

// TupleIterator is an iterator over matched tuples.
type TupleIterator interface {
	// Next returns the next tuple in the result set.
	Next() *v0.RelationTuple

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
