package datastore

import (
	"context"

	"github.com/shopspring/decimal"

	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
)

const (
	// Ellipsis is a special relation that is assumed to be valid on the right
	// hand side of a tuple.
	Ellipsis = "..."
)

// RevisionChanges represents the changes in a single transaction.
type RevisionChanges struct {
	Revision Revision
	Changes  []*pb.RelationTupleUpdate
}

// Datastore represents tuple access for a single namespace.
type Datastore interface {
	GraphDatastore

	// WriteTuples takes a list of existing tuples that must exist, and a list of tuple
	// mutations and applies it to the datastore for the specified namespace.
	WriteTuples(ctx context.Context, preconditions []*pb.RelationTuple, mutations []*pb.RelationTupleUpdate) (Revision, error)

	// Revision gets the currently replicated revision for this datastore.
	Revision(ctx context.Context) (Revision, error)

	// SyncRevision gets a revision that is guaranteed to be at least as fresh as right now.
	SyncRevision(ctx context.Context) (Revision, error)

	// Watch notifies the caller about all changes to tuples.
	//
	// All events following afterRevision will be sent to the caller.
	Watch(ctx context.Context, afterRevision Revision) (<-chan *RevisionChanges, <-chan error)

	// WriteNamespace takes a proto namespace definition and persists it,
	// returning the version of the namespace that was created.
	WriteNamespace(ctx context.Context, newConfig *pb.NamespaceDefinition) (Revision, error)

	// ReadNamespace reads a namespace definition and version and returns it if found.
	ReadNamespace(ctx context.Context, nsName string) (*pb.NamespaceDefinition, Revision, error)

	// DeleteNamespace deletes a namespace and any associated tuples.
	DeleteNamespace(ctx context.Context, nsName string) (Revision, error)
}

// GraphDatastore is a subset of the datastore interface that is passed to graph resolvers.
type GraphDatastore interface {
	// QueryTuples creates a builder for reading tuples from the datastore.
	QueryTuples(namespace string, revision Revision) TupleQuery

	// ReverseQueryTuplesFromSubject creates a builder for reading tuples from subject onward
	// from the datastore.
	ReverseQueryTuplesFromSubject(subject *pb.ObjectAndRelation, revision Revision) ReverseTupleQuery

	// ReverseQueryTuplesFromSubjectRelation creates a builder for reading tuples from a subject
	// relation onward from the datastore.
	ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision Revision) ReverseTupleQuery

	// CheckRevision checks the specified revision to make sure it's valid and hasn't been
	// garbage collected.
	CheckRevision(ctx context.Context, revision Revision) error
}

// CommonTupleQuery is the common interface shared between TupleQuery and ReverseTupleQuery.
type CommonTupleQuery interface {
	// Execute runs the tuple query and returns a result iterator.
	Execute(ctx context.Context) (TupleIterator, error)

	// Limit sets a limit on the query.
	Limit(limit uint64) CommonTupleQuery
}

// ReverseTupleQuery is a builder for constructing reverse tuple queries.
type ReverseTupleQuery interface {
	CommonTupleQuery

	// WithObjectRelation filters to tuples with the given object relation on the left hand side.
	WithObjectRelation(namespace string, relation string) ReverseTupleQuery
}

// TupleQuery is a builder for constructing tuple queries.
type TupleQuery interface {
	CommonTupleQuery

	// WithObjectID adds an object ID filter to the query.
	WithObjectID(objectID string) TupleQuery

	// WithRelation adds a relation filter to the query.
	WithRelation(relation string) TupleQuery

	// WithUserset adds a userset filter to the query.
	WithUserset(userset *pb.ObjectAndRelation) TupleQuery
}

// TupleIterator is an iterator over matched tuples.
type TupleIterator interface {
	// Next returns the next tuple in the result set.
	Next() *pb.RelationTuple

	// After receiving a nil response, the caller must check for an error.
	Err() error

	// Close cancels the query and closes any open connections.
	Close()
}

// Add a type alias to make changing the revision type a little bit easier if we need to do it in
// the future. Implementations should code directly against decimal.Decimal when creating or parsing.
type Revision = decimal.Decimal

// A zero type for the revision that will make changing the revision type in the future a bit easier
// if necessary. Implementations should use any time they want to signal an empty/error revision.
var NoRevision Revision
