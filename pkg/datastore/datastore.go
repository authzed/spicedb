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

var Engines []string

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

// FilterMaximumIDCount is the maximum number of resource IDs or subject IDs that can be sent into
// a filter.
const FilterMaximumIDCount = 100

// RevisionChanges represents the changes in a single transaction.
type RevisionChanges struct {
	Revision Revision
	Changes  []*core.RelationTupleUpdate
}

// RelationshipsFilter is a filter for relationships.
type RelationshipsFilter struct {
	// ResourceType is the namespace/type for the resources to be found.
	ResourceType string

	// OptionalResourceIds are the IDs of the resources to find. If nil empty, any resource ID will be allowed.
	OptionalResourceIds []string

	// OptionalResourceRelation is the relation of the resource to find. If empty, any relation is allowed.
	OptionalResourceRelation string

	// OptionalSubjectsFilter is the filter to use for subjects of the relationship. If nil, all subjects are allowed.
	OptionalSubjectsFilter *SubjectsFilter
}

// RelationshipsFilterFromPublicFilter constructs a datastore RelationshipsFilter from an API-defined RelationshipFilter.
func RelationshipsFilterFromPublicFilter(filter *v1.RelationshipFilter) RelationshipsFilter {
	var resourceIds []string
	if filter.OptionalResourceId != "" {
		resourceIds = []string{filter.OptionalResourceId}
	}

	var subjectsFilter *SubjectsFilter
	if filter.OptionalSubjectFilter != nil {
		var subjectIds []string
		if filter.OptionalSubjectFilter.OptionalSubjectId != "" {
			subjectIds = []string{filter.OptionalSubjectFilter.OptionalSubjectId}
		}

		relationFilter := SubjectRelationFilter{}

		if filter.OptionalSubjectFilter.OptionalRelation != nil {
			relation := filter.OptionalSubjectFilter.OptionalRelation.GetRelation()
			if relation != "" {
				relationFilter = relationFilter.WithNonEllipsisRelation(relation)
			} else {
				relationFilter = relationFilter.WithEllipsisRelation()
			}
		}

		subjectsFilter = &SubjectsFilter{
			SubjectType:        filter.OptionalSubjectFilter.SubjectType,
			OptionalSubjectIds: subjectIds,
			RelationFilter:     relationFilter,
		}
	}

	return RelationshipsFilter{
		ResourceType:             filter.ResourceType,
		OptionalResourceIds:      resourceIds,
		OptionalResourceRelation: filter.OptionalRelation,
		OptionalSubjectsFilter:   subjectsFilter,
	}
}

// SubjectsFilter is a filter for subjects.
type SubjectsFilter struct {
	// SubjectType is the namespace/type for the subjects to be found.
	SubjectType string

	// OptionalSubjectIds are the IDs of the subjects to find. If nil or empty, any subject ID will be allowed.
	OptionalSubjectIds []string

	// RelationFilter is the filter to use for the relation(s) of the subjects. If neither field
	// is set, any relation is allowed.
	RelationFilter SubjectRelationFilter
}

// SubjectRelationFilter is the filter to use for relation(s) of subjects being queried.
type SubjectRelationFilter struct {
	// NonEllipsisRelation is the relation of the subject type to find. If empty,
	// IncludeEllipsisRelation must be true.
	NonEllipsisRelation string

	// IncludeEllipsisRelation, if true, indicates that the ellipsis relation
	// should be included as an option.
	IncludeEllipsisRelation bool
}

// WithEllipsisRelation indicates that the subject filter should include the ellipsis relation
// as an option for the subjects' relation.
func (sf SubjectRelationFilter) WithEllipsisRelation() SubjectRelationFilter {
	sf.IncludeEllipsisRelation = true
	return sf
}

// WithNonEllipsisRelation indicates that the specified non-ellipsis relation should be included as an
// option for the subjects' relation.
func (sf SubjectRelationFilter) WithNonEllipsisRelation(relation string) SubjectRelationFilter {
	sf.NonEllipsisRelation = relation
	return sf
}

// IsEmpty returns true if the subject relation filter is empty.
func (sf SubjectRelationFilter) IsEmpty() bool {
	return !sf.IncludeEllipsisRelation && sf.NonEllipsisRelation == ""
}

type Reader interface {
	// QueryRelationships reads relationships, starting from the resource side.
	QueryRelationships(
		ctx context.Context,
		filter RelationshipsFilter,
		options ...options.QueryOptionsOption,
	) (RelationshipIterator, error)

	// ReverseQueryRelationships reads relationships, starting from the subject.
	ReverseQueryRelationships(
		ctx context.Context,
		subjectFilter SubjectsFilter,
		options ...options.ReverseQueryOptionsOption,
	) (RelationshipIterator, error)

	// ReadNamespace reads a namespace definition and the revision at which it was created or
	// last written. It returns an instance of ErrNamespaceNotFound if not found.
	ReadNamespace(ctx context.Context, nsName string) (ns *core.NamespaceDefinition, lastWritten Revision, err error)

	// ListNamespaces lists all namespaces defined.
	ListNamespaces(ctx context.Context) ([]*core.NamespaceDefinition, error)
}

type ReadWriteTransaction interface {
	Reader

	// WriteRelationships takes a list of tuple mutations and applies them to the datastore.
	WriteRelationships(mutations []*core.RelationTupleUpdate) error

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
	// SnapshotReader creates a read-only handle that reads the datastore at the specified revision.
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

	// Features returns an object representing what features this
	// datastore can support.
	Features(ctx context.Context) (*Features, error)

	// Statistics returns relevant values about the data contained in this cluster.
	Statistics(ctx context.Context) (Stats, error)

	// Close closes the data store.
	Close() error
}

// Feature represents a capability that a datastore can support, plus an
// optional message explaining the feature is available (or not).
type Feature struct {
	Enabled bool
	Reason  string
}

// Features holds values that represent what features a database can support.
type Features struct {
	// Watch is enabled if the underlying datastore can support the Watch api.
	Watch Feature
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
	// in the datastore. Computing it should use a lightweight method such as reading
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

	// Err after receiving a nil response, the caller must check for an error.
	Err() error

	// Close cancels the query and closes any open connections.
	Close()
}

// Revision is a type alias to make changing the revision type a bit
// easier if we need to do it in the future. Implementations should code
// directly against decimal.Decimal when creating or parsing.
type Revision = decimal.Decimal

// NoRevision is a zero type for the revision that will make changing the
// revision type in the future a bit easier if necessary. Implementations
// should use any time they want to signal an empty/error revision.
var NoRevision Revision
