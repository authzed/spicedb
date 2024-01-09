package datastore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/pkg/tuple"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore/options"
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
		quoted = append(quoted, `"`+id+`"`)
	}
	return strings.Join(quoted, ", ")
}

// Ellipsis is a special relation that is assumed to be valid on the right
// hand side of a tuple.
const Ellipsis = "..."

// FilterMaximumIDCount is the maximum number of resource IDs or subject IDs that can be sent into
// a filter.
const FilterMaximumIDCount uint16 = 100

// RevisionChanges represents the changes in a single transaction.
type RevisionChanges struct {
	Revision Revision

	// RelationshipChanges are any relationships that were changed at this revision.
	RelationshipChanges []*core.RelationTupleUpdate

	// ChangedDefinitions are any definitions that were added or changed at this revision.
	ChangedDefinitions []SchemaDefinition

	// DeletedNamespaces are any namespaces that were deleted.
	DeletedNamespaces []string

	// DeletedCaveats are any caveats that were deleted.
	DeletedCaveats []string

	// IsCheckpoint, if true, indicates that the datastore has reported all changes
	// up until and including the Revision and that no additional schema updates can
	// have occurred before this point.
	IsCheckpoint bool
}

func (rc *RevisionChanges) MarshalZerologObject(e *zerolog.Event) {
	e.Str("revision", rc.Revision.String())
	e.Bool("is-checkpoint", rc.IsCheckpoint)
	e.Array("deleted-namespaces", strArray(rc.DeletedNamespaces))
	e.Array("deleted-caveats", strArray(rc.DeletedCaveats))

	changedNames := make([]string, 0, len(rc.ChangedDefinitions))
	for _, cd := range rc.ChangedDefinitions {
		changedNames = append(changedNames, fmt.Sprintf("%T:%s", cd, cd.GetName()))
	}

	e.Array("changed-definitions", strArray(changedNames))
	e.Int("num-changed-relationships", len(rc.RelationshipChanges))
}

// RelationshipsFilter is a filter for relationships.
type RelationshipsFilter struct {
	// ResourceType is the namespace/type for the resources to be found.
	ResourceType string

	// OptionalResourceIds are the IDs of the resources to find. If nil empty, any resource ID will be allowed.
	OptionalResourceIds []string

	// OptionalResourceRelation is the relation of the resource to find. If empty, any relation is allowed.
	OptionalResourceRelation string

	// OptionalSubjectsSelectors is the selectors to use for subjects of the relationship. If nil, all subjects are allowed.
	// If specified, relationships matching *any* selector will be returned.
	OptionalSubjectsSelectors []SubjectsSelector

	// OptionalCaveatName is the filter to use for caveated relationships, filtering by a specific caveat name.
	// If nil, all caveated and non-caveated relationships are allowed
	OptionalCaveatName string
}

// RelationshipsFilterFromPublicFilter constructs a datastore RelationshipsFilter from an API-defined RelationshipFilter.
func RelationshipsFilterFromPublicFilter(filter *v1.RelationshipFilter) RelationshipsFilter {
	var resourceIds []string
	if filter.OptionalResourceId != "" {
		resourceIds = []string{filter.OptionalResourceId}
	}

	var subjectsSelectors []SubjectsSelector
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

		subjectsSelectors = append(subjectsSelectors, SubjectsSelector{
			OptionalSubjectType: filter.OptionalSubjectFilter.SubjectType,
			OptionalSubjectIds:  subjectIds,
			RelationFilter:      relationFilter,
		})
	}

	return RelationshipsFilter{
		ResourceType:              filter.ResourceType,
		OptionalResourceIds:       resourceIds,
		OptionalResourceRelation:  filter.OptionalRelation,
		OptionalSubjectsSelectors: subjectsSelectors,
	}
}

// SubjectsSelector is a selector for subjects.
type SubjectsSelector struct {
	// OptionalSubjectType is the namespace/type for the subjects to be found, if any.
	OptionalSubjectType string

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

	// OnlyNonEllipsisRelations, if true, indicates that only non-ellipsis relations
	// should be included.
	OnlyNonEllipsisRelations bool
}

// WithOnlyNonEllipsisRelations indicates that only non-ellipsis relations should be included.
func (sf SubjectRelationFilter) WithOnlyNonEllipsisRelations() SubjectRelationFilter {
	sf.OnlyNonEllipsisRelations = true
	sf.NonEllipsisRelation = ""
	sf.IncludeEllipsisRelation = false
	return sf
}

// WithEllipsisRelation indicates that the subject filter should include the ellipsis relation
// as an option for the subjects' relation.
func (sf SubjectRelationFilter) WithEllipsisRelation() SubjectRelationFilter {
	sf.IncludeEllipsisRelation = true
	sf.OnlyNonEllipsisRelations = false
	return sf
}

// WithNonEllipsisRelation indicates that the specified non-ellipsis relation should be included as an
// option for the subjects' relation.
func (sf SubjectRelationFilter) WithNonEllipsisRelation(relation string) SubjectRelationFilter {
	sf.NonEllipsisRelation = relation
	sf.OnlyNonEllipsisRelations = false
	return sf
}

// WithRelation indicates that the specified relation should be included as an
// option for the subjects' relation.
func (sf SubjectRelationFilter) WithRelation(relation string) SubjectRelationFilter {
	if relation == tuple.Ellipsis {
		return sf.WithEllipsisRelation()
	}
	return sf.WithNonEllipsisRelation(relation)
}

// IsEmpty returns true if the subject relation filter is empty.
func (sf SubjectRelationFilter) IsEmpty() bool {
	return !sf.IncludeEllipsisRelation && sf.NonEllipsisRelation == "" && !sf.OnlyNonEllipsisRelations
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

func (sf SubjectsFilter) AsSelector() SubjectsSelector {
	return SubjectsSelector{
		OptionalSubjectType: sf.SubjectType,
		OptionalSubjectIds:  sf.OptionalSubjectIds,
		RelationFilter:      sf.RelationFilter,
	}
}

// SchemaDefinition represents a namespace or caveat definition under a schema.
type SchemaDefinition interface {
	GetName() string
}

// RevisionedDefinition holds a schema definition and its last updated revision.
type RevisionedDefinition[T SchemaDefinition] struct {
	// Definition is the namespace or caveat definition.
	Definition T

	// LastWrittenRevision is the revision at which the namespace or caveat was last updated.
	LastWrittenRevision Revision
}

func (rd RevisionedDefinition[T]) GetLastWrittenRevision() Revision {
	return rd.LastWrittenRevision
}

// RevisionedNamespace is a revisioned version of a namespace definition.
type RevisionedNamespace = RevisionedDefinition[*core.NamespaceDefinition]

// Reader is an interface for reading relationships from the datastore.
type Reader interface {
	CaveatReader

	// QueryRelationships reads relationships, starting from the resource side.
	QueryRelationships(
		ctx context.Context,
		filter RelationshipsFilter,
		options ...options.QueryOptionsOption,
	) (RelationshipIterator, error)

	// ReverseQueryRelationships reads relationships, starting from the subject.
	ReverseQueryRelationships(
		ctx context.Context,
		subjectsFilter SubjectsFilter,
		options ...options.ReverseQueryOptionsOption,
	) (RelationshipIterator, error)

	// ReadNamespaceByName reads a namespace definition and the revision at which it was created or
	// last written. It returns an instance of ErrNamespaceNotFound if not found.
	ReadNamespaceByName(ctx context.Context, nsName string) (ns *core.NamespaceDefinition, lastWritten Revision, err error)

	// ListAllNamespaces lists all namespaces defined.
	ListAllNamespaces(ctx context.Context) ([]RevisionedNamespace, error)

	// LookupNamespacesWithNames finds all namespaces with the matching names.
	LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]RevisionedNamespace, error)
}

type ReadWriteTransaction interface {
	Reader
	CaveatStorer

	// WriteRelationships takes a list of tuple mutations and applies them to the datastore.
	WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error

	// DeleteRelationships deletes all Relationships that match the provided filter.
	DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error

	// WriteNamespaces takes proto namespace definitions and persists them.
	WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error

	// DeleteNamespaces deletes namespaces including associated relationships.
	DeleteNamespaces(ctx context.Context, nsNames ...string) error

	// BulkLoad takes a relationship source iterator, and writes all of the
	// relationships to the backing datastore in an optimized fashion. This
	// method can and will omit checks and otherwise cut corners in the
	// interest of performance, and should not be relied upon for OLTP-style
	// workloads.
	BulkLoad(ctx context.Context, iter BulkWriteRelationshipSource) (uint64, error)
}

// TxUserFunc is a type for the function that users supply when they invoke a read-write transaction.
type TxUserFunc func(context.Context, ReadWriteTransaction) error

// ReadyState represents the ready state of the datastore.
type ReadyState struct {
	// Message is a human-readable status message for the current state.
	Message string

	// IsReady indicates whether the datastore is ready.
	IsReady bool
}

// BulkWriteRelationshipSource is an interface for transferring relationships
// to a backing datastore with a zero-copy methodology.
type BulkWriteRelationshipSource interface {
	// Next Returns a pointer to a relation tuple if one is available, or nil if
	// there are no more or there was an error.
	//
	// Note: sources may re-use the same memory address for every tuple, data
	// may change on every call to next even if the pointer has not changed.
	Next(ctx context.Context) (*core.RelationTuple, error)
}

type WatchContent int

const (
	WatchRelationships WatchContent = 1 << 0
	WatchSchema        WatchContent = 1 << 1
	WatchCheckpoints   WatchContent = 1 << 2
)

// WatchOptions are options for a Watch call.
type WatchOptions struct {
	// Content is the content to watch.
	Content WatchContent

	// CheckpointInterval is the interval to use for checkpointing in the watch.
	// If given the zero value, the datastore's default will be used. If smaller
	// than the datastore's minimum, the minimum will be used.
	CheckpointInterval time.Duration

	// WatchBufferLength is the length of the buffer for the watch channel. If
	// given the zero value, the datastore's default will be used.
	WatchBufferLength uint16

	// WatchBufferWriteTimeout is the timeout for writing to the watch channel.
	// If given the zero value, the datastore's default will be used.
	WatchBufferWriteTimeout time.Duration
}

// WatchJustRelationships returns watch options for just relationships.
func WatchJustRelationships() WatchOptions {
	return WatchOptions{
		Content: WatchRelationships,
	}
}

// WatchJustSchema returns watch options for just schema.
func WatchJustSchema() WatchOptions {
	return WatchOptions{
		Content: WatchSchema,
	}
}

// WithCheckpointInterval sets the checkpoint interval on a watch options, returning
// an updated options struct.
func (wo WatchOptions) WithCheckpointInterval(interval time.Duration) WatchOptions {
	return WatchOptions{
		Content:            wo.Content,
		CheckpointInterval: interval,
	}
}

// Datastore represents tuple access for a single namespace.
type Datastore interface {
	// SnapshotReader creates a read-only handle that reads the datastore at the specified revision.
	// Any errors establishing the reader will be returned by subsequent calls.
	SnapshotReader(Revision) Reader

	// ReadWriteTx tarts a read/write transaction, which will be committed if no error is
	// returned and rolled back if an error is returned.
	ReadWriteTx(context.Context, TxUserFunc, ...options.RWTOptionsOption) (Revision, error)

	// OptimizedRevision gets a revision that will likely already be replicated
	// and will likely be shared amongst many queries.
	OptimizedRevision(ctx context.Context) (Revision, error)

	// HeadRevision gets a revision that is guaranteed to be at least as fresh as
	// right now.
	HeadRevision(ctx context.Context) (Revision, error)

	// CheckRevision checks the specified revision to make sure it's valid and
	// hasn't been garbage collected.
	CheckRevision(ctx context.Context, revision Revision) error

	// RevisionFromString will parse the revision text and return the specific type of Revision
	// used by the specific datastore implementation.
	RevisionFromString(serialized string) (Revision, error)

	// Watch notifies the caller about changes to the datastore, based on the specified options.
	//
	// All events following afterRevision will be sent to the caller.
	Watch(ctx context.Context, afterRevision Revision, options WatchOptions) (<-chan *RevisionChanges, <-chan error)

	// ReadyState returns a state indicating whether the datastore is ready to accept data.
	// Datastores that require database schema creation will return not-ready until the migrations
	// have been run to create the necessary tables.
	ReadyState(ctx context.Context) (ReadyState, error)

	// Features returns an object representing what features this
	// datastore can support.
	Features(ctx context.Context) (*Features, error)

	// Statistics returns relevant values about the data contained in this cluster.
	Statistics(ctx context.Context) (Stats, error)

	// Close closes the data store.
	Close() error
}

type strArray []string

// MarshalZerologArray implements zerolog array marshalling.
func (strs strArray) MarshalZerologArray(a *zerolog.Array) {
	for _, val := range strs {
		a.Str(val)
	}
}

// StartableDatastore is an optional extension to the datastore interface that, when implemented,
// provides the ability for callers to start background operations on the datastore.
type StartableDatastore interface {
	Datastore

	// Start starts any background operations on the datastore. The context provided, if canceled, will
	// also cancel the background operation(s) on the datastore.
	Start(ctx context.Context) error
}

// RepairOperation represents a single kind of repair operation that can be run in a repairable
// datastore.
type RepairOperation struct {
	// Name is the command-line name for the repair operation.
	Name string

	// Description is the human-readable description for the repair operation.
	Description string
}

// RepairableDatastore is an optional extension to the datastore interface that, when implemented,
// provides the ability for callers to repair the datastore's data in some fashion.
type RepairableDatastore interface {
	Datastore

	// Repair runs the repair operation on the datastore.
	Repair(ctx context.Context, operationName string, outputProgress bool) error

	// RepairOperations returns the available repair operations for the datastore.
	RepairOperations() []RepairOperation
}

// UnwrappableDatastore represents a datastore that can be unwrapped into the underlying
// datastore.
type UnwrappableDatastore interface {
	// Unwrap returns the wrapped datastore.
	Unwrap() Datastore
}

// UnwrapAs recursively attempts to unwrap the datastore into the specified type
// In none of the layers of the datastore implement the specified type, nil is returned.
func UnwrapAs[T any](datastore Datastore) T {
	var ds T
	uwds := datastore

	for {
		var ok bool
		ds, ok = uwds.(T)
		if ok {
			break
		}

		wds, ok := uwds.(UnwrappableDatastore)
		if !ok {
			break
		}

		uwds = wds.Unwrap()
	}

	return ds
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

	// Cursor returns a cursor that can be used to resume reading of relationships
	// from the last relationship returned. Only applies if a sort ordering was
	// requested.
	Cursor() (options.Cursor, error)

	// Err after receiving a nil response, the caller must check for an error.
	Err() error

	// Close cancels the query and closes any open connections.
	Close()
}

// Revision is an interface for a comparable revision type that can be different for
// each datastore implementation.
type Revision interface {
	fmt.Stringer

	// Equal returns whether the revisions should be considered equal.
	Equal(Revision) bool

	// GreaterThan returns whether the receiver is probably greater than the right hand side.
	GreaterThan(Revision) bool

	// LessThan returns whether the receiver is probably less than the right hand side.
	LessThan(Revision) bool
}

type nilRevision struct{}

func (nilRevision) Equal(rhs Revision) bool {
	return rhs == NoRevision
}

func (nilRevision) GreaterThan(_ Revision) bool {
	return false
}

func (nilRevision) LessThan(_ Revision) bool {
	return true
}

func (nilRevision) String() string {
	return "nil"
}

// NoRevision is a zero type for the revision that will make changing the
// revision type in the future a bit easier if necessary. Implementations
// should use any time they want to signal an empty/error revision.
var NoRevision Revision = nilRevision{}
