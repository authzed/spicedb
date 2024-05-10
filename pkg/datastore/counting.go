package datastore

import (
	"context"
	"strings"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// RelationshipCounter is a struct that represents a count of relationships that match a filter.
type RelationshipCounter struct {
	// Filter is the filter that the count represents.
	Filter *core.RelationshipFilter

	// Count is the count of relationships that match the filter.
	Count int

	// ComputedAtRevision is the revision at which the count was last computed. If NoRevision,
	// the count has never been computed.
	ComputedAtRevision Revision
}

// CounterRegisterer is an interface for registering and unregistering counts.
type CounterRegisterer interface {
	// RegisterCounter registers a count with the provided filter. If the filter already exists,
	// returns an error.
	RegisterCounter(ctx context.Context, filter *core.RelationshipFilter) error

	// UnregisterCounter unregisters a count with the provided filter. If the filter did not exist,
	// returns an error.
	UnregisterCounter(ctx context.Context, filter *core.RelationshipFilter) error

	// StoreCounterValue stores a count with the provided filter at the given revision. If the filter does not exist,
	// returns an error.
	StoreCounterValue(ctx context.Context, filter *core.RelationshipFilter, value int, computedAtRevision Revision) error
}

// CounterReader is an interface for reading counts.
type CounterReader interface {
	// CountRelationships returns the count of relationships that match the provided filter. If the filter is not
	// registered, returns an error.
	CountRelationships(ctx context.Context, filter *core.RelationshipFilter) (int, error)

	// LookupCounters returns all registered counters.
	LookupCounters(ctx context.Context) ([]RelationshipCounter, error)
}

// FilterStableName returns a string representation of a filter that can be used as a key in a map.
// This is guaranteed to be stable across versions and can be used to store and retrieve counters.
func FilterStableName(filter *core.RelationshipFilter) string {
	var sb strings.Builder
	sb.WriteString("f::")
	sb.WriteString(filter.ResourceType)
	sb.WriteString(":")
	sb.WriteString(filter.OptionalResourceId)
	if filter.OptionalResourceIdPrefix != "" {
		sb.WriteString(filter.OptionalResourceIdPrefix)
		sb.WriteString("%")
	}
	sb.WriteString("#")
	sb.WriteString(filter.OptionalRelation)

	if filter.OptionalSubjectFilter != nil {
		sb.WriteString("@")
		sb.WriteString(filter.OptionalSubjectFilter.SubjectType)
		sb.WriteString(":")
		sb.WriteString(filter.OptionalSubjectFilter.OptionalSubjectId)

		if filter.OptionalSubjectFilter.GetOptionalRelation() != nil {
			sb.WriteString("#")
			sb.WriteString(filter.OptionalSubjectFilter.GetOptionalRelation().Relation)
		}
	}

	return sb.String()
}
