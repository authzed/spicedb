package datastore

import (
	"context"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// RelationshipCounter is a struct that represents a count of relationships that match a filter.
type RelationshipCounter struct {
	// Name is the name of the counter.
	Name string

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
	// RegisterCounter registers a count with the provided filter. If the counter already exists,
	// returns an error.
	RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error

	// UnregisterCounter unregisters a counter. If the counter did not exist, returns an error.
	UnregisterCounter(ctx context.Context, name string) error

	// StoreCounterValue stores a count for the counter with the given name, at the given revision.
	// If the counter does not exist, returns an error.
	StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision Revision) error
}

// CounterReader is an interface for reading counts.
type CounterReader interface {
	// CountRelationships returns the count of relationships that match the provided counter. If the counter is not
	// registered, returns an error.
	CountRelationships(ctx context.Context, name string) (int, error)

	// LookupCounters returns all registered counters.
	LookupCounters(ctx context.Context) ([]RelationshipCounter, error)
}
