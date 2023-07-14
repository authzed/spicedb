package options

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

//go:generate go run github.com/ecordell/optgen -output zz_generated.query_options.go . QueryOptions ReverseQueryOptions RWTOptions

// SortOrder is an enum which represents the order in which the caller would like
// the data returned.
type SortOrder int8

const (
	// Unsorted lets the underlying datastore choose the order, or no order at all
	Unsorted SortOrder = iota

	// ByResource sorts the relationships by the resource component first
	ByResource

	// BySubject sorts the relationships by the subject component first. Note that
	// BySubject might be quite a bit slower than ByResource, as relationships are
	// indexed by resource.
	BySubject
)

type Cursor *core.RelationTuple

// QueryOptions are the options that can affect the results of a normal forward query.
type QueryOptions struct {
	Limit *uint64   `debugmap:"visible"`
	Sort  SortOrder `debugmap:"visible"`
	After Cursor    `debugmap:"visible"`
}

// ReverseQueryOptions are the options that can affect the results of a reverse query.
type ReverseQueryOptions struct {
	ResRelation *ResourceRelation `debugmap:"visible"`

	LimitForReverse *uint64   `debugmap:"visible"`
	SortForReverse  SortOrder `debugmap:"visible"`
	AfterForReverse Cursor    `debugmap:"visible"`
}

// ResourceRelation combines a resource object type and relation.
type ResourceRelation struct {
	Namespace string
	Relation  string
}

// RWTOptions are options that can affect the way a read-write transaction is
// executed.
type RWTOptions struct {
	DisableRetries bool `debugmap:"visible"`
}

var (
	one = uint64(1)

	// LimitOne is a constant *uint64 that can be used with WithLimit requests.
	LimitOne = &one
)
