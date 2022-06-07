package options

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

//go:generate go run github.com/ecordell/optgen -output zz_generated.query_options.go . QueryOptions ReverseQueryOptions

// QueryOptions are the options that can affect the results of a normal forward query.
type QueryOptions struct {
	Limit    *uint64
	Usersets []*core.ObjectAndRelation
}

// ReverseQueryOptions are the options that can affect the results of a reverse query.
type ReverseQueryOptions struct {
	ReverseLimit *uint64
	ResRelation  *ResourceRelation
}

// ResourceRelation combines a resource object type and relation.
type ResourceRelation struct {
	Namespace string
	Relation  string
}

var (
	one = uint64(1)

	// LimitOne is a constant *uint64 that can be used with WithLimit requests.
	LimitOne = &one
)
