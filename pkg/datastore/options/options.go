package options

import (
	"context"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

//go:generate go run github.com/ecordell/optgen -output zz_generated.query_options.go . QueryOptions ReverseQueryOptions RWTOptions
//go:generate go run github.com/ecordell/optgen -output zz_generated.delete_options.go . DeleteOptions

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

type Cursor *tuple.Relationship

func ToCursor(r tuple.Relationship) Cursor {
	spiceerrors.DebugAssert(r.ValidateNotEmpty, "cannot create cursor from empty relationship")
	return Cursor(&r)
}

func ToRelationship(c Cursor) *tuple.Relationship {
	if c == nil {
		return nil
	}
	return (*tuple.Relationship)(c)
}

// SQLCheckAssertionForTest is a function that can be used to assert a condition on the SQL query string.
// Assertions will only be run during testing and only apply in datastores that support SQL.
type SQLCheckAssertionForTest func(sql string)

// SQLIndexInformation holds the expected index names for a SQL query.
type SQLIndexInformation struct {
	// ExpectedIndexNames are the name(s) of the index(es) that are expected to be used by this
	// SQL query.
	ExpectedIndexNames []string
}

// SQLExplainCallbackForTest is a callback invoked with the explain plan of the SQL query string.
type SQLExplainCallbackForTest func(ctx context.Context, sql string, args []any, shape queryshape.Shape, explain string, expectedIndexes SQLIndexInformation) error

// QueryOptions are the options that can affect the results of a normal forward query.
type QueryOptions struct {
	Limit          *uint64   `debugmap:"visible"`
	Sort           SortOrder `debugmap:"visible"`
	After          Cursor    `debugmap:"visible"`
	SkipCaveats    bool      `debugmap:"visible"`
	SkipExpiration bool      `debugmap:"visible"`

	// SQLCheckAssertionForTest is a function that can be used to assert a condition on the SQL query string.
	// For testing and validation only.
	SQLCheckAssertionForTest SQLCheckAssertionForTest `debugmap:"visible"`

	// SQLExplainCallbackForTest is a callback invoked with the explain plan of the SQL query string.
	// For testing and validation only.
	SQLExplainCallbackForTest SQLExplainCallbackForTest `debugmap:"visible"`

	// QueryShape is the marked shape of the query.
	// For testing and validation only.
	QueryShape queryshape.Shape `debugmap:"visible"`
}

// ReverseQueryOptions are the options that can affect the results of a reverse query.
type ReverseQueryOptions struct {
	ResRelation *ResourceRelation `debugmap:"visible"`

	LimitForReverse *uint64   `debugmap:"visible"`
	SortForReverse  SortOrder `debugmap:"visible"`
	AfterForReverse Cursor    `debugmap:"visible"`

	// SQLExplainCallbackForTestForReverse is a callback invoked with the explain plan of the SQL query string.
	// For testing and validation only.
	SQLExplainCallbackForTestForReverse SQLExplainCallbackForTest `debugmap:"visible"`

	// QueryShapeForReverse is the marked shape of the reverse query.
	// For testing and validation only.
	QueryShapeForReverse queryshape.Shape `debugmap:"visible"`
}

// ResourceRelation combines a resource object type and relation.
type ResourceRelation struct {
	Namespace string
	Relation  string
}

// RWTOptions are options that can affect the way a read-write transaction is
// executed.
type RWTOptions struct {
	DisableRetries bool             `debugmap:"visible"`
	Metadata       *structpb.Struct `debugmap:"visible"`
}

// DeleteOptions are the options that can affect the results of a delete relationships
// operation.
type DeleteOptions struct {
	DeleteLimit *uint64 `debugmap:"visible"`
}

var (
	one = uint64(1)

	// LimitOne is a constant *uint64 that can be used with WithLimit requests.
	LimitOne = &one
)
