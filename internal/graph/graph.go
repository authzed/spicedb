package graph

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"

	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Ellipsis relation is used to signify a semantic-free relationship.
const Ellipsis = "..."

var tracer = otel.Tracer("spicedb/internal/graph")

// CheckRequest contains the data for a single check request.
type CheckRequest struct {
	Start          *v0.ObjectAndRelation
	Goal           *v0.ObjectAndRelation
	AtRevision     decimal.Decimal
	DepthRemaining uint16
}

// CheckResult is the data that is returned by a single check or sub-check.
type CheckResult struct {
	IsMember bool
	Err      error
}

type ExpansionMode int

const (
	ShallowExpansion   ExpansionMode = 0
	RecursiveExpansion ExpansionMode = 1
)

// ExpandRequest contains the data for a single expand request.
type ExpandRequest struct {
	Start          *v0.ObjectAndRelation
	AtRevision     decimal.Decimal
	DepthRemaining uint16
	ExpansionMode  ExpansionMode
}

// ExpandResult is the data that is returned by a single expand or sub-expand.
type ExpandResult struct {
	Tree *v0.RelationTupleTreeNode
	Err  error
}

// LookupRequest contains the data for a single lookup request.
type LookupRequest struct {
	// StartRelation is the relation at which to start the lookup.
	StartRelation *v0.RelationReference

	// TargetONR is the target ONR that we are trying to reach.
	TargetONR *v0.ObjectAndRelation

	Limit          int
	AtRevision     decimal.Decimal
	DepthRemaining uint16
	DirectStack    *tuple.ONRSet
	TTUStack       *tuple.ONRSet
	DebugTracer    DebugTracer
}

// LookupResult is the data that is returned by a single lookup or sub-lookup.
type LookupResult struct {
	ResolvedObjects []*v0.ObjectAndRelation
	Err             error
}

// Dispatcher interface describes a method for passing subchecks off to additional machines.
type Dispatcher interface {
	// Check submits a single check request and returns its result.
	Check(ctx context.Context, req CheckRequest) CheckResult

	// Expand submits a single expand request and returns its result.
	Expand(ctx context.Context, req ExpandRequest) ExpandResult

	// Lookup submits a single lookup request and returns its result.
	Lookup(ctx context.Context, req LookupRequest) LookupResult
}

// ReduceableCheckFunc is a function that can be bound to a execution context.
type ReduceableCheckFunc func(ctx context.Context, resultChan chan<- CheckResult)

// Reducer is a type for the functions Any and All which combine check results.
type Reducer func(ctx context.Context, requests []ReduceableCheckFunc) CheckResult

// AlwaysFail is a ReduceableCheckFunc which will always fail when reduced.
func AlwaysFail(ctx context.Context, resultChan chan<- CheckResult) {
	resultChan <- CheckResult{IsMember: false, Err: NewAlwaysFailErr()}
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr CheckRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Str("request", tuple.String(&v0.RelationTuple{
		ObjectAndRelation: cr.Start,
		User: &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: cr.Goal,
			},
		},
	}))
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr CheckResult) MarshalZerologObject(e *zerolog.Event) {
	e.Bool("isMember", cr.IsMember).Err(cr.Err)
}

type checker interface {
	check(ctx context.Context, req CheckRequest, relation *v0.Relation) ReduceableCheckFunc
}

// ReduceableExpandFunc is a function that can be bound to a execution context.
type ReduceableExpandFunc func(ctx context.Context, resultChan chan<- ExpandResult)

// AlwaysFailExpand is a ReduceableExpandFunc which will always fail when reduced.
func AlwaysFailExpand(ctx context.Context, resultChan chan<- ExpandResult) {
	resultChan <- ExpandResult{Tree: nil, Err: NewAlwaysFailErr()}
}

// ExpandReducer is a type for the functions Any and All which combine check results.
type ExpandReducer func(
	ctx context.Context,
	start *v0.ObjectAndRelation,
	requests []ReduceableExpandFunc,
) ExpandResult

type expander interface {
	expand(ctx context.Context, req ExpandRequest, relation *v0.Relation) ReduceableExpandFunc
}

// MarshalZerologObject implements zerolog object marshalling.
func (er ExpandRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Str("expand", tuple.StringONR(er.Start))
}

// ReduceableLookupFunc is a function that can be bound to a execution context.
type ReduceableLookupFunc func(ctx context.Context, resultChan chan<- LookupResult)

// LookupReducer is a type for the functions which combine lookup results.
type LookupReducer func(ctx context.Context, limit int, requests []ReduceableLookupFunc) LookupResult

type lookupHandler interface {
	lookup(ctx context.Context, req LookupRequest) ReduceableLookupFunc
}

// MarshalZerologObject implements zerolog object marshalling.
func (lr LookupRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Str("lookup", tuple.StringONR(lr.TargetONR))
}
