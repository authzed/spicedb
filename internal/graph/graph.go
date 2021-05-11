package graph

import (
	"context"
	"errors"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"

	"github.com/authzed/spicedb/internal/namespace"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Ellipsis relation is used to signify a semantic-free relationship.
const Ellipsis = "..."

// ErrAlwaysFail is returned when an internal error leads to an operation
// guaranteed to fail.
var ErrAlwaysFail = errors.New("always fail")

var tracer = otel.Tracer("spicedb/internal/graph")

// Publicly exposed errors from methods in this package.
var (
	ErrNamespaceNotFound = errors.New("namespace not found")
	ErrRelationNotFound  = errors.New("relation not found")
	ErrRequestCanceled   = errors.New("request canceled")
)

// CheckRequest contains the data for a single check request.
type CheckRequest struct {
	Start          *pb.ObjectAndRelation
	Goal           *pb.ObjectAndRelation
	AtRevision     uint64
	DepthRemaining uint16
}

// CheckResult is the data that is returned by a single check or sub-check.
type CheckResult struct {
	IsMember bool
	Err      error
}

// ExpandRequest contains the data for a single expand request.
type ExpandRequest struct {
	Start          *pb.ObjectAndRelation
	AtRevision     uint64
	DepthRemaining uint16
}

// ExpandResult is the data that is returned by a single expand or sub-expand.
type ExpandResult struct {
	Tree *pb.RelationTupleTreeNode
	Err  error
}

// LookupRequest contains the data for a single lookup request.
type LookupRequest struct {
	// Start is the starting ONR for this step of the lookup.
	Start *pb.ObjectAndRelation

	// TargetRelation is the relation to which the walk should lookup.
	TargetRelation *pb.RelationReference

	// ReductionNodeID, if none empty, means that the request is performing lookup
	// under a reduction node with the given ID.
	ReductionNodeID namespace.NodeID

	// IsRootRequest, if true, indicates that this request is part of the root request
	// and reduction should occur if present.
	IsRootRequest bool

	// PostReductionRequest indicates that this request was created as a result of reduction.
	PostReductionRequest bool

	Limit          int
	AtRevision     uint64
	DepthRemaining uint16
}

type ResolvedObject struct {
	ONR             *pb.ObjectAndRelation
	ReductionNodeID namespace.NodeID
}

// LookupResult is the data that is returned by a single lookup or sub-lookup.
type LookupResult struct {
	ResolvedObjects []ResolvedObject
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
	resultChan <- CheckResult{IsMember: false, Err: ErrAlwaysFail}
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr CheckRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Str("request", tuple.String(&pb.RelationTuple{
		ObjectAndRelation: cr.Start,
		User: &pb.User{
			UserOneof: &pb.User_Userset{
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
	check(ctx context.Context, req CheckRequest, relation *pb.Relation) ReduceableCheckFunc
}

// ReduceableExpandFunc is a function that can be bound to a execution context.
type ReduceableExpandFunc func(ctx context.Context, resultChan chan<- ExpandResult)

// AlwaysFailExpand is a ReduceableExpandFunc which will always fail when reduced.
func AlwaysFailExpand(ctx context.Context, resultChan chan<- ExpandResult) {
	resultChan <- ExpandResult{Tree: nil, Err: ErrAlwaysFail}
}

// ExpandReducer is a type for the functions Any and All which combine check results.
type ExpandReducer func(
	ctx context.Context,
	start *pb.ObjectAndRelation,
	requests []ReduceableExpandFunc,
) ExpandResult

type expander interface {
	expand(ctx context.Context, req ExpandRequest, relation *pb.Relation) ReduceableExpandFunc
}

// MarshalZerologObject implements zerolog object marshalling.
func (er ExpandRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Str("expand", tuple.StringONR(er.Start))
}

// ReduceableLookupFunc is a function that can be bound to a execution context.
type ReduceableLookupFunc func(ctx context.Context, resultChan chan<- LookupResult)

type lookupHandler interface {
	lookup(ctx context.Context, req LookupRequest) ReduceableLookupFunc
}

// MarshalZerologObject implements zerolog object marshalling.
func (lr LookupRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Str("lookup", tuple.StringONR(lr.Start))
}
