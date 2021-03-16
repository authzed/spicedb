package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Ellipsis relation is used to signify a semantic-free relationship.
const Ellipsis = "..."

// ErrAlwaysFail is returned when an internal error leads to an operation
// guaranteed to fail.
var ErrAlwaysFail = errors.New("always fail")

// Publicly exposed errors from methods in this package.
var (
	ErrNamespaceNotFound = errors.New("namespace not found")
	ErrRelationNotFound  = errors.New("relation not found")
	ErrRequestCanceled   = errors.New("request canceled")
)

// CheckRequest contains the data for a single check request.
type CheckRequest struct {
	Start      *pb.ObjectAndRelation
	Goal       *pb.ObjectAndRelation
	AtRevision uint64
}

// CheckResult is the data that is returned by a single check or sub-check.
type CheckResult struct {
	IsMember bool
	Err      error
}

// ExpandRequest contains the data for a single expand request.
type ExpandRequest struct {
	Start      *pb.ObjectAndRelation
	AtRevision uint64
}

// ExpandResult is the data that is returned by a single expand or sub-expand.
type ExpandResult struct {
	Tree *pb.RelationTupleTreeNode
	Err  error
}

// Dispatcher interface describes a method for passing subchecks off to additional machines.
type Dispatcher interface {
	// Check submits a single check request and returns its result.
	Check(ctx context.Context, req CheckRequest) CheckResult

	// Expand submits a single expand request and returns its result.
	Expand(ctx context.Context, req ExpandRequest) ExpandResult
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
	check(req CheckRequest, relation *pb.Relation) ReduceableCheckFunc
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
	expand(req ExpandRequest, relation *pb.Relation) ReduceableExpandFunc
}

// MarshalZerologObject implements zerolog object marshalling.
func (er ExpandRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Str("expand", fmt.Sprintf("%s:%s#%s", er.Start.Namespace, er.Start.ObjectId, er.Start.Relation))
}

// MarshalZerologObject implements zerolog object marshalling.
func (er ExpandResult) MarshalZerologObject(e *zerolog.Event) {
	e.Str("fun", "times")
}
