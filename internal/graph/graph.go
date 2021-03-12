package graph

import (
	"context"
	"errors"

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

// Dispatcher interface describes a method for passing subchecks off to additional machines.
type Dispatcher interface {
	// Check submits a single check request and returns its result.
	Check(ctx context.Context, req CheckRequest) CheckResult
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
