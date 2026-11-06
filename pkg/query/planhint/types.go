// Package planhint defines typed, inspectable planning recommendations.
package planhint

import (
	"errors"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

type (

	// ID identifies a recommendation within a ledger.
	ID uint64

	// Kind names a typed recommendation category.
	Kind string
)

const (

	// ArrowDirectionKind identifies an arrow evaluation direction recommendation.
	ArrowDirectionKind Kind = "arrow-direction"

	// BranchOrderKind identifies a branch permutation recommendation.
	BranchOrderKind Kind = "branch-order"
)

// Direction selects an arrow's evaluation direction.
type Direction uint8

const (

	// LeftToRight recommends evaluating the arrow from its left side.
	LeftToRight Direction = iota

	// RightToLeft recommends evaluating the arrow from its right side.
	RightToLeft

	// InvalidDirection marks an unsupported direction and fails validation.
	InvalidDirection Direction = 255
)

// Payload is a typed recommendation with validation and kind inspection.
type Payload interface {
	// Kind returns the recommendation kind of the payload.
	Kind() Kind

	// Validate checks that the payload represents a valid recommendation.
	Validate() error
}

// ArrowDirection recommends an arrow evaluation direction.
type ArrowDirection struct {
	// Direction selects an arrow's evaluation direction.
	Direction Direction
}

// Kind returns the recommendation kind of the payload.
func (ArrowDirection) Kind() Kind { return ArrowDirectionKind }

// Validate checks that the payload represents a valid recommendation.
func (a ArrowDirection) Validate() error {
	if a.Direction > RightToLeft {
		return errors.New("invalid arrow direction")
	}

	return nil
}

// BranchOrder recommends a permutation of a node's immediate branches.
type BranchOrder struct {
	// Order lists child indices in recommended evaluation order.
	Order []int
}

// Kind returns the recommendation kind of the payload.
func (BranchOrder) Kind() Kind { return BranchOrderKind }

// Validate checks that the payload represents a valid recommendation.
func (b BranchOrder) Validate() error {
	if len(b.Order) == 0 {
		return errors.New("empty branch order")
	}

	seen := make([]bool, len(b.Order))

	for _, n := range b.Order {
		if n < 0 || n >= len(seen) || seen[n] {
			return errors.New("invalid branch permutation")
		}

		seen[n] = true
	}

	return nil
}

// Target identifies a planning occurrence and its structural fingerprint.
type Target struct {
	// Node identifies a unique occurrence in the planning outline.
	Node uint64

	// Fingerprint is the full structural identity used to reject stale targets.
	Fingerprint string
}

// EvidenceReference identifies the evidence supporting a recommendation.
type EvidenceReference struct {
	// Signal names the provider supporting the recommendation.
	Signal signal.Name

	// ID identifies the supporting signal snapshot.
	ID signal.EvidenceID

	// Historical reports whether evidence was derived across revisions.
	Historical bool
}

// Hint is a typed, inspectable planning recommendation.
type Hint struct {
	// ID identifies this recommendation within its ledger.
	ID ID

	// Target identifies a planning occurrence and its structural fingerprint.
	Target Target

	// Payload is a typed recommendation with validation and kind inspection.
	Payload Payload

	// Producer names the optimization that first issued the recommendation.
	Producer string

	// Producers lists the optimizations whose identical recommendations were coalesced.
	Producers []string

	// Evidence lists signal snapshots supporting the recommendation.
	Evidence []EvidenceReference
}

// Outcome describes how a recommendation was handled.
type Outcome uint8

const (

	// Accepted indicates that a consumer applied the recommendation.
	Accepted Outcome = iota

	// Declined indicates that a consumer rejected the recommendation with a reason.
	Declined

	// Superseded indicates that another recommendation took precedence.
	Superseded

	// Invalidated indicates that a rewrite made the recommendation target stale.
	Invalidated

	// Unconsumed indicates that no consumer accounted for the recommendation.
	Unconsumed
)

// ReasonCode is a structured explanation for declining or replacing a recommendation.
type ReasonCode string

const (

	// UnsupportedOperation explains that the consumer cannot apply hints to this operation.
	UnsupportedOperation ReasonCode = "unsupported-operation"

	// UnsupportedTarget explains that the consumer cannot apply hints to this node type.
	UnsupportedTarget ReasonCode = "unsupported-target"

	// StaleTarget explains that the target's structural fingerprint no longer matches.
	StaleTarget ReasonCode = "stale-target"

	// CorrectnessConstraint explains that applying the hint would violate semantics.
	CorrectnessConstraint ReasonCode = "correctness-constraint"

	// ExecutionLimit explains that an execution constraint prevents applying the hint.
	ExecutionLimit ReasonCode = "execution-limit"

	// ConflictingHint explains that a competing recommendation took precedence.
	ConflictingHint ReasonCode = "conflicting-hint"

	// StrongerEvidence explains that another evidence-backed recommendation took precedence.
	StrongerEvidence ReasonCode = "stronger-evidence"

	// NoConsumer explains that no pass or compiler handled the recommendation.
	NoConsumer ReasonCode = "no-consumer"
)

// Disposition records a consumer's response to a recommendation.
type Disposition struct {
	// HintID identifies the recommendation being accounted for.
	HintID ID

	// Consumer names the optimization or compiler handling the recommendation.
	Consumer string

	// Outcome describes how a recommendation was handled.
	Outcome Outcome

	// Reason explains a non-accepted disposition in a structured form.
	Reason ReasonCode

	// Details contains bounded diagnostic context for the response.
	Details map[string]string

	// RelatedHint identifies the recommendation that superseded this one.
	RelatedHint ID
}

// TraceSink receives hint dispositions for optional debug tracing.
type TraceSink interface {
	// Record receives a hint disposition for debug tracing.
	Record(Disposition)
}
