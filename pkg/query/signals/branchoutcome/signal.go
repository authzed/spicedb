// Package branchoutcome estimates completed Check outcomes among attempted subtrees.
package branchoutcome

import (
	"errors"
	"time"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// Name identifies the provider for completed Check outcome frequencies.
const Name signal.Name = "branch-outcome-at-revision"

var (

	// Ref provides typed access to completed Check outcome frequencies.
	Ref = signal.Reference[signal.ExecutionKey, Estimate]{Name: Name}

	// Input provides typed publication of observations for completed Check outcome frequencies.
	Input = signal.Input[signal.ExecutionKey, Observation]{Name: Name}
)

// Outcome classifies the result of a completed Check invocation.
type Outcome uint8

const (

	// Deny indicates a completed check with no matching path.
	Deny Outcome = iota

	// Grant indicates a completed check with an unconditional matching path.
	Grant

	// Conditional indicates a completed check with a caveated matching path.
	Conditional
)

// Observation records a Check outcome or an incomplete attempt.
type Observation struct {
	// UnresolvedAttemptUpperBound marks a failed batch whose per-element attempts are unknown.
	UnresolvedAttemptUpperBound uint64

	// Status describes how the observed invocation ended.
	Status signal.Terminal

	// Outcome classifies the result of a completed Check invocation.
	Outcome Outcome

	// Started is the invocation's original start time.
	Started time.Time
}

// Estimate summarizes attempted and completed Check outcomes.
type Estimate struct {
	// Attempted counts known per-element attempts, including incomplete invocations.
	Attempted uint64

	// Completed counts checks that returned a known outcome.
	Completed uint64

	// Denied counts completed denials.
	Denied uint64

	// Granted counts completed unconditional grants.
	Granted uint64

	// Conditional counts completed caveated results.
	Conditional uint64

	// UnresolvedBatches counts failed batches with unknown per-element attempts.
	UnresolvedBatches uint64

	// UnresolvedAttemptUpperBound bounds attempted elements in failed unresolved batches.
	UnresolvedAttemptUpperBound uint64

	// GrantFraction is the unconditional-grant fraction among completed checks.
	GrantFraction float64

	// DenyFraction is the denial fraction among completed checks.
	DenyFraction float64

	// ConditionalFraction is the caveated-result fraction among completed checks.
	ConditionalFraction float64
}

type accumulator struct{ e Estimate }

// New returns the revision-bound provider descriptor for completed Check outcome frequencies.
// Its RevisionBound policy isolates observations to the request-selected datastore revision.
func New() signal.Descriptor {
	return signal.NewSource(Name, signal.SourceConfig[signal.ExecutionKey, Observation, Estimate]{
		Policy:      signal.RevisionBound,
		Description: "Completed Check outcome frequencies among attempted subtrees",
		New: func() signal.Accumulator[Observation, Estimate] {
			return &accumulator{}
		},
		Accept: func(k signal.ExecutionKey, _ Observation) bool {
			return k.Operation == signal.Check
		},
		ObservationTime: func(o Observation) time.Time {
			return o.Started
		},
		Footprint: func(k signal.ExecutionKey) signal.Footprint {
			return k.Footprint()
		},
	})
}

// Add incorporates an observation into the accumulator.
func (a *accumulator) Add(o Observation) error {
	if o.Status == signal.Complete && o.Outcome > Conditional {
		return errors.New("invalid Check outcome")
	}

	if o.UnresolvedAttemptUpperBound > 0 {
		a.e.UnresolvedBatches++
		a.e.UnresolvedAttemptUpperBound += o.UnresolvedAttemptUpperBound
		return nil
	}

	a.e.Attempted++

	if o.Status != signal.Complete {
		return nil
	}

	a.e.Completed++

	switch o.Outcome {
	case Deny:
		a.e.Denied++
	case Grant:
		a.e.Granted++
	case Conditional:
		a.e.Conditional++
	}

	return nil
}

// Value returns an independent estimate snapshot at the supplied time.
func (a *accumulator) Value(time.Time) (Estimate, signal.Availability) {
	e := a.e
	if e.Completed > 0 {
		e.GrantFraction = float64(e.Granted) / float64(e.Completed)
		e.DenyFraction = float64(e.Denied) / float64(e.Completed)
		e.ConditionalFraction = float64(e.Conditional) / float64(e.Completed)
	}

	return e, signal.Available
}

// Size returns the estimated resident size of the accumulator in bytes.
func (a *accumulator) Size() int64 { return 96 }

// Clone returns an independent copy of the accumulator state.
func (a *accumulator) Clone() signal.Accumulator[Observation, Estimate] { b := *a; return &b }
