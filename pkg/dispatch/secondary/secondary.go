// Package secondary lets programs that embed SpiceDB add their own secondary dispatch strategies.
//
// A secondary dispatch expression (--experimental-dispatch-secondary-upstream-exprs) chooses, per
// request, which secondary dispatchers race the primary and the strategy that runs the race, such
// as hedged([...]) or prefer_secondary([...], duration('50ms')). A Function registered through
// server.Config.DispatchSecondaryStrategies adds a strategy function to those expressions: the
// race engine runs its participants, and the strategy's Policy takes the decisions, exactly as for
// the built-in strategies.
package secondary

import (
	"context"
	"errors"
	"time"

	"github.com/authzed/spicedb/pkg/tuple"
)

// PrimaryName is the name of the primary dispatcher, in Participant.Name and in the errors passed
// to Policy.NoWinnerError.
const PrimaryName = "$primary"

// ErrNotStarted is passed to Policy.Done for a participant whose Policy.Start returned false.
var ErrNotStarted = errors.New("participant not started by race policy")

// Function declares a strategy function for secondary dispatch expressions. Calls are checked
// when the expression is parsed, at startup, and the strategy is built each time the expression
// is evaluated.
type Function struct {
	// Name is the CEL function name, such as "compare". It must not be the name of a built-in
	// strategy function or of another registered Function.
	Name string

	// Methods lists the request kinds (the keys of the expressions flag: "check",
	// "lookupresources", "lookupsubjects") whose expressions may call the function, or is nil if
	// all may. A call from any other kind's expression is rejected when it is parsed.
	Methods []string

	// New, if set, builds the strategy for a call with a list of secondary names, such as
	// compare(['a', 'b']).
	New func() (Strategy, error)

	// NewWithDuration, if set, builds the strategy for a call with a list of secondary names and
	// a duration, such as compare(['a'], duration('5s')). The duration must be positive: literal
	// durations are checked when the expression is parsed, computed ones before this is called.
	NewWithDuration func(time.Duration) (Strategy, error)
}

// Strategy creates a Policy for each race. Values are built when an expression is evaluated, and
// must be immutable.
type Strategy interface {
	NewPolicy(race Race) Policy
}

// Race describes the race a Policy is created for.
type Race struct {
	// Method is the request kind: "check", "lookupresources" or "lookupsubjects".
	Method string

	// Resource and Subject are the request's resource and subject relations.
	Resource tuple.RelationReference
	Subject  tuple.RelationReference

	// Request is the dispatch request, such as a *dispatchv1.DispatchCheckRequest. It must not
	// be modified.
	Request any

	// Participants holds the primary, when it takes part, first, then the secondaries. There is
	// always at least one secondary.
	Participants []Participant
}

// Participant is one dispatcher taking part in a race.
type Participant struct {
	// Name is PrimaryName for the primary, or the secondary's name.
	Name    string
	Primary bool
}

// Policy takes every decision in a single race between the primary and its secondaries; the race
// engine owns the mechanics: goroutines, contexts, streams, buffering, choosing a single winner,
// publishing and cancellation. A participant becomes the winner when it is the first whose policy
// lets it claim the race; the other participants are then canceled.
//
// A new Policy is created for each race. Its methods are called concurrently from the
// participants' goroutines, so implementations must be safe for concurrent use. Embed BasePolicy
// to keep the default decisions.
type Policy interface {
	// Start blocks until p may start, and must return promptly once ctx is done. Returning false
	// means p never runs.
	Start(ctx context.Context, p Participant) bool

	// ReadyToClaim is called after every result p receives (complete=false), and once more when
	// p's stream ends successfully (complete=true). received counts the results so far. Once it
	// returns true, p tries to become the winner; results received before that are buffered by
	// the engine, up to 1000, and published, in order, if p wins. It may block, but must return
	// promptly once ctx (p's context) is done.
	ReadyToClaim(ctx context.Context, p Participant, received int, complete bool) bool

	// FirstResult reports the time from p's start to its first result, or to the end of its
	// stream if the stream was empty.
	FirstResult(p Participant, latency time.Duration)

	// Done is called exactly once for every participant, on every path. err is nil if p
	// finished, lost the race, or was the winner and finished; ErrNotStarted if Start returned
	// false; otherwise the error p stopped with.
	Done(p Participant, err error)

	// NoWinnerError picks the error to return when no participant won. errs holds the error of
	// every participant that stopped with one, by name. Returning nil lets the engine fall back
	// to the context's error, then to the error of the only participant that failed, then to a
	// generic error.
	NoWinnerError(errs map[string]error) error
}

// ResultObserver may be implemented by a Policy that needs the participants' results, for example
// to compare them. ObserveResult is called with every result p receives, from p's goroutine,
// before ReadyToClaim. The result, such as a *dispatchv1.DispatchCheckResponse, must not be
// modified, and must not be retained past the race: the winner's results are published to the
// caller.
type ResultObserver interface {
	ObserveResult(p Participant, result any)
}

// BasePolicy implements the default decisions. Policies embed it and override only the
// decisions they change.
type BasePolicy struct{}

// Start lets every participant start immediately.
func (BasePolicy) Start(context.Context, Participant) bool { return true }

// ReadyToClaim lets a participant claim the win at its first result (or at the end of an empty
// stream), so results are published as soon as they arrive.
func (BasePolicy) ReadyToClaim(_ context.Context, _ Participant, received int, complete bool) bool {
	return received >= 1 || complete
}

// FirstResult ignores the latency.
func (BasePolicy) FirstResult(Participant, time.Duration) {}

// Done ignores the participant's exit.
func (BasePolicy) Done(Participant, error) {}

// NoWinnerError returns the primary's error, if the primary stopped with one.
func (BasePolicy) NoWinnerError(errs map[string]error) error {
	return errs[PrimaryName]
}
