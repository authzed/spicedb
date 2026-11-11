package remote

import (
	"context"
	"errors"
	"time"

	"github.com/authzed/spicedb/pkg/tuple"
)

// participant is one dispatcher taking part in a race: the primary or a secondary.
type participant struct {
	// name is primaryDispatcher for the primary, or the secondary's name.
	name    string
	primary bool
	client  ClusterClient

	// maxPrimaryDelay is SecondaryDispatch.MaximumPrimaryHedgingDelay for a secondary,
	// and zero for the primary.
	maxPrimaryDelay time.Duration
}

// raceRequest describes the race a policy is created for.
type raceRequest struct {
	reqKey   string
	resource tuple.RelationReference
	subject  tuple.RelationReference

	// participants holds the primary (when allowed) first, then the resolved secondaries.
	participants []participant
	stats        *raceStats
}

// secondaryStrategy is the value produced by a CEL strategy function such as hedged(...):
// its fixed configuration plus a factory for per-request policies. Values are built when
// the dispatch expression is evaluated and must be immutable.
type secondaryStrategy interface {
	// name is used as a metric label and in logs.
	name() string

	// newPolicy creates the policy for a single race. It is only called when at least
	// one secondary takes part.
	newPolicy(r raceRequest) racePolicy
}

// racePolicy takes every decision in a single race between the primary and its secondaries.
// The race engine (runRace) owns all the mechanics: goroutines, contexts, streams, buffering,
// choosing a single winner, publishing and cancellation. It asks the policy at each decision
// point of a participant's lifecycle:
//
//	start         may the participant start (blocking until it may)?
//	readyToClaim  after each result: may the participant try to become the winner now?
//	firstResult   feedback: the participant's time to its first result
//	done          the participant exited (exactly once per participant)
//	noWinnerError which error to return when nobody won
//
// A new policy is created for each request. Its methods are called concurrently from the
// participants' goroutines, so implementations must be safe for concurrent use.
type racePolicy interface {
	// start blocks until p may start, and must return promptly once ctx is done.
	// Returning false means p never runs. The engine checks ctx after start returns.
	start(ctx context.Context, p participant) bool

	// readyToClaim is called after every result p receives (complete=false), and once more
	// when p's stream ends successfully (complete=true). received counts the results so far.
	// Once it returns true, p tries to become the winner; results received before that are
	// buffered by the engine and published, in order, if p wins.
	readyToClaim(p participant, received int, complete bool) bool

	// firstResult reports the time from p's start to its first result, or to the end of its
	// stream if the stream was empty.
	firstResult(p participant, latency time.Duration)

	// done is called exactly once for every participant, on every path. err is nil if p
	// finished, lost the race, or was the winner and finished; errNotStarted if start
	// returned false; otherwise the error p stopped with.
	done(p participant, err error)

	// noWinnerError picks the error to return when no participant won. errs holds the error
	// of every participant that stopped with one. Returning nil lets the engine fall back to
	// the context's error, then to the error of the only participant that failed (such as the
	// secondary a cursor is locked to), then to a generic error.
	noWinnerError(errs map[string]error) error
}

// errNotStarted is reported to racePolicy.done for a participant the policy never started.
var errNotStarted = errors.New("participant not started by race policy")

// basePolicy implements the default decisions. Policies embed it and override only the
// decisions they change.
type basePolicy struct{}

// start lets every participant start immediately.
func (basePolicy) start(context.Context, participant) bool { return true }

// readyToClaim lets a participant claim the win at its first result (or at the end of an
// empty stream), so results are published as soon as they arrive.
func (basePolicy) readyToClaim(_ participant, received int, complete bool) bool {
	return received >= 1 || complete
}

func (basePolicy) firstResult(participant, time.Duration) {}

func (basePolicy) done(participant, error) {}

// noWinnerError returns the primary's error, if the primary stopped with one.
func (basePolicy) noWinnerError(errs map[string]error) error {
	return errs[primaryDispatcher]
}

// cursorLockedStrategy races a continuation pinned to the secondary that produced its cursor.
// The secondary is the only participant, so the request's strategy plays no part.
type cursorLockedStrategy struct{}

func (cursorLockedStrategy) name() string { return cursorLockedStrategyName }

func (cursorLockedStrategy) newPolicy(raceRequest) racePolicy { return basePolicy{} }
