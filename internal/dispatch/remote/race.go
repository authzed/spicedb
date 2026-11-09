package remote

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// maxBufferedResults bounds how many results a participant may buffer while its policy has
// not yet let it claim the win. It protects against a policy that never claims.
const maxBufferedResults = 1000

// errClaimBufferFull is returned for a participant that buffered maxBufferedResults results
// without its policy letting it claim the win.
var errClaimBufferFull = errors.New("dispatcher buffered too many results before claiming the race")

// noWinner is the value of raceRun.winner until a participant wins.
const noWinner = -1

// receiver is the receiving side of a dispatch stream.
type receiver[S any] interface {
	Recv() (S, error)
}

// raceMode selects how a race uses contexts and when it returns.
type raceMode int

const (
	// streamingRace is for streaming dispatches. The winner keeps streaming after it wins, so
	// each participant has its own context, the losers are canceled as soon as there is a
	// winner, and the race returns once every participant has exited.
	streamingRace raceMode = iota

	// unaryRace is for unary dispatches. The winner holds its whole result when it wins, so
	// the participants share the race's context, which the caller cancels once the race
	// returns, and the race returns as soon as the winner has exited. The canceled losers exit
	// in the background.
	unaryRace
)

// runRace races r's participants under a policy created by strategy, and publishes the
// winner's results to stream. A participant becomes the winner when it is the first whose
// policy lets it claim the race; the other participants are then canceled. runRace returns
// once every participant has exited, so no participant outlives the stream.
//
// If there is a winner, runRace returns the error the winner stopped with, if any (the winner
// may have published only part of its results). Otherwise it returns the policy's choice of
// error, then the context's error, then a generic error.
func runRace[R any](
	ctx context.Context,
	r raceRequest,
	strategy secondaryStrategy,
	open func(context.Context, ClusterClient) (receiver[R], error),
	stream dispatch.Stream[R],
) error {
	return race(ctx, r, strategy, open, stream, streamingRace)
}

// runUnaryRace races r's participants for a unary dispatch, under a policy created by
// strategy, and returns the winner's result. It returns as soon as the winner has its result.
// The participants run under ctx itself, so the caller must cancel ctx once runUnaryRace
// returns: the losers then stop and exit in the background, and racePolicy.done may be called
// for them after runUnaryRace returns. Errors are chosen as in runRace.
func runUnaryRace[S any](
	ctx context.Context,
	r raceRequest,
	strategy secondaryStrategy,
	call func(context.Context, ClusterClient) (S, error),
) (S, error) {
	result := &unaryResultStream[S]{ctx: ctx}
	err := race(ctx, r, strategy, func(ctx context.Context, client ClusterClient) (receiver[S], error) {
		resp, err := call(ctx, client)
		return &unaryReceiver[S]{resp: resp, err: err}, nil
	}, result, unaryRace)
	if err != nil {
		return *new(S), err
	}
	if !result.ok {
		return *new(S), spiceerrors.MustBugf("unary dispatch race finished without a result or an error")
	}
	return result.result, nil
}

func race[R any](
	ctx context.Context,
	r raceRequest,
	strategy secondaryStrategy,
	open func(context.Context, ClusterClient) (receiver[R], error),
	stream dispatch.Stream[R],
	mode raceMode,
) error {
	countStrategy(r.reqKey, strategy.name())

	n := len(r.participants)
	run := &raceRun[R]{
		request: r,
		policy:  strategy.newPolicy(r),
		open:    open,
		stream:  stream,
		mode:    mode,
		exits:   make(chan struct{}, 2),
	}
	if n <= len(run.slotsBuf) {
		run.slots = run.slotsBuf[:n]
	} else {
		run.slots = make([]raceSlot, n)
	}
	run.winner.Store(noWinner)
	run.running.Store(int32(n)) //nolint:gosec // a handful of participants

	if mode == unaryRace {
		for i := range run.slots {
			run.slots[i].ctx = ctx
		}
	} else {
		for i := range run.slots {
			run.slots[i].ctx, run.slots[i].cancel = context.WithCancel(ctx)
		}
		defer run.cancelAllExcept(noWinner)
	}

	for i, p := range r.participants {
		go run.runAndReport(i, p)
	}

	// Wait for the first exit signal: the winner's exit (unary races only), or the last
	// participant's exit. If there is a winner, it has exited, so its slot is final. Without
	// a winner, the signal came from the last participant, so every slot is final.
	<-run.exits

	if winner := run.winner.Load(); winner != noWinner {
		if err := run.slots[winner].err; err != nil {
			log.Ctx(ctx).Warn().Err(err).Str("dispatcher", r.participants[winner].name).Msg("dispatcher that returned results encountered an error during streaming")
			return err
		}
		return nil
	}

	errs := make(map[string]error, n)
	for i, slot := range run.slots {
		if slot.err != nil {
			errs[r.participants[i].name] = slot.err
		}
	}

	if err := run.policy.noWinnerError(errs); err != nil {
		log.Ctx(ctx).Warn().Err(err).Errs("all-errors", slices.Collect(maps.Values(errs))).Msg("returning error as no dispatcher returned results")
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return errors.New("no dispatcher returned results; please check the logs for more information")
}

// raceRun is the state of a single race, shared by its participants' goroutines. Slices are
// indexed like raceRequest.participants.
type raceRun[R any] struct {
	request raceRequest
	policy  racePolicy
	open    func(context.Context, ClusterClient) (receiver[R], error)
	stream  dispatch.Stream[R]
	mode    raceMode
	slots   []raceSlot   // one per participant
	winner  atomic.Int32 // index of the winning participant, or noWinner
	running atomic.Int32 // participants that have not exited yet

	// exits receives a signal when the last participant exits and, in a unary race, when the
	// winner exits (once if the winner is also the last). At most two signals are sent, so the
	// buffer never fills.
	exits chan struct{}

	// slotsBuf backs slots for the common case of a primary and one secondary.
	slotsBuf [2]raceSlot
}

// raceSlot is the per-participant state of a race.
type raceSlot struct {
	ctx    context.Context
	cancel context.CancelFunc // nil in a unary race, whose participants share the race's context

	// err is the error the participant stopped with. It is written only by the participant's
	// goroutine, before it signals its exit.
	err error
}

func (rr *raceRun[R]) cancelAllExcept(keep int) {
	for i := range rr.slots {
		if i != keep {
			rr.slots[i].cancel()
		}
	}
}

// runAndReport runs participant i, records how it stopped, and signals its exit.
//
// Calls that reach deep into other packages (the policy's start, metrics) are made here,
// rather than from runParticipant, to keep the participant goroutine's stack shallow: a stack
// that outgrows its initial size is copied, on every dispatch.
func (rr *raceRun[R]) runAndReport(i int, p participant) {
	ctx := rr.slots[i].ctx
	reqKey := rr.request.reqKey

	var err error
	switch {
	case !rr.policy.start(ctx, p):
		traceDispatcher(ctx, p.name, "race policy did not start dispatcher")
		err = errNotStarted

	case ctx.Err() != nil:
		traceDispatcher(ctx, p.name, "dispatcher context canceled before it started")
		if p.primary {
			countPrimaryDispatch(reqKey, true)
		}
		err = ctx.Err()

	default:
		if p.primary {
			countPrimaryDispatch(reqKey, false)
		}
		err = rr.runParticipant(i, p)
	}

	if !errors.Is(err, errNotStarted) {
		rr.slots[i].err = err
	}
	won := rr.winner.Load() == int32(i) //nolint:gosec // i < number of participants
	if won {
		countWinner(reqKey, p.name)
	}

	// The only call site of done, so it runs exactly once per participant.
	rr.policy.done(p, err)

	// Signal last. The woken caller is queued to run next on this thread, so it runs as soon as
	// this goroutine exits; anything done after the signal would make the scheduler wake
	// another thread to run the caller instead.
	last := rr.running.Add(-1) == 0
	if last || (won && rr.mode == unaryRace) {
		rr.exits <- struct{}{}
	}
}

// runParticipant runs participant i, once its policy has let it start, until its stream
// ends, it fails, it loses the race, or it is canceled. It returns the error the participant
// stopped with (nil when it finished or lost).
func (rr *raceRun[R]) runParticipant(i int, p participant) error {
	ctx := rr.slots[i].ctx
	reqKey := rr.request.reqKey

	// Measured after start returns: time spent waiting to start is not latency.
	startTime := time.Now()

	client, err := rr.open(ctx, p.client)
	if err != nil {
		if !p.primary {
			rr.request.stats.recordSecondaryError(err)
		}
		warnDispatcher(ctx, p.name, err, "error when trying to run dispatcher")
		return err
	}

	var buffer []R
	received := 0
	won := false
	for {
		if err := ctx.Err(); err != nil {
			// A winner may have published only a prefix of its results. Returning the
			// cancellation keeps that prefix from being treated as a complete stream.
			traceDispatcher(ctx, p.name, "dispatcher context canceled, in results loop")
			return err
		}

		result, err := client.Recv()
		complete := errors.Is(err, io.EOF)
		if err != nil && !complete {
			traceDispatcherError(ctx, p.name, err, "dispatcher recv error")
			if !p.primary {
				rr.request.stats.recordSecondaryError(err)
			}
			return err
		}

		if !complete {
			if !p.primary {
				if err := tagCursorWithSecondary(result, p.name); err != nil {
					return err
				}
			}
			received++
		}

		if (!complete && received == 1) || (complete && received == 0) {
			latency := time.Since(startTime)
			rr.policy.firstResult(p, latency)
			if !p.primary {
				rr.request.stats.recordSecondaryFirstResult(ctx, reqKey, rr.request.resource, rr.request.subject, latency)
			}
		}

		if won {
			if complete {
				return nil
			}
			if err := rr.stream.Publish(result); err != nil {
				return err
			}
			continue
		}

		if !rr.policy.readyToClaim(p, received, complete) {
			if complete {
				// The stream ended, but the policy never let this participant claim the race.
				return nil
			}
			if len(buffer) >= maxBufferedResults {
				return errClaimBufferFull
			}
			buffer = append(buffer, result)
			continue
		}

		if !rr.winner.CompareAndSwap(noWinner, int32(i)) { //nolint:gosec // i < number of participants
			traceDispatcher(ctx, p.name, "another dispatcher has already returned results")
			return nil
		}

		won = true
		traceDispatcher(ctx, p.name, "this dispatcher won the race; canceling the others")
		if rr.mode == streamingRace {
			// In a unary race, the caller cancels the shared context once the race returns.
			rr.cancelAllExcept(i)
		}

		for _, buffered := range buffer {
			if err := rr.stream.Publish(buffered); err != nil {
				return err
			}
		}
		buffer = nil

		if complete {
			return nil
		}
		if err := rr.stream.Publish(result); err != nil {
			return err
		}
	}
}

// The helpers below keep logging and metrics out of line. Inlined, each zerolog chain or
// variadic WithLabelValues call reserves its own temporaries in the caller's frame; a
// participant's goroutine starts with a small stack and would have to grow (and copy) it on
// every dispatch.

//go:noinline
func traceDispatcher(ctx context.Context, dispatcher string, msg string) {
	log.Ctx(ctx).Trace().Str("dispatcher", dispatcher).Msg(msg)
}

//go:noinline
func traceDispatcherError(ctx context.Context, dispatcher string, err error, msg string) {
	log.Ctx(ctx).Trace().Str("dispatcher", dispatcher).Err(err).Msg(msg)
}

//go:noinline
func warnDispatcher(ctx context.Context, dispatcher string, err error, msg string) {
	log.Ctx(ctx).Warn().Err(err).Str("dispatcher", dispatcher).Msg(msg)
}

//go:noinline
func countPrimaryDispatch(reqKey string, canceled bool) {
	counters, ok := primaryDispatchByKey[reqKey]
	if !ok {
		primaryDispatch.WithLabelValues(strconv.FormatBool(canceled), reqKey).Inc()
		return
	}
	if canceled {
		counters[1].Inc()
	} else {
		counters[0].Inc()
	}
}

// strategyCounters caches secondaryStrategyCounter's children by request key and strategy
// name, both of which come from small fixed sets.
var strategyCounters sync.Map // [2]string{reqKey, strategy} -> prometheus.Counter

func countStrategy(reqKey string, strategy string) {
	key := [2]string{reqKey, strategy}
	counter, ok := strategyCounters.Load(key)
	if !ok {
		counter, _ = strategyCounters.LoadOrStore(key, secondaryStrategyCounter.WithLabelValues(reqKey, strategy))
	}
	counter.(prometheus.Counter).Inc()
}

//go:noinline
func countWinner(reqKey string, dispatcher string) {
	dispatchCounter.WithLabelValues(reqKey, dispatcher).Add(1)
}

// unaryReceiver presents the result of a unary call as a stream of one message, so unary
// dispatches can run on the race engine.
type unaryReceiver[S any] struct {
	resp S
	err  error
	sent bool
}

func (u *unaryReceiver[S]) Recv() (S, error) {
	if u.err != nil {
		return *new(S), u.err
	}
	if u.sent {
		return *new(S), io.EOF
	}
	u.sent = true
	return u.resp, nil
}

// unaryResultStream collects the single result of a unary race. Only the winner publishes,
// from its own goroutine, and the result is read after the race has finished.
type unaryResultStream[S any] struct {
	ctx    context.Context
	result S
	ok     bool
}

func (s *unaryResultStream[S]) Context() context.Context {
	return s.ctx
}

func (s *unaryResultStream[S]) Publish(result S) error {
	if s.ok {
		return spiceerrors.MustBugf("unary dispatch race published more than one result")
	}
	s.result = result
	s.ok = true
	return nil
}
