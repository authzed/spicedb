package remote

import (
	"context"
	"sync"
	"time"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"

	log "github.com/authzed/spicedb/internal/logging"
)

// hedgedStrategy starts the secondaries immediately and delays the primary by an adaptive
// amount: the configured quantile of the secondaries' first-result latency, capped at a
// maximum delay. Any secondary error releases the primary early. This is the default strategy,
// used when a dispatch expression returns a plain list.
//
// In CEL: hedged(['secondary']) or hedged(['secondary'], duration('5ms')).
type hedgedStrategy struct {
	// maxDelay caps the primary's delay for this query. Zero means it was not set: the cap is
	// then the largest MaximumPrimaryHedgingDelay of the participating secondaries.
	maxDelay time.Duration
}

func (hedgedStrategy) name() string { return hedgedFunction }

func (s hedgedStrategy) newPolicy(r raceRequest) racePolicy {
	maxDelay := s.maxDelay
	if maxDelay == 0 {
		for _, p := range r.participants {
			maxDelay = max(maxDelay, p.maxPrimaryDelay)
		}
	}

	waitTime := r.stats.hedgingDelay(r.reqKey, r.resource, r.subject, maxDelay)
	return &hedgedPolicy{sleeper: primarySleeper{
		reqKey:     r.reqKey,
		waitTime:   waitTime,
		cancelFunc: func() {}, // No-op until sleep is called.
	}}
}

type hedgedPolicy struct {
	basePolicy
	sleeper primarySleeper
}

func (h *hedgedPolicy) start(ctx context.Context, p participant) bool {
	if p.primary {
		h.sleeper.sleep(ctx)
	}
	return true
}

func (h *hedgedPolicy) done(p participant, err error) {
	if !p.primary && err != nil {
		// Let the primary start immediately if it is still waiting.
		h.sleeper.cancelSleep()
	}
}

const (
	hedgedFunction         = "hedged"
	hedgedDurationArgument = "maximum delay"
)

var hedgedDescriptor = strategyDescriptor{
	function:         hedgedFunction,
	durationArgument: hedgedDurationArgument,
	overloads: []cel.FunctionOpt{
		cel.Overload("hedged_list_string",
			[]*cel.Type{cel.ListType(cel.StringType)}, dispatchPlanCelType,
			cel.UnaryBinding(func(list ref.Val) ref.Val {
				names, err := toStringSlice(list)
				if err != nil {
					return types.NewErr("%s: %s", hedgedFunction, err)
				}
				return dispatchPlanVal{dispatchPlan{secondaries: names, strategy: hedgedStrategy{}}}
			})),
		cel.Overload("hedged_list_string_duration",
			[]*cel.Type{cel.ListType(cel.StringType), cel.DurationType}, dispatchPlanCelType,
			cel.BinaryBinding(func(list, maxDelay ref.Val) ref.Val {
				names, err := toStringSlice(list)
				if err != nil {
					return types.NewErr("%s: %s", hedgedFunction, err)
				}
				d, ok := maxDelay.(types.Duration)
				if !ok {
					return types.MaybeNoSuchOverloadErr(maxDelay)
				}
				if err := checkStrategyDuration(hedgedFunction, hedgedDurationArgument, d.Duration); err != nil {
					return types.NewErr("%s", err)
				}
				return dispatchPlanVal{dispatchPlan{secondaries: names, strategy: hedgedStrategy{maxDelay: d.Duration}}}
			})),
	},
}

// primarySleeper delays a hedged primary: it waits for waitTime, unless cancelSleep is called
// first or meanwhile.
type primarySleeper struct {
	reqKey     string
	waitTime   time.Duration
	cancelFunc context.CancelFunc // GUARDED_BY(lock)
	canceled   bool               // GUARDED_BY(lock)
	lock       sync.Mutex
}

// sleep sets the value of cancelFunc, and sleeps for the configured wait time or exits early if the context is cancelled.
func (s *primarySleeper) sleep(parentCtx context.Context) {
	if s.waitTime <= 0 {
		return
	}

	sleepCtx, cf := context.WithCancel(context.Background())

	s.lock.Lock()
	s.cancelFunc = cf
	if s.canceled {
		// cancelSleep ran before sleep did, so don't wait at all.
		cf()
	}
	s.lock.Unlock()

	s.traceWaiting(parentCtx)

	startTime := time.Now()
	timer := time.NewTimer(s.waitTime)
	defer timer.Stop()

	select {
	case <-parentCtx.Done():
		s.trace(parentCtx, "primary dispatch canceled before waiting completed")
		return

	case <-sleepCtx.Done():
		s.trace(parentCtx, "primary dispatch early terminated waiting")
		s.observeActualWait(time.Since(startTime))
		return

	case <-timer.C:
		s.trace(parentCtx, "primary dispatch finished waiting")
		s.observeActualWait(s.waitTime)
		return
	}
}

// These helpers keep logging and metrics out of sleep's frame, per the stack rule above
// traceDispatcher in race.go.

//go:noinline
func (s *primarySleeper) traceWaiting(ctx context.Context) {
	metricsFor(s.reqKey).hedgeWait.Observe(s.waitTime.Seconds())
	log.Ctx(ctx).Trace().Str("request-key", s.reqKey).Stringer("wait-time", s.waitTime).Msg("primary dispatch waiting before running")
}

//go:noinline
func (s *primarySleeper) trace(ctx context.Context, msg string) {
	log.Ctx(ctx).Trace().Str("request-key", s.reqKey).Msg(msg)
}

//go:noinline
func (s *primarySleeper) observeActualWait(wait time.Duration) {
	metricsFor(s.reqKey).hedgeActualWait.Observe(wait.Seconds())
}

func (s *primarySleeper) cancelSleep() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.canceled = true
	s.cancelFunc()
}
