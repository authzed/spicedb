package remote

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"

	log "github.com/authzed/spicedb/internal/logging"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// compareStrategy sends a request to the primary and to every selected secondary at once, waits
// for all of them, and compares each secondary's result with the primary's. The caller always
// gets the primary's result, or its error: the secondaries never win, so answers are unchanged,
// but every request takes as long as its slowest participant. It is meant for validating
// secondaries against the primary, such as a new cache implementation, not for production traffic.
//
// Each comparison is counted in remote_dispatch_compare_total, and each mismatch is logged with
// the differing resources. A secondary that answers Unavailable, such as a cache that has not
// reached the request's revision yet, is counted as unavailable, not as a mismatch.
//
// Only Check is supported: the expression parser rejects compare(...) for other methods.
//
// In CEL: compare(['secondary-a', 'secondary-b']).
type compareStrategy struct{}

func (compareStrategy) name() string { return compareFunction }

func (compareStrategy) newPolicy(r raceRequest) racePolicy {
	policy := &comparePolicy{
		request:         r,
		secondariesDone: make(chan struct{}),
		results:         make(map[string]*v1.DispatchCheckResponse, len(r.participants)),
	}
	for _, p := range r.participants {
		if !p.primary {
			policy.secondaries++
		}
	}
	if policy.secondaries == 0 {
		// Not expected (races have at least one secondary), but never hold the primary for none.
		close(policy.secondariesDone)
	}
	return policy
}

// Outcomes of comparing a secondary with the primary, the outcome label of compareCounter.
const (
	compareMatch          = "match"
	compareMismatch       = "mismatch"
	compareUnavailable    = "unavailable"
	compareSecondaryError = "secondary_error"
	comparePrimaryError   = "primary_error"
)

// maxLoggedMismatches caps the differing resources listed in a mismatch log line.
const maxLoggedMismatches = 20

// errNoResult stands for a secondary that was still running when the primary's context ended.
var errNoResult = errors.New("secondary had no result when the primary's context ended")

type comparePolicy struct {
	basePolicy
	request raceRequest

	secondaries     int32
	exited          atomic.Int32
	secondariesDone chan struct{} // closed once every secondary has exited

	lock    sync.Mutex
	results map[string]*v1.DispatchCheckResponse // GUARDED_BY(lock)
	errs    map[string]error                     // GUARDED_BY(lock)
}

func (c *comparePolicy) observe(p participant, result any) {
	resp, ok := result.(*v1.DispatchCheckResponse)
	if !ok {
		return // unreachable: compare is only accepted in Check expressions
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.results[p.name] = resp
}

// readyToClaim never lets a secondary claim the race, so the secondaries' results are recorded
// and dropped. The primary claims once its result is complete and every secondary has exited, or
// its context has ended, after comparing their results with its own.
func (c *comparePolicy) readyToClaim(ctx context.Context, p participant, _ int, complete bool) bool {
	if !p.primary || !complete {
		return false
	}

	waitStart := time.Now()
	select {
	case <-c.secondariesDone:
	case <-ctx.Done():
	}
	compareWaitHistogram.WithLabelValues(c.request.reqKey).Observe(time.Since(waitStart).Seconds())

	c.compareResults(ctx)
	return true
}

func (c *comparePolicy) done(p participant, err error) {
	if err != nil {
		c.lock.Lock()
		if c.errs == nil {
			c.errs = make(map[string]error, len(c.request.participants))
		}
		c.errs[p.name] = err
		c.lock.Unlock()

		if p.primary {
			// Nothing to compare against: the caller gets this error (noWinnerError).
			c.countForEverySecondary(comparePrimaryError, errorCode(err).String())
		}
	}

	if !p.primary && c.exited.Add(1) == c.secondaries {
		close(c.secondariesDone)
	}
}

// compareResults compares every secondary's result with the primary's, counting each outcome and
// logging each mismatch.
func (c *comparePolicy) compareResults(ctx context.Context) {
	req, _ := c.request.request.(*v1.DispatchCheckRequest)

	c.lock.Lock()
	defer c.lock.Unlock()

	primary := c.results[primaryDispatcher]
	for _, p := range c.request.participants {
		if p.primary {
			continue
		}

		secondary, hasResult := c.results[p.name]
		err := c.errs[p.name]
		switch {
		case err != nil:
			c.countSecondaryError(p.name, err)

		case !hasResult:
			if ctxErr := ctx.Err(); ctxErr != nil {
				err = ctxErr
			} else {
				err = errNoResult
			}
			c.countSecondaryError(p.name, err)

		default:
			diffs := diffCheckResults(req, primary, secondary)
			if len(diffs) == 0 {
				countCompare(c.request.reqKey, p.name, compareMatch, "")
				continue
			}
			countCompare(c.request.reqKey, p.name, compareMismatch, "")
			c.logMismatch(ctx, p.name, req, diffs)
		}
	}
}

func (c *comparePolicy) countSecondaryError(secondary string, err error) {
	code := errorCode(err)
	outcome := compareSecondaryError
	if code == codes.Unavailable {
		outcome = compareUnavailable
	}
	countCompare(c.request.reqKey, secondary, outcome, code.String())
}

func (c *comparePolicy) countForEverySecondary(outcome, code string) {
	for _, p := range c.request.participants {
		if !p.primary {
			countCompare(c.request.reqKey, p.name, outcome, code)
		}
	}
}

func (c *comparePolicy) logMismatch(ctx context.Context, secondary string, req *v1.DispatchCheckRequest, diffs []membershipDiff) {
	shown := diffs[:min(len(diffs), maxLoggedMismatches)]
	mismatches := make([]string, 0, len(shown))
	for _, d := range shown {
		mismatches = append(mismatches, d.String())
	}

	log.Ctx(ctx).Warn().
		Str("secondary", secondary).
		Str("rpc", c.request.reqKey).
		Str("at-revision", req.GetMetadata().GetAtRevision()).
		Str("resource-relation", tuple.StringRR(c.request.resource)).
		Str("subject", tuple.StringCoreONR(req.GetSubject())).
		Str("results-setting", req.GetResultsSetting().String()).
		Int("resources", len(req.GetResourceIds())).
		Int("mismatched-resources", len(diffs)).
		Strs("mismatches", mismatches).
		Msg("secondary dispatcher result differs from the primary's")
}

func countCompare(reqKey, secondary, outcome, code string) {
	compareCounter.WithLabelValues(reqKey, secondary, outcome, code).Inc()
}

// errorCode returns the gRPC code of a participant's error, mapping context errors to Canceled
// and DeadlineExceeded.
func errorCode(err error) codes.Code {
	if s, ok := status.FromError(err); ok {
		return s.Code()
	}
	return status.FromContextError(err).Code()
}

// membershipDiff is a requested resource whose membership differs between the primary and a
// secondary.
type membershipDiff struct {
	resourceID string
	primary    v1.ResourceCheckResult_Membership
	secondary  v1.ResourceCheckResult_Membership
}

func (d membershipDiff) String() string {
	return fmt.Sprintf("%s: primary=%s secondary=%s", d.resourceID, d.primary, d.secondary)
}

// diffCheckResults returns the requested resources whose membership differs between the primary's
// and a secondary's Check result, in request order. A resource missing from a result is not a
// member. Only memberships are compared: caveat expressions may legitimately differ in structure.
//
// With ALLOW_SINGLE_RESULT, a dispatcher may stop at the first resource it finds to be a member, so
// when both results have a member, which ones they have is not compared. When only one has a
// member, only the resources that are members on either side are reported. When neither has one,
// both must have evaluated every resource, and the results are compared in full.
func diffCheckResults(req *v1.DispatchCheckRequest, primary, secondary *v1.DispatchCheckResponse) []membershipDiff {
	primaryResults := primary.GetResultsByResourceId()
	secondaryResults := secondary.GetResultsByResourceId()

	membersOnly := false
	if req.GetResultsSetting() == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT {
		primaryHas, secondaryHas := hasMember(primaryResults), hasMember(secondaryResults)
		if primaryHas && secondaryHas {
			return nil
		}
		membersOnly = primaryHas != secondaryHas
	}

	var diffs []membershipDiff
	seen := make(map[string]struct{}, len(req.GetResourceIds()))
	for _, id := range req.GetResourceIds() {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		p, s := membershipOf(primaryResults, id), membershipOf(secondaryResults, id)
		if p == s {
			continue
		}
		if membersOnly && p != v1.ResourceCheckResult_MEMBER && s != v1.ResourceCheckResult_MEMBER {
			continue
		}
		diffs = append(diffs, membershipDiff{resourceID: id, primary: p, secondary: s})
	}
	return diffs
}

func membershipOf(results map[string]*v1.ResourceCheckResult, resourceID string) v1.ResourceCheckResult_Membership {
	result, ok := results[resourceID]
	if !ok {
		return v1.ResourceCheckResult_NOT_MEMBER
	}
	return result.GetMembership()
}

func hasMember(results map[string]*v1.ResourceCheckResult) bool {
	for _, result := range results {
		if result.GetMembership() == v1.ResourceCheckResult_MEMBER {
			return true
		}
	}
	return false
}

const compareFunction = "compare"

var compareDescriptor = strategyDescriptor{
	function: compareFunction,
	methods:  []string{"check"},
	overloads: []cel.FunctionOpt{
		cel.Overload("compare_list_string",
			[]*cel.Type{cel.ListType(cel.StringType)}, dispatchPlanCelType,
			cel.UnaryBinding(func(list ref.Val) ref.Val {
				names, err := toStringSlice(list)
				if err != nil {
					return types.NewErr("%s: %s", compareFunction, err)
				}
				return dispatchPlanVal{dispatchPlan{secondaries: names, strategy: compareStrategy{}}}
			})),
	},
}
