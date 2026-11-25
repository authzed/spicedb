package query

import (
	"strconv"

	"github.com/authzed/spicedb/pkg/genutil/execscope"
	"github.com/authzed/spicedb/pkg/query/planhint"
)

// setCheckConcurrency accounts for the recommendation at execution time because
// planning cannot know whether this invocation will collect a recursion frontier
// or use a legacy observer. A configured ceiling is authoritative in either case.
func (ctx *Context) setCheckConcurrency(requested uint16, hint planhint.ID) (uint16, error) {
	if requested == 0 {
		return 1, nil
	}

	effective := min(requested, max(ctx.checkConcurrency, 1))
	outcome, reason := planhint.Accepted, planhint.ReasonCode("")

	if effective > 1 {
		if ctx.TraceLogger != nil || len(ctx.observers) > 0 {
			effective, outcome, reason = 1, planhint.Declined, planhint.UnsupportedObserver
		} else if len(ctx.recursiveFrontierCollectors) > 0 {
			effective, outcome, reason = 1, planhint.Declined, planhint.ActiveFrontier
		}
	}

	execscope.RecordConcurrency(ctx.Context, requested, effective, outcome == planhint.Declined)

	// A reusable iterator contains the hint ID and policy, never the request ledger.
	// Repeated recursive invocations must each justify their execution decision.
	if ctx.hintLedger != nil && hint != 0 {
		disposition := planhint.Disposition{
			HintID:   hint,
			Consumer: "query-executor",
			Outcome:  outcome,
			Reason:   reason,
		}

		if ctx.hintLedger.CollectsEvidence() {
			disposition.Details = map[string]string{
				"requested": strconv.FormatUint(uint64(requested), 10),
				"effective": strconv.FormatUint(uint64(effective), 10),
				"ceiling":   strconv.FormatUint(uint64(max(ctx.checkConcurrency, 1)), 10),
			}
		}

		if err := ctx.hintLedger.RespondExecution(disposition); err != nil {
			return 1, err
		}
	}

	return effective, nil
}

// Batch reducers retain child order to reduce unresolved target vectors. The
// scalar sibling scheduler does not implement that vector reduction, so account
// for its recommendation explicitly rather than silently ignoring the hint.
func (ctx *Context) accountBatchConcurrency(requested uint16, hint planhint.ID) error {
	return ctx.accountOrderedCheckConcurrency(requested, hint, "batch", planhint.BatchChildOrder)
}

// independentCheckConcurrency distinguishes permission operands from the direct
// and indirect forms of a relation. A direct grant must avoid indirect work in
// scalar Checks just as it does in a shared batch.
func (ctx *Context) independentCheckConcurrency(requested uint16, hint planhint.ID, independent bool) (uint16, error) {
	if independent {
		return ctx.setCheckConcurrency(requested, hint)
	}

	return 1, ctx.accountOrderedCheckConcurrency(requested, hint, "scalar", planhint.RelationChildOrder)
}

func (ctx *Context) accountOrderedCheckConcurrency(requested uint16, hint planhint.ID, mode string, reason planhint.ReasonCode) error {
	if requested == 0 {
		return nil
	}

	execscope.RecordConcurrency(ctx.Context, requested, 1, requested > 1)

	if ctx.hintLedger == nil || hint == 0 {
		return nil
	}

	response := planhint.Disposition{HintID: hint, Consumer: "query-executor", Outcome: planhint.Accepted}

	if requested > 1 {
		response.Outcome = planhint.Declined
		response.Reason = reason
	}

	if ctx.hintLedger.CollectsEvidence() {
		response.Details = map[string]string{"requested": strconv.FormatUint(uint64(requested), 10), "effective": "1", "mode": mode}
	}

	return ctx.hintLedger.RespondExecution(response)
}
