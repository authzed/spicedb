package planhint_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query/planhint"
)

func TestRemapPreservesRecommendationAndRejectsConflicts(t *testing.T) {
	ledger := planhint.NewLedger(nil)
	first, err := ledger.Issue(arrow("planner", planhint.LeftToRight))
	require.NoError(t, err)
	other := arrow("planner", planhint.RightToLeft)
	other.Target = planhint.Target{Node: 2, Fingerprint: "other"}
	second, err := ledger.Issue(other)

	require.NoError(t, err)
	require.ErrorContains(t, ledger.Remap(first, other.Target), "conflict")
	require.Error(t, ledger.Remap(first, planhint.Target{Node: 3}))
	require.Error(t, ledger.Remap(99, other.Target))
	require.Equal(t, uint64(1), ledger.Hints()[0].Target.Node)
	moved := planhint.Target{Node: 3, Fingerprint: "equivalent"}

	require.NoError(t, ledger.Remap(first, moved))
	require.Equal(t, first, ledger.Hints()[0].ID)
	require.Equal(t, moved, ledger.Hints()[0].Target)
	require.Equal(t, second, ledger.Hints()[1].ID)
	require.NoError(t, ledger.Respond(planhint.Disposition{
		HintID: first, Consumer: "rewrite", Outcome: planhint.Invalidated, Reason: planhint.StaleTarget,
	}))
	require.ErrorContains(t, ledger.Remap(first, moved), "inactive")
	require.Len(t, ledger.Hints(), 1)
}

func TestAcceptedBranchOrderOwnsItsPayloadAndBecomesInactive(t *testing.T) {
	ledger := planhint.NewLedger(nil)
	order := []int{1, 0}
	id, err := ledger.Issue(planhint.Hint{
		Producer: "planner", Target: planhint.Target{Node: 1}, Payload: planhint.BranchOrder{Order: order},
	})

	require.NoError(t, err)
	order[0] = 0
	require.Equal(t, []int{1, 0}, ledger.Hints()[0].Payload.(planhint.BranchOrder).Order)
	returned := ledger.Hints()[0].Payload.(planhint.BranchOrder).Order
	returned[0] = 0

	require.Equal(t, []int{1, 0}, ledger.Hints()[0].Payload.(planhint.BranchOrder).Order)
	require.NoError(t, ledger.Respond(planhint.Disposition{HintID: id, Consumer: "reorder", Outcome: planhint.Accepted}))
	require.Empty(t, ledger.Hints())
	require.True(t, ledger.HasResponse(id, "reorder"))
	require.False(t, ledger.HasResponse(id, "compiler"))
}

func TestDispositionDetailsAreBoundedAndOwned(t *testing.T) {
	ledger := planhint.NewLedger(nil)
	id, err := ledger.Issue(arrow("planner", planhint.LeftToRight))

	require.NoError(t, err)
	details := map[string]string{"why": "budget"}
	response := planhint.Disposition{
		HintID: id, Consumer: "compiler", Outcome: planhint.Declined, Reason: planhint.ExecutionLimit, Details: details,
	}
	require.NoError(t, ledger.Respond(response))
	details["why"] = "mutated input"
	decisions := ledger.Decisions()

	require.Equal(t, "budget", decisions[0].Details["why"])
	decisions[0].Details["why"] = "mutated output"

	require.Equal(t, "budget", ledger.Decisions()[0].Details["why"])
	require.ErrorContains(t, ledger.Respond(response), "duplicate")

	for _, invalid := range []planhint.Disposition{
		{HintID: 99, Consumer: "unknown", Outcome: planhint.Accepted},
		{HintID: id, Consumer: "bad-outcome", Outcome: planhint.Outcome(99)},
		{HintID: id, Consumer: "supersession", Outcome: planhint.Superseded, Reason: planhint.ConflictingHint},
		{HintID: id, Consumer: "stronger", Outcome: planhint.Declined, Reason: planhint.StrongerEvidence},
		{HintID: id, Consumer: "long-key", Details: map[string]string{strings.Repeat("k", 129): "v"}},
		{HintID: id, Consumer: "long-value", Details: map[string]string{"k": strings.Repeat("v", 513)}},
	} {
		require.Error(t, ledger.Respond(invalid))
	}

	tooMany := map[string]string{}

	for i := range 17 {
		tooMany[fmt.Sprint(i)] = "value"
	}

	require.ErrorContains(t, ledger.Respond(planhint.Disposition{HintID: id, Consumer: "many", Details: tooMany}), "too many")
	require.Len(t, ledger.Decisions(), 1)
}

type recordingSink struct {
	decisions []planhint.Disposition
}

// Record retains the trace dispositions emitted by the ledger.
func (s *recordingSink) Record(d planhint.Disposition) {
	s.decisions = append(s.decisions, d)
}

func TestTraceRecordsSupersessionAndFinalization(t *testing.T) {
	sink := &recordingSink{}
	ledger := planhint.NewLedger(sink)
	ledger.SetPrecedence(planhint.ArrowDirectionKind, []string{"strong", "weak"})
	strong, err := ledger.Issue(arrow("strong", planhint.LeftToRight))

	require.NoError(t, err)
	weak, err := ledger.Issue(arrow("weak", planhint.RightToLeft))

	require.NoError(t, err)
	require.Len(t, sink.decisions, 1)
	require.Equal(t, weak, sink.decisions[0].HintID)
	require.Equal(t, strong, sink.decisions[0].RelatedHint)
	require.Equal(t, planhint.Superseded, sink.decisions[0].Outcome)
	require.Equal(t, strong, ledger.Hints()[0].ID)
	require.NoError(t, ledger.Finalize())
	require.Len(t, sink.decisions, 2)
	require.Equal(t, strong, sink.decisions[1].HintID)
	require.Equal(t, planhint.Unconsumed, sink.decisions[1].Outcome)
	require.NoError(t, ledger.Finalize())
	require.Len(t, sink.decisions, 2)
}

type unsupportedPayload struct{}

// Kind deliberately impersonates a supported kind to test concrete-type validation.
func (unsupportedPayload) Kind() planhint.Kind { return planhint.ArrowDirectionKind }

// Validate permits the payload so the ledger must reject its unsupported type.
func (unsupportedPayload) Validate() error { return nil }

func TestIssueRejectsMissingFieldsAndUnknownPayloads(t *testing.T) {
	for name, hint := range map[string]planhint.Hint{
		"missing payload":  {Producer: "planner", Target: planhint.Target{Node: 1}},
		"missing producer": {Target: planhint.Target{Node: 1}, Payload: planhint.ArrowDirection{}},
		"missing target":   {Producer: "planner", Payload: planhint.ArrowDirection{}},
		"unknown payload":  {Producer: "planner", Target: planhint.Target{Node: 1}, Payload: unsupportedPayload{}},
		"empty order":      {Producer: "planner", Target: planhint.Target{Node: 1}, Payload: planhint.BranchOrder{}},
	} {
		t.Run(name, func(t *testing.T) {
			ledger := planhint.NewLedger(nil)
			_, err := ledger.Issue(hint)

			require.Error(t, err)
			require.Empty(t, ledger.Hints())
			require.Empty(t, ledger.Decisions())
		})
	}
}
