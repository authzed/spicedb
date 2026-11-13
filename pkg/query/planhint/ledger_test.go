package planhint_test

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func arrow(producer string, d planhint.Direction) planhint.Hint {
	return planhint.Hint{
		Producer: producer,
		Target:   planhint.Target{Node: 1, Fingerprint: "arrow"},
		Payload:  planhint.ArrowDirection{Direction: d},
	}
}

func TestDeclineRequiresReasonAndConsumer(t *testing.T) {
	l := planhint.NewLedger(nil)
	id, err := l.Issue(arrow("a", planhint.LeftToRight))
	require.NoError(t, err)
	require.Error(t, l.Respond(planhint.Disposition{HintID: id, Consumer: "compiler", Outcome: planhint.Declined}))
	require.Error(t, l.Respond(planhint.Disposition{HintID: id, Outcome: planhint.Declined, Reason: planhint.UnsupportedOperation}))
	require.NoError(t, l.Respond(planhint.Disposition{
		HintID:   id,
		Consumer: "compiler",
		Outcome:  planhint.Declined,
		Reason:   planhint.UnsupportedOperation,
	}))
	require.Len(t, l.Decisions(), 1)
}

func TestUnconsumedIsRecorded(t *testing.T) {
	l := planhint.NewLedger(nil)
	_, err := l.Issue(arrow("a", planhint.LeftToRight))
	require.NoError(t, err)
	require.NoError(t, l.Finalize())
	require.Equal(t, planhint.Unconsumed, l.Decisions()[0].Outcome)
}

func TestConflictsNeedExplicitPrecedence(t *testing.T) {
	l := planhint.NewLedger(nil)
	_, err := l.Issue(arrow("a", planhint.LeftToRight))
	require.NoError(t, err)
	_, err = l.Issue(arrow("b", planhint.RightToLeft))
	require.Error(t, err)
	l = planhint.NewLedger(nil)
	l.SetPrecedence(planhint.ArrowDirectionKind, []string{"b", "a"})
	old, _ := l.Issue(arrow("a", planhint.LeftToRight))
	next, err := l.Issue(arrow("b", planhint.RightToLeft))
	require.NoError(t, err)
	require.Equal(t, planhint.Superseded, l.Decisions()[0].Outcome)
	require.Equal(t, next, l.Decisions()[0].RelatedHint)
	require.Equal(t, old, l.Decisions()[0].HintID)
}

func TestIdenticalHintsPreserveProvenance(t *testing.T) {
	l := planhint.NewLedger(nil)
	a, err := l.Issue(arrow("a", planhint.LeftToRight))
	require.NoError(t, err)
	b, err := l.Issue(arrow("b", planhint.LeftToRight))
	require.NoError(t, err)
	require.Equal(t, a, b)
	require.ElementsMatch(t, []string{"a", "b"}, l.Hints()[0].Producers)
}

func TestMalformedPayloadRejected(t *testing.T) {
	l := planhint.NewLedger(nil)
	_, err := l.Issue(arrow("a", planhint.Direction(99)))
	require.Error(t, err)
	_, err = l.Issue(planhint.Hint{Producer: "a", Target: planhint.Target{Node: 1}, Payload: planhint.BranchOrder{Order: []int{0, 0}}})
	require.Error(t, err)
}

type inspectingSink struct {
	ledger *planhint.Ledger
	called chan struct{}
}

func (s *inspectingSink) Record(planhint.Disposition) {
	_ = s.ledger.Decisions()
	s.called <- struct{}{}
}

func TestTraceSinkCanInspectLedger(t *testing.T) {
	sink := &inspectingSink{called: make(chan struct{}, 1)}
	l := planhint.NewLedger(sink)
	sink.ledger = l
	id, err := l.Issue(arrow("a", planhint.LeftToRight))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		done <- l.Respond(planhint.Disposition{HintID: id, Consumer: "compiler", Outcome: planhint.Accepted})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("trace callback blocked by ledger lock")
	}

	<-sink.called
}

func TestCoalescedPrecedenceUsesStrongestProducer(t *testing.T) {
	for _, order := range [][]string{{"weak", "strong", "middle"}, {"middle", "weak", "strong"}, {"strong", "middle", "weak"}} {
		l := planhint.NewLedger(nil)
		l.SetPrecedence(planhint.ArrowDirectionKind, []string{"strong", "middle", "weak"})

		for _, name := range order {
			direction := planhint.LeftToRight

			if name == "middle" {
				direction = planhint.RightToLeft
			}

			_, err := l.Issue(arrow(name, direction))
			require.NoError(t, err)
		}

		require.Equal(t, planhint.LeftToRight, l.Hints()[0].Payload.(planhint.ArrowDirection).Direction)
	}
}

type discardSink struct{}

// Record accepts trace dispositions without retaining them in this fixture.
func (discardSink) Record(planhint.Disposition) {}

func TestHintEvidenceIsRetainedOnlyForTracing(t *testing.T) {
	for _, tracing := range []bool{false, true} {
		var sink planhint.TraceSink

		if tracing {
			sink = discardSink{}
		}

		ledger := planhint.NewLedger(sink)
		first := arrow("first", planhint.LeftToRight)
		first.Evidence = []planhint.EvidenceReference{{Signal: "exact", ID: signal.EvidenceID{Epoch: 1, Version: 2}}}
		id, err := ledger.Issue(first)
		require.NoError(t, err)
		first.Evidence[0].Signal = "changed-after-issue"

		second := arrow("second", planhint.LeftToRight)
		second.Evidence = []planhint.EvidenceReference{{Signal: "historical", Historical: true}}
		coalesced, err := ledger.Issue(second)
		require.NoError(t, err)
		require.Equal(t, id, coalesced)
		require.ElementsMatch(t, []string{"first", "second"}, ledger.Hints()[0].Producers)

		if tracing {
			evidence := ledger.Hints()[0].Evidence
			require.Len(t, evidence, 2)
			require.Equal(t, signal.Name("exact"), evidence[0].Signal)
			require.True(t, evidence[1].Historical)
			evidence[0].Signal = "changed-returned-snapshot"
			require.Equal(t, signal.Name("exact"), ledger.Hints()[0].Evidence[0].Signal)
		} else {
			require.Nil(t, ledger.Hints()[0].Evidence)
		}

		// Disabling evidence must not disable strong-hint accounting.
		require.Error(t, ledger.Respond(planhint.Disposition{HintID: id, Consumer: "compiler", Outcome: planhint.Declined}))
		require.NoError(t, ledger.Respond(planhint.Disposition{
			HintID: id, Consumer: "compiler", Outcome: planhint.Declined, Reason: planhint.UnsupportedOperation,
		}))
		require.Len(t, ledger.Decisions(), 1)
	}
}

func TestExecutionResponsesMayRepeat(t *testing.T) {
	ledger := planhint.NewLedger(nil)
	id, err := ledger.Issue(planhint.Hint{
		Producer: "fixed", Target: planhint.Target{Node: 1}, Payload: planhint.CheckConcurrency{Limit: 2},
	})
	require.NoError(t, err)
	response := planhint.Disposition{HintID: id, Consumer: "query-executor", Outcome: planhint.Accepted}
	require.NoError(t, ledger.RespondExecution(response))
	require.NoError(t, ledger.RespondExecution(response))
	require.Error(t, ledger.Respond(response))
	response.Outcome = planhint.Declined
	require.Error(t, ledger.RespondExecution(response))
	response.Reason = planhint.UnsupportedObserver
	require.NoError(t, ledger.RespondExecution(response))
}

// inspectingConcurrentSink checks callback ownership while exercising the supported
// contract that a sink may inspect its ledger during notification.
type inspectingConcurrentSink struct {
	ledger *planhint.Ledger
	active atomic.Int32
	peak   atomic.Int32
}

func (s *inspectingConcurrentSink) Record(planhint.Disposition) {
	active := s.active.Add(1)
	defer s.active.Add(-1)

	for old := s.peak.Load(); active > old && !s.peak.CompareAndSwap(old, active); old = s.peak.Load() {
	}

	_ = s.ledger.Decisions()
	runtime.Gosched()
}

func TestConcurrentExecutionResponsesSerializeTraceCallbacks(t *testing.T) {
	sink := &inspectingConcurrentSink{}
	ledger := planhint.NewLedger(sink)
	sink.ledger = ledger
	id, err := ledger.Issue(arrow("a", planhint.LeftToRight))
	require.NoError(t, err)
	start := make(chan struct{})
	var workers sync.WaitGroup

	for range 32 {
		workers.Go(func() {
			<-start

			for range 32 {
				require.NoError(t, ledger.RespondExecution(planhint.Disposition{
					HintID: id, Consumer: "query-executor", Outcome: planhint.Accepted,
				}))
			}
		})
	}

	close(start)
	workers.Wait()
	require.EqualValues(t, 1, sink.peak.Load())
	require.Len(t, ledger.Decisions(), 1)
}

func TestClonedLedgerKeepsRequestDispositionsIndependent(t *testing.T) {
	template := planhint.NewLedger(nil)
	id, err := template.Issue(planhint.Hint{
		Producer: "fixed-check-concurrency",
		Target:   planhint.Target{Node: 1},
		Payload:  planhint.CheckConcurrency{Limit: 2},
	})
	require.NoError(t, err)
	require.NoError(t, template.Respond(planhint.Disposition{HintID: id, Consumer: "iterator-compiler", Outcome: planhint.Accepted}))

	first := template.Clone()
	second := template.Clone()
	require.NoError(t, first.Respond(planhint.Disposition{
		HintID: id, Consumer: "check-parallelism", Outcome: planhint.Declined, Reason: planhint.BatchChildOrder,
	}))
	require.Len(t, template.Decisions(), 1)
	require.Len(t, second.Decisions(), 1)
	require.NoError(t, second.Respond(planhint.Disposition{HintID: id, Consumer: "check-parallelism", Outcome: planhint.Accepted}))
	require.Len(t, first.Decisions(), 2)
	require.Len(t, second.Decisions(), 2)
	require.Equal(t, planhint.Declined, first.Decisions()[0].Outcome)
	require.Equal(t, planhint.Accepted, second.Decisions()[0].Outcome)
	require.Equal(t, template.Hints(), first.Hints())
	require.Equal(t, template.Hints(), second.Hints())
}
