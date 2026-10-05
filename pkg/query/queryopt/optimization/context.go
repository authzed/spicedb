package optimization

import (
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// Decision records why a planning optimization was skipped.
type Decision struct {
	// Optimization names the optimization associated with the decision.
	Optimization string

	// Reason explains why the optimization was skipped.
	Reason string
}

// ErrUnavailable indicates that a required planning input has no usable value.
var ErrUnavailable = errors.New("required planning input unavailable")

// Context provides a planning pass with declared inputs and hint accounting.
type Context struct {
	// Decisions records planning decisions such as skipped passes.
	Decisions []Decision

	// Params contains request-specific optimization inputs.
	Params RequestParams

	// Signals provides access to the pass's declared signal inputs.
	Signals signal.ReadView

	// Ledger tracks issued recommendations and consumer responses.
	Ledger *planhint.Ledger

	// Name names the currently executing optimization.
	Name string

	allowedHints []planhint.Kind
	produced     []planhint.Kind

	// Key constructs an execution population key for a subtree and operation.
	// Callers must supply it for passes that look up execution signals.
	Key func(query.Outline, query.Operation) signal.ExecutionKey
}

type (

	// TargetRemap moves a recommendation between validated planning occurrences.
	TargetRemap struct {
		// From identifies the original recommendation target.
		From planhint.Target

		// To identifies the replacement recommendation target.
		To planhint.Target
	}

	// Result contains a rewritten outline, emitted hints, and target remaps.
	Result struct {
		// Outline is the rewritten canonical outline.
		Outline query.CanonicalOutline

		// Hints contains recommendations emitted by the pass.
		Hints []planhint.Hint

		// Remaps contains target transfers for recommendations retained by the rewrite.
		Remaps []TargetRemap
	}
)

// NewContext creates a planning context with signal access and a fresh hint ledger.
func NewContext(p RequestParams, view signal.ReadView, sink planhint.TraceSink) *Context {
	return &Context{Params: p, Signals: view, Ledger: planhint.NewLedger(sink)}
}

// HintEvidenceEnabled reports whether the active ledger retains evidence for debug tracing.
func (c *Context) HintEvidenceEnabled() bool {
	return c.Ledger != nil && c.Ledger.CollectsEvidence()
}

// ForPass creates a context restricted to an optimization's declared inputs and outputs.
func (c *Context) ForPass(opt Optimizer) *Context {
	next := *c
	next.Name = opt.Name
	next.allowedHints = append(slices.Clone(opt.RequiredHints), opt.OptionalHints...)
	next.produced = opt.ProducesHints
	next.Signals = restrictedView{
		view:     c.Signals,
		allowed:  append(slices.Clone(opt.RequiredSignals), opt.OptionalSignals...),
		required: opt.RequiredSignals,
	}
	return &next
}

// Hints returns active recommendations of a declared input kind.
func (c *Context) Hints(kind planhint.Kind) ([]planhint.Hint, error) {
	if !slices.Contains(c.allowedHints, kind) {
		return nil, fmt.Errorf("undeclared input hint %q", kind)
	}

	out := []planhint.Hint{}

	for _, h := range c.Ledger.Hints() {
		if h.Payload.Kind() == kind {
			out = append(out, h)
		}
	}

	return out, nil
}

// Respond records the current pass's response to a recommendation.
func (c *Context) Respond(id planhint.ID, outcome planhint.Outcome, reason planhint.ReasonCode) error {
	return c.Ledger.Respond(planhint.Disposition{HintID: id, Consumer: c.Name, Outcome: outcome, Reason: reason})
}

type restrictedView struct {
	view     signal.ReadView
	allowed  []signal.Name
	required []signal.Name
}

// Read reads an estimate after validating its key and value types.
func (v restrictedView) Read(n signal.Name, k any, kt, vt reflect.Type) (any, signal.Availability, signal.Evidence, error) {
	if !slices.Contains(v.allowed, n) {
		return nil, signal.Missing, signal.Evidence{}, fmt.Errorf("undeclared signal %q", n)
	}

	var value any
	var availability signal.Availability
	var evidence signal.Evidence
	var err error

	if v.view == nil {
		availability = signal.Missing
	} else {
		value, availability, evidence, err = v.view.Read(n, k, kt, vt)
	}

	if err == nil && availability != signal.Available && slices.Contains(v.required, n) {
		err = fmt.Errorf("%w: %s", ErrUnavailable, n)
	}

	return value, availability, evidence, err
}
