package queryopt

import (
	"errors"
	"fmt"
	"slices"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/signals"
)

// Pipeline runs validated optimization passes in dependency order.
type Pipeline struct{ passes []Optimizer }

// NewPipeline validates declarations and constructs a dependency-ordered optimization pipeline.
func NewPipeline(opts []Optimizer, catalogs ...*signals.Registry) (*Pipeline, error) {
	var catalog *signals.Registry

	switch len(catalogs) {
	case 0:
		var err error
		catalog, err = signals.NewDefaultRegistry()
		if err != nil {
			return nil, err
		}
	case 1:
		catalog = catalogs[0]
	default:
		return nil, errors.New("one signal catalog is required")
	}

	if catalog == nil {
		return nil, errors.New("nil signal catalog")
	}

	byName := map[string]Optimizer{}
	producers := map[planhint.Kind][]string{}

	for _, o := range opts {
		for _, name := range append(slices.Clone(o.RequiredSignals), o.OptionalSignals...) {
			if !catalog.Contains(name) {
				return nil, fmt.Errorf("optimization %q declares unknown signal %q", o.Name, name)
			}
		}

		for _, kind := range append(append(slices.Clone(o.RequiredHints), o.OptionalHints...), o.ProducesHints...) {
			switch kind {
			case planhint.ArrowDirectionKind, planhint.BranchOrderKind:
			default:
				return nil, fmt.Errorf("optimization %q declares unknown hint %q", o.Name, kind)
			}
		}

		if o.Name == "" || (o.Run == nil) == (o.NewTransform == nil) {
			return nil, errors.New("optimization needs a name and exactly one callback")
		}

		if _, ok := byName[o.Name]; ok {
			return nil, fmt.Errorf("duplicate optimization %q", o.Name)
		}

		byName[o.Name] = o

		for _, k := range o.ProducesHints {
			producers[k] = append(producers[k], o.Name)
		}
	}

	deps := map[string]map[string]bool{}

	for _, o := range opts {
		deps[o.Name] = map[string]bool{}

		for _, n := range o.After {
			if _, ok := byName[n]; !ok {
				return nil, fmt.Errorf("missing optimizer dependency %q", n)
			}

			deps[o.Name][n] = true
		}

		for _, k := range o.RequiredHints {
			if len(producers[k]) == 0 {
				return nil, fmt.Errorf("missing required hint producer %q", k)
			}
		}

		for _, k := range append(slices.Clone(o.RequiredHints), o.OptionalHints...) {
			for _, n := range producers[k] {
				deps[o.Name][n] = true
			}
		}
	}

	p := &Pipeline{}
	done := map[string]bool{}

	for len(done) < len(opts) {
		ready := []Optimizer{}

		for _, o := range opts {
			if done[o.Name] {
				continue
			}

			valid := true

			for n := range deps[o.Name] {
				if !done[n] {
					valid = false
				}
			}

			if valid {
				ready = append(ready, o)
			}
		}

		if len(ready) == 0 {
			return nil, errors.New("optimization dependency cycle")
		}

		slices.SortFunc(ready, func(a, b Optimizer) int {
			if a.Priority > b.Priority {
				return -1
			}

			if a.Priority < b.Priority {
				return 1
			}

			if a.Name < b.Name {
				return -1
			}

			if a.Name > b.Name {
				return 1
			}

			return 0
		})
		o := ready[0]
		p.passes = append(p.passes, o)
		done[o.Name] = true
	}

	return p, nil
}

// Run executes the ordered passes and accounts for emitted recommendations.
func (p *Pipeline) Run(co query.CanonicalOutline, ctx *optimization.Context) (query.CanonicalOutline, error) {
	if ctx == nil {
		return query.CanonicalOutline{}, errors.New("nil optimization context")
	}

	if ctx.Ledger == nil {
		ctx.Ledger = planhint.NewLedger(nil)
	}

	if co.HintLedger != nil {
		ctx.Ledger = co.HintLedger
	}

	co = query.UniqueOutlineOccurrences(co)
	if co.HintLedger == nil && len(co.Hints) > 0 {
		facts := query.DescribeOutline(co.Root)

		for node, hints := range co.Hints {
			for _, h := range hints {
				h.Target = planhint.Target{Node: uint64(node), Fingerprint: facts[node].Identity}

				if h.Producer == "" {
					h.Producer = "legacy-advisor"
				}

				if _, err := ctx.Ledger.Issue(h); err != nil {
					return query.CanonicalOutline{}, err
				}
			}
		}
	}

	for _, o := range p.passes {
		missing := false

		for _, kind := range o.RequiredHints {
			present := false

			for _, h := range ctx.Ledger.Hints() {
				if h.Payload.Kind() == kind {
					present = true
				}
			}

			if !present {
				missing = true
			}
		}

		if missing {
			ctx.Decisions = append(ctx.Decisions, optimization.Decision{Optimization: o.Name, Reason: "required-hint-unavailable"})
			continue
		}

		pass := ctx.ForPass(o)
		before := query.DescribeOutline(co.Root)
		var result optimization.Result
		var err error

		if o.Run != nil {
			result, err = o.Run(pass, co)
		} else {
			next := co
			next.Root = o.NewTransform(ctx.Params)(co.Root)
			result.Outline = next
		}

		if errors.Is(err, optimization.ErrUnavailable) {
			ctx.Decisions = append(ctx.Decisions, optimization.Decision{Optimization: o.Name, Reason: err.Error()})
			continue
		}

		if err != nil {
			return query.CanonicalOutline{}, fmt.Errorf("optimization %s: %w", o.Name, err)
		}

		next := query.UniqueOutlineOccurrences(result.Outline)
		after := query.DescribeOutline(next.Root)

		for _, remap := range result.Remaps {
			from, ok := before[query.OutlineNodeID(remap.From.Node)]
			to, exists := after[query.OutlineNodeID(remap.To.Node)]
			if !ok || !exists || from.Identity != remap.From.Fingerprint || to.Identity != remap.To.Fingerprint {
				return query.CanonicalOutline{}, errors.New("invalid explicit hint remap")
			}

			for _, h := range ctx.Ledger.Hints() {
				if h.Target == remap.From {
					if err := ctx.Ledger.Remap(h.ID, remap.To); err != nil {
						return query.CanonicalOutline{}, err
					}
				}
			}
		}

		for _, h := range ctx.Ledger.Hints() {
			if f, ok := after[query.OutlineNodeID(h.Target.Node)]; !ok || f.Identity != h.Target.Fingerprint {
				if err := ctx.Ledger.Respond(
					planhint.Disposition{HintID: h.ID, Consumer: o.Name, Outcome: planhint.Invalidated, Reason: planhint.StaleTarget},
				); err != nil {
					return query.CanonicalOutline{}, err
				}
			}
		}

		for _, kind := range append(slices.Clone(o.RequiredHints), o.OptionalHints...) {
			for _, hint := range ctx.Ledger.Hints() {
				if hint.Payload.Kind() == kind && !ctx.Ledger.HasResponse(hint.ID, o.Name) {
					return query.CanonicalOutline{}, fmt.Errorf("consumer %s did not justify disposition for hint %d", o.Name, hint.ID)
				}
			}
		}

		for _, h := range result.Hints {
			if h.Payload == nil || !slices.Contains(o.ProducesHints, h.Payload.Kind()) {
				return query.CanonicalOutline{}, fmt.Errorf("undeclared hint output by %s", o.Name)
			}

			f, ok := after[query.OutlineNodeID(h.Target.Node)]
			if !ok {
				return query.CanonicalOutline{}, errors.New("invalid hint target")
			}

			h.Target.Fingerprint = f.Identity
			h.Producer = o.Name

			if _, err := ctx.Ledger.Issue(h); err != nil {
				return query.CanonicalOutline{}, err
			}
		}

		co = next
	}

	co.HintLedger = ctx.Ledger
	co.Hints = map[query.OutlineNodeID][]query.Hint{}

	for _, h := range ctx.Ledger.Hints() {
		co.Hints[query.OutlineNodeID(h.Target.Node)] = append(co.Hints[query.OutlineNodeID(h.Target.Node)], h)
	}

	return co, nil
}
