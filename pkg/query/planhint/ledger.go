package planhint

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"
)

// Ledger validates hint accounting even without a debug trace sink.
type Ledger struct {
	mu         sync.Mutex
	next       ID // GUARDED_BY(mu)
	hints      map[ID]Hint
	responses  map[ID]map[string]Disposition
	active     map[targetKey]ID
	precedence map[Kind][]string
	sink       TraceSink
}

type targetKey struct {
	kind   Kind
	target Target
}

// NewLedger creates a hint ledger with an optional disposition trace sink.
// Hint evidence is retained only when a trace sink is configured; accounting is always enabled.
func NewLedger(sink TraceSink) *Ledger {
	return &Ledger{
		hints:      map[ID]Hint{},
		responses:  map[ID]map[string]Disposition{},
		active:     map[targetKey]ID{},
		precedence: map[Kind][]string{},
		sink:       sink,
	}
}

// CollectsEvidence reports whether a trace sink enables retention of hint evidence.
func (l *Ledger) CollectsEvidence() bool {
	return l.sink != nil
}

// SetPrecedence sets producer order for conflicting hints; earlier producers take precedence.
func (l *Ledger) SetPrecedence(k Kind, producers []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.precedence[k] = slices.Clone(producers)
}

func cloneHint(h Hint) Hint {
	h.Producers = slices.Clone(h.Producers)
	h.Evidence = slices.Clone(h.Evidence)

	if b, ok := h.Payload.(BranchOrder); ok {
		b.Order = slices.Clone(b.Order)
		h.Payload = b
	}

	return h
}

func cloneResponse(d Disposition) Disposition { d.Details = maps.Clone(d.Details); return d }

// Issue validates and records a recommendation, coalescing identical payloads.
func (l *Ledger) Issue(h Hint) (ID, error) {
	if h.Payload == nil || h.Producer == "" || h.Target.Node == 0 {
		return 0, errors.New("hint requires payload, producer and target")
	}

	if err := h.Payload.Validate(); err != nil {
		return 0, err
	}

	switch h.Payload.(type) {
	case ArrowDirection, BranchOrder:
	default:
		return 0, fmt.Errorf("unknown hint payload kind %q", h.Payload.Kind())
	}

	// Evidence explains a recommendation but never determines how it is applied.
	// Drop externally supplied evidence too, so untraced plans do not retain it even
	// when their producer did not consult the collection policy.
	if !l.CollectsEvidence() {
		h.Evidence = nil
	}

	l.mu.Lock()
	var notification *Disposition
	defer func() {
		l.mu.Unlock()

		if notification != nil && l.sink != nil {
			l.sink.Record(cloneResponse(*notification))
		}
	}()
	key := targetKey{h.Payload.Kind(), h.Target}
	old := l.active[key]
	if old != 0 && reflect.DeepEqual(l.hints[old].Payload, h.Payload) {
		v := l.hints[old]
		if !slices.Contains(v.Producers, h.Producer) {
			v.Producers = append(v.Producers, h.Producer)
		}

		v.Evidence = append(v.Evidence, h.Evidence...)
		l.hints[old] = v
		return old, nil
	}

	winner := true

	if old != 0 {
		order := l.precedence[h.Payload.Kind()]
		a, b := len(order), slices.Index(order, h.Producer)

		for _, producer := range l.hints[old].Producers {
			rank := slices.Index(order, producer)
			if rank < 0 {
				return 0, fmt.Errorf("coalesced producer %q needs explicit precedence", producer)
			}

			a = min(a, rank)
		}

		if a < 0 || b < 0 || a == b {
			return 0, fmt.Errorf("conflicting hint %q needs explicit producer precedence", key.kind)
		}

		winner = b < a
	}

	l.next++
	h.ID = l.next
	h.Producers = []string{h.Producer}
	l.hints[h.ID] = cloneHint(h)

	if old == 0 || winner {
		l.active[key] = h.ID
	}

	if old != 0 {
		loser, winning := h.ID, old

		if winner {
			loser, winning = old, h.ID
		}

		d := Disposition{
			HintID:      loser,
			Consumer:    "hint-resolver",
			Outcome:     Superseded,
			Reason:      ConflictingHint,
			RelatedHint: winning,
		}
		l.respondLocked(d)
		notification = &d
	}

	return h.ID, nil
}

// Respond validates and records a consumer's response, notifying the optional trace sink.
func (l *Ledger) Respond(d Disposition) error {
	l.mu.Lock()
	notify := false
	defer func() {
		l.mu.Unlock()

		if notify && l.sink != nil {
			l.sink.Record(cloneResponse(d))
		}
	}()

	if _, ok := l.hints[d.HintID]; !ok || d.Consumer == "" || d.Outcome > Unconsumed {
		return errors.New("invalid hint disposition")
	}

	if d.Outcome != Accepted && d.Reason == "" {
		return errors.New("hint decline requires structured reason")
	}

	if (d.Reason == StrongerEvidence || d.Outcome == Superseded) && d.RelatedHint == 0 {
		return errors.New("supersession requires related evidence hint")
	}

	if len(d.Details) > 16 {
		return errors.New("too many disposition details")
	}

	for k, v := range d.Details {
		if len(k) > 128 || len(v) > 512 {
			return errors.New("oversize disposition details")
		}
	}

	if prior, ok := l.responses[d.HintID][d.Consumer]; ok && prior.Outcome == d.Outcome {
		return errors.New("duplicate hint disposition")
	}

	l.respondLocked(d)
	notify = true
	return nil
}

func (l *Ledger) respondLocked(d Disposition) {
	if l.responses[d.HintID] == nil {
		l.responses[d.HintID] = map[string]Disposition{}
	}

	l.responses[d.HintID][d.Consumer] = cloneResponse(d)

	if d.Outcome == Invalidated || d.Outcome == Superseded ||
		(d.Outcome == Accepted && l.hints[d.HintID].Payload.Kind() == BranchOrderKind) {
		h := l.hints[d.HintID]
		key := targetKey{h.Payload.Kind(), h.Target}
		if l.active[key] == d.HintID {
			delete(l.active, key)
		}
	}
}

// Hints returns independent copies of active recommendations in ID order.
func (l *Ledger) Hints() []Hint {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Hint, 0, len(l.active))

	for _, id := range l.active {
		out = append(out, cloneHint(l.hints[id]))
	}

	slices.SortFunc(out, func(a, b Hint) int {
		if a.ID < b.ID {
			return -1
		}

		if a.ID > b.ID {
			return 1
		}

		return 0
	})
	return out
}

// Decisions returns independent copies of recorded dispositions in hint and consumer order.
func (l *Ledger) Decisions() []Disposition {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := []Disposition{}
	ids := make([]ID, 0, len(l.responses))

	for id := range l.responses {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	for _, id := range ids {
		names := slices.Sorted(maps.Keys(l.responses[id]))

		for _, n := range names {
			out = append(out, cloneResponse(l.responses[id][n]))
		}
	}

	return out
}

// HasResponse reports whether a consumer has accounted for the given recommendation.
func (l *Ledger) HasResponse(id ID, consumer string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.responses[id][consumer]
	return ok
}

// Finalize records unconsumed dispositions for recommendations with no response.
func (l *Ledger) Finalize() error {
	l.mu.Lock()
	notifications := []Disposition{}
	defer func() {
		l.mu.Unlock()

		if l.sink != nil {
			for _, d := range notifications {
				l.sink.Record(cloneResponse(d))
			}
		}
	}()

	for id := range l.hints {
		if len(l.responses[id]) == 0 {
			d := Disposition{HintID: id, Consumer: "pipeline", Outcome: Unconsumed, Reason: NoConsumer}
			l.respondLocked(d)
			notifications = append(notifications, d)
		}
	}

	return nil
}

// Remap moves a recommendation only after the pipeline validates both occurrences.
func (l *Ledger) Remap(id ID, target Target) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	h, ok := l.hints[id]
	if !ok || target.Node == 0 || target.Fingerprint == "" {
		return errors.New("invalid hint remap")
	}

	old := targetKey{h.Payload.Kind(), h.Target}
	if l.active[old] != id {
		return errors.New("cannot remap inactive hint")
	}

	next := targetKey{h.Payload.Kind(), target}
	if other := l.active[next]; other != 0 && other != id {
		return fmt.Errorf("remap would conflict with hint %d", other)
	}

	delete(l.active, old)
	h.Target = target
	l.hints[id] = h
	l.active[next] = id
	return nil
}
