package signals

import (
	"reflect"
	"sync"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// View provides observation and lookup access for one schema/revision scope.
type View struct {
	owner *Owner
	scope signal.Scope
}

// Read reads an estimate after validating its key and value types.
func (v *View) Read(n signal.Name, k any, kt, vt reflect.Type) (any, signal.Availability, signal.Evidence, error) {
	d, err := v.owner.binding(n, kt, vt, false)
	if err != nil {
		return nil, signal.Missing, signal.Evidence{}, err
	}

	return v.owner.read(v.scope, d, k)
}

// Write publishes an observation after validating its key and observation types.
func (v *View) Write(n signal.Name, k, observation any, kt, ot reflect.Type) error {
	d, err := v.owner.binding(n, kt, ot, true)
	if err != nil {
		return err
	}

	return v.owner.write(v.scope, d, k, observation)
}

// ForPlan returns a view that memoizes independent snapshots for one planning run.
func (v *View) ForPlan() signal.ReadView { return &planView{view: v, values: map[planKey]readResult{}} }

type planKey struct {
	name   signal.Name
	key    any
	kt, vt reflect.Type
}

type readResult struct {
	value        any
	availability signal.Availability
	evidence     signal.Evidence
	err          error
}

type planView struct {
	mu     sync.Mutex
	view   *View
	values map[planKey]readResult // GUARDED_BY(mu)
}

// Read reads an estimate after validating its key and value types.
func (p *planView) Read(n signal.Name, k any, kt, vt reflect.Type) (any, signal.Availability, signal.Evidence, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := planKey{n, k, kt, vt}
	r, ok := p.values[key]
	if !ok {
		r.value, r.availability, r.evidence, r.err = p.view.Read(n, k, kt, vt)
		p.values[key] = r
	}

	return r.value, r.availability, r.evidence, r.err
}

// Enabled reports whether the owner's learning profile enables the named signal.
func (v *View) Enabled(n signal.Name) bool { return v.owner.enabled[n] }

// Registry returns the frozen provider catalog for declaration validation.
func (v *View) Registry() *Registry { return v.owner.registry }
